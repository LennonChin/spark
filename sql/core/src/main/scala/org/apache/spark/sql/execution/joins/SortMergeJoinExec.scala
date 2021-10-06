/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.collection.BitSet

/**
 * Performs a sort merge join of two child relations.
 */
case class SortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // 输出属性
  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    // Inner Like Join的输出数据分区是由左右表的输出分区共同决定的
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    // 左外连接由左表的输出分区决定
    case LeftOuter => left.outputPartitioning
    // 右外连接由右表的输出分区决定
    case RightOuter => right.outputPartitioning
    // 全外连接是未知输出分区，但分区数是确定的
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    // Left existence连接由左表的输出分区决定
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  // 子节点的数据分布是按连接键全局分区
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  // 按左表连接键升序排序
  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  // 子节点的数据排序是按连接键升序排序
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    // 按照键升序排序
    keys.map(SortOrder(_, Ascending))
  }

  // 左表连接键的Projection
  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(leftKeys, left.output)

  // 右表连接键的Projection
  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(rightKeys, right.output)

  protected override def doExecute(): RDD[InternalRow] = {

    // 输出数据行度量
    val numOutputRows = longMetric("numOutputRows")

    // 左右表的分区一对一进行zip后，遍历
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>

      // 创建Join连接时的条件判断谓词
      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          newPredicate(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      // 根据左表连接键判断用于比较两侧键的Ordering，会使用Codegen生成具体的Ordering对象。
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))

      // 用于生成结果的Projection
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      // 根据不同的Join类型分别处理
      joinType match {
        // Inner Like Join => RowIterator + SortMergeJoinScanner
        case _: InnerLike =>
          new RowIterator {
            // 当前左表的行
            private[this] var currentLeftRow: InternalRow = _
            // 当前右表匹配的行
            private[this] var currentRightMatches: ArrayBuffer[InternalRow] = _
            // 当前匹配的索引
            private[this] var currentMatchIdx: Int = -1

            // 创建JoinScanner
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(), // 左表连接键Projection
              createRightKeyGenerator(), // 右表连接键Projection
              keyOrdering, // 连接键比较器
              RowIterator.fromScala(leftIter), // 左表数据接迭代器
              RowIterator.fromScala(rightIter) // 右表数据迭代器
            )

            // 连接后的结果行
            private[this] val joinRow = new JoinedRow

            // 初始化时，调用JonScanner的findNextInnerJoinRows()得到满足Join条件的数据
            if (smjScanner.findNextInnerJoinRows()) {
              currentRightMatches = smjScanner.getBufferedMatches
              currentLeftRow = smjScanner.getStreamedRow
              currentMatchIdx = 0
            }

            // 推进到下一行
            override def advanceNext(): Boolean = {
              while (currentMatchIdx >= 0) {
                // 当推进的Buffered Row数据已经到达缓冲区末尾，就进入该if分支
                if (currentMatchIdx == currentRightMatches.length) {
                  if (smjScanner.findNextInnerJoinRows()) {
                    /**
                     * 尝试推进获得下一个匹配行，findNextInnerJoinRows()会推进streamedRow，
                     * 返回true表示有新的匹配行，否则返回true
                     */
                    currentRightMatches = smjScanner.getBufferedMatches
                    currentLeftRow = smjScanner.getStreamedRow
                    currentMatchIdx = 0
                  } else {
                    // 如果JoinScanner的findNextInnerJoinRows()返回false，说明没有剩余的行的，可以结束终止
                    currentRightMatches = null
                    currentLeftRow = null
                    currentMatchIdx = -1
                    return false
                  }
                }

                // 不断将当前Streamed Row与Buffered Rows缓冲区里的所有Buffered Row组合为JoinedRow，用于在getRow方法返回给外界
                joinRow(currentLeftRow, currentRightMatches(currentMatchIdx))
                // 更新索引
                currentMatchIdx += 1

                // 检查其他Join条件是否满足，如果满足才返回false
                if (boundCondition(joinRow)) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            // 结果行是JoinedRow的Project结果
            override def getRow: InternalRow = resultProj(joinRow)
          }.toScala

        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),  // 左表是Streamed Iterator
            bufferedIter = RowIterator.fromScala(rightIter) // 右表是Buffered Iterator
          )

          // 用于空值填充的通用Row；空值填充列是右表的列。
          val rightNullRow = new GenericInternalRow(right.output.length)

          // 使用LeftOuterIterator
          new LeftOuterIterator(
            smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),  // 右表是Streamed Iterator
            bufferedIter = RowIterator.fromScala(leftIter) // 左表是Buffered Iterator
          )
          // 用于空值填充的通用Row；空值填充列是左表的列。
          val leftNullRow = new GenericInternalRow(left.output.length)

          // 使用RightOuterIterator
          new RightOuterIterator(
            smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows).toScala

        case FullOuter =>
          // 左右表留Null时的通用行
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)

          // 创建SortMergeFullOuterJoinScanner
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter), // 左表
            rightIter = RowIterator.fromScala(rightIter), // 右表
            boundCondition,
            leftNullRow, // 左表Null填充行
            rightNullRow) // 右表Null填充行

          // 返回FullOuterIterator
          new FullOuterIterator(
            smjScanner,
            resultProj,
            numOutputRows).toScala

        case LeftSemi =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _

            // 创建SortMergeJoinScanner
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter), // 左表是Streamed侧
              RowIterator.fromScala(rightIter) // 右表是Buffered侧
            )
            private[this] val joinRow = new JoinedRow

            // 推进下一行
            override def advanceNext(): Boolean = {
              while (smjScanner.findNextInnerJoinRows()) { // 能够找到匹配的行
                // 获取当前匹配度额bufferedMatches缓冲区
                val currentRightMatches = smjScanner.getBufferedMatches

                // 获取当期Streamed Row，使用currentLeftRow记录
                currentLeftRow = smjScanner.getStreamedRow
                var i = 0

                // 遍历bufferedMatches中所有的Buffered行
                while (i < currentRightMatches.length) {
                  joinRow(currentLeftRow, currentRightMatches(i))

                  // 匹配行满足Join条件，则说明这条数据是保留的，返回true
                  if (boundCondition(joinRow)) {
                    numOutputRows += 1
                    return true
                  }
                  i += 1
                }
              }
              false
            }

            // 获取当前的结果行，注意只返回左表的行数据。
            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case LeftAnti =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _

            // 创建SortMergeJoinScanner
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter), // 左表是Streamed侧
              RowIterator.fromScala(rightIter) // 右表是Buffered侧
            )
            private[this] val joinRow = new JoinedRow

            // 推进下一行
            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) { // 能够找到匹配的行
                // 获取当期Streamed Row，使用currentLeftRow记录
                currentLeftRow = smjScanner.getStreamedRow

                // 获取当前匹配度额bufferedMatches缓冲区
                val currentRightMatches = smjScanner.getBufferedMatches

                // 如果bufferedMatches为空，说明没有匹配到Buffered Row，说明这条数据是保留的，返回true
                if (currentRightMatches == null) {
                  return true
                }

                // 遍历匹配到的Buffered Rows
                var i = 0
                var found = false


                while (!found && i < currentRightMatches.length) {
                  joinRow(currentLeftRow, currentRightMatches(i))

                  // 一旦出现满足判断条件的Buffered Row，就结束循环，说明该行数据不应该保留，会返回false
                  if (boundCondition(joinRow)) {
                    found = true
                  }
                  i += 1
                }

                // 所有Buffered Rows都遍历并判断完了，如果没有满足判断条件的Buffered Row，说明这条数据是保留的，返回true
                if (!found) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            // 获取当前的结果行
            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case j: ExistenceJoin =>
          new RowIterator {
            // 记录左表当前的行
            private[this] var currentLeftRow: InternalRow = _
            // 记录结果行的exists标记
            private[this] val result: InternalRow = new GenericInternalRow(Array[Any](null))

            // 创建SortMergeJoinScanner
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter), // 左表是Streamed侧
              RowIterator.fromScala(rightIter) // 右表是Buffered侧
            )
            private[this] val joinRow = new JoinedRow

            // 推进下一行
            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) { // 能够找到匹配的行
                // 获取当期Streamed Row，使用currentLeftRow记录
                currentLeftRow = smjScanner.getStreamedRow

                // 获取当前匹配度额bufferedMatches缓冲区
                val currentRightMatches = smjScanner.getBufferedMatches

                // 遍历bufferedMatches中所有的Buffered行
                var found = false
                if (currentRightMatches != null) {
                  var i = 0
                  while (!found && i < currentRightMatches.length) {

                    // 匹配行满足Join条件，将found置为true，以结束while循环
                    joinRow(currentLeftRow, currentRightMatches(i))
                    if (boundCondition(joinRow)) {
                      found = true
                    }
                    i += 1
                  }
                }

                /**
                 * 如果上面的while条件遍历完也没有找到满足判断条件的Buffered Row，found的值是false
                 */
                result.setBoolean(0, found)
                numOutputRows += 1
                return true
              }
              false
            }

            // 获取当前的结果行，注意返回了左表的行数据和exists标记。
            override def getRow: InternalRow = resultProj(joinRow(currentLeftRow, result))
          }.toScala

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeJoin should not take $x as the JoinType")
      }

    }
  }

  override def supportCodegen: Boolean = {
    joinType.isInstanceOf[InnerLike]
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    left.execute() :: right.execute() :: Nil
  }

  private def createJoinKey(
      ctx: CodegenContext,
      row: String,
      keys: Seq[Expression],
      input: Seq[Attribute]): Seq[ExprCode] = {
    ctx.INPUT_ROW = row
    keys.map(BindReferences.bindReference(_, input).genCode(ctx))
  }

  private def copyKeys(ctx: CodegenContext, vars: Seq[ExprCode]): Seq[ExprCode] = {
    vars.zipWithIndex.map { case (ev, i) =>
      ctx.addBufferedState(leftKeys(i).dataType, "value", ev.value)
    }
  }

  private def genComparision(ctx: CodegenContext, a: Seq[ExprCode], b: Seq[ExprCode]): String = {
    val comparisons = a.zip(b).zipWithIndex.map { case ((l, r), i) =>
      s"""
         |if (comp == 0) {
         |  comp = ${ctx.genComp(leftKeys(i).dataType, l.value, r.value)};
         |}
       """.stripMargin.trim
    }
    s"""
       |comp = 0;
       |${comparisons.mkString("\n")}
     """.stripMargin
  }

  /**
   * Generate a function to scan both left and right to find a match, returns the term for
   * matched one row from left side and buffered rows from right side.
   */
  private def genScanner(ctx: CodegenContext): (String, String) = {
    // Create class member for next row from both sides.
    val leftRow = ctx.freshName("leftRow")
    ctx.addMutableState("InternalRow", leftRow, "")
    val rightRow = ctx.freshName("rightRow")
    ctx.addMutableState("InternalRow", rightRow, s"$rightRow = null;")

    // Create variables for join keys from both sides.
    val leftKeyVars = createJoinKey(ctx, leftRow, leftKeys, left.output)
    val leftAnyNull = leftKeyVars.map(_.isNull).mkString(" || ")
    val rightKeyTmpVars = createJoinKey(ctx, rightRow, rightKeys, right.output)
    val rightAnyNull = rightKeyTmpVars.map(_.isNull).mkString(" || ")
    // Copy the right key as class members so they could be used in next function call.
    val rightKeyVars = copyKeys(ctx, rightKeyTmpVars)

    // A list to hold all matched rows from right side.
    val matches = ctx.freshName("matches")
    val clsName = classOf[java.util.ArrayList[InternalRow]].getName
    ctx.addMutableState(clsName, matches, s"$matches = new $clsName();")
    // Copy the left keys as class members so they could be used in next function call.
    val matchedKeyVars = copyKeys(ctx, leftKeyVars)

    ctx.addNewFunction("findNextInnerJoinRows",
      s"""
         |private boolean findNextInnerJoinRows(
         |    scala.collection.Iterator leftIter,
         |    scala.collection.Iterator rightIter) {
         |  $leftRow = null;
         |  int comp = 0;
         |  while ($leftRow == null) {
         |    if (!leftIter.hasNext()) return false;
         |    $leftRow = (InternalRow) leftIter.next();
         |    ${leftKeyVars.map(_.code).mkString("\n")}
         |    if ($leftAnyNull) {
         |      $leftRow = null;
         |      continue;
         |    }
         |    if (!$matches.isEmpty()) {
         |      ${genComparision(ctx, leftKeyVars, matchedKeyVars)}
         |      if (comp == 0) {
         |        return true;
         |      }
         |      $matches.clear();
         |    }
         |
         |    do {
         |      if ($rightRow == null) {
         |        if (!rightIter.hasNext()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return !$matches.isEmpty();
         |        }
         |        $rightRow = (InternalRow) rightIter.next();
         |        ${rightKeyTmpVars.map(_.code).mkString("\n")}
         |        if ($rightAnyNull) {
         |          $rightRow = null;
         |          continue;
         |        }
         |        ${rightKeyVars.map(_.code).mkString("\n")}
         |      }
         |      ${genComparision(ctx, leftKeyVars, rightKeyVars)}
         |      if (comp > 0) {
         |        $rightRow = null;
         |      } else if (comp < 0) {
         |        if (!$matches.isEmpty()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return true;
         |        }
         |        $leftRow = null;
         |      } else {
         |        $matches.add($rightRow.copy());
         |        $rightRow = null;;
         |      }
         |    } while ($leftRow != null);
         |  }
         |  return false; // unreachable
         |}
       """.stripMargin)

    (leftRow, matches)
  }

  /**
   * Creates variables for left part of result row.
   *
   * In order to defer the access after condition and also only access once in the loop,
   * the variables should be declared separately from accessing the columns, we can't use the
   * codegen of BoundReference here.
   */
  private def createLeftVars(ctx: CodegenContext, leftRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = leftRow
    left.output.zipWithIndex.map { case (a, i) =>
      val value = ctx.freshName("value")
      val valueCode = ctx.getValue(leftRow, a.dataType, i.toString)
      // declare it as class member, so we can access the column before or in the loop.
      ctx.addMutableState(ctx.javaType(a.dataType), value, "")
      if (a.nullable) {
        val isNull = ctx.freshName("isNull")
        ctx.addMutableState("boolean", isNull, "")
        val code =
          s"""
             |$isNull = $leftRow.isNullAt($i);
             |$value = $isNull ? ${ctx.defaultValue(a.dataType)} : ($valueCode);
           """.stripMargin
        ExprCode(code, isNull, value)
      } else {
        ExprCode(s"$value = $valueCode;", "false", value)
      }
    }
  }

  /**
   * Creates the variables for right part of result row, using BoundReference, since the right
   * part are accessed inside the loop.
   */
  private def createRightVar(ctx: CodegenContext, rightRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = rightRow
    right.output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).genCode(ctx)
    }
  }

  /**
   * Splits variables based on whether it's used by condition or not, returns the code to create
   * these variables before the condition and after the condition.
   *
   * Only a few columns are used by condition, then we can skip the accessing of those columns
   * that are not used by condition also filtered out by condition.
   */
  private def splitVarsByCondition(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode]): (String, String) = {
    if (condition.isDefined) {
      val condRefs = condition.get.references
      val (used, notUsed) = attributes.zip(variables).partition{ case (a, ev) =>
        condRefs.contains(a)
      }
      val beforeCond = evaluateVariables(used.map(_._2))
      val afterCond = evaluateVariables(notUsed.map(_._2))
      (beforeCond, afterCond)
    } else {
      (evaluateVariables(variables), "")
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    ctx.copyResult = true
    val leftInput = ctx.freshName("leftInput")
    ctx.addMutableState("scala.collection.Iterator", leftInput, s"$leftInput = inputs[0];")
    val rightInput = ctx.freshName("rightInput")
    ctx.addMutableState("scala.collection.Iterator", rightInput, s"$rightInput = inputs[1];")

    val (leftRow, matches) = genScanner(ctx)

    // Create variables for row from both sides.
    val leftVars = createLeftVars(ctx, leftRow)
    val rightRow = ctx.freshName("rightRow")
    val rightVars = createRightVar(ctx, rightRow)

    val size = ctx.freshName("size")
    val i = ctx.freshName("i")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val (beforeLoop, condCheck) = if (condition.isDefined) {
      // Split the code of creating variables based on whether it's used by condition or not.
      val loaded = ctx.freshName("loaded")
      val (leftBefore, leftAfter) = splitVarsByCondition(left.output, leftVars)
      val (rightBefore, rightAfter) = splitVarsByCondition(right.output, rightVars)
      // Generate code for condition
      ctx.currentVars = leftVars ++ rightVars
      val cond = BindReferences.bindReference(condition.get, output).genCode(ctx)
      // evaluate the columns those used by condition before loop
      val before = s"""
           |boolean $loaded = false;
           |$leftBefore
         """.stripMargin

      val checking = s"""
         |$rightBefore
         |${cond.code}
         |if (${cond.isNull} || !${cond.value}) continue;
         |if (!$loaded) {
         |  $loaded = true;
         |  $leftAfter
         |}
         |$rightAfter
     """.stripMargin
      (before, checking)
    } else {
      (evaluateVariables(leftVars), "")
    }

    s"""
       |while (findNextInnerJoinRows($leftInput, $rightInput)) {
       |  int $size = $matches.size();
       |  ${beforeLoop.trim}
       |  for (int $i = 0; $i < $size; $i ++) {
       |    InternalRow $rightRow = (InternalRow) $matches.get($i);
       |    ${condCheck.trim}
       |    $numOutput.add(1);
       |    ${consume(ctx, leftVars ++ rightVars)}
       |  }
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}

/**
 * Helper class that is used to implement [[SortMergeJoinExec]].
 *
 * To perform an inner (outer) join, users of this class call [[findNextInnerJoinRows()]]
 * ([[findNextOuterJoinRows()]]), which returns `true` if a result has been produced and `false`
 * otherwise. If a result has been produced, then the caller may call [[getStreamedRow]] to return
 * the matching row from the streamed input and may call [[getBufferedMatches]] to return the
 * sequence of matching rows from the buffered input (in the case of an outer join, this will return
 * an empty sequence if there are no matches from the buffered input). For efficiency, both of these
 * methods return mutable objects which are re-used across calls to the `findNext*JoinRows()`
 * methods.
 *
 * 要执行内部（外部）连接，此类的用户调用 [[findNextInnerJoinRows()]] ([[findNextOuterJoinRows()]])，
 * 如果结果已生成，则返回 `true`，否则返回 `false`。
 * 如果已经产生了结果，那么调用者可以调用 [[getStreamedRow]] 从流输入中返回匹配的行，
 * 也可以调用 [[getBufferedMatches]] 从缓冲输入中返回匹配行的序列（在外连接的情况下，如果缓冲输入中没有匹配项，这将返回一个空序列）。
 * 为了提高效率，这两种方法都返回可变对象，这些对象在调用 findNext*JoinRows() 方法时重复使用。
 *
 * @param streamedKeyGenerator a projection that produces join keys from the streamed input.
 *                             用于生成Streamed输入侧的Join keys的Projection
 * @param bufferedKeyGenerator a projection that produces join keys from the buffered input.
 *                             用于生成Buffered输入侧的Join keys的Projection
 * @param keyOrdering an ordering which can be used to compare join keys.
 *                    用于比较Join Keys的比较器
 * @param streamedIter an input whose rows will be streamed.
 *                     Streamed输入侧的迭代器
 * @param bufferedIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 *                     Buffered输入侧的迭代器，用于在Join keys相同时构建行的序列。
 */
private[joins] class SortMergeJoinScanner(
    streamedKeyGenerator: Projection,
    bufferedKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    streamedIter: RowIterator,
    bufferedIter: RowIterator) {
  private[this] var streamedRow: InternalRow = _ // Streamed表迭代器所指向的数据行
  private[this] var streamedRowKey: InternalRow = _ // Streamed表当前指向的数据行的Join Key
  private[this] var bufferedRow: InternalRow = _ // Buffered表迭代器所指向的数据行
  // Note: this is guaranteed to never have any null columns:
  private[this] var bufferedRowKey: InternalRow = _ // Buffered表当前指向的数据行的Join Key，需要保证不会有null列
  /**
   * The join key for the rows buffered in `bufferedMatches`, or null if `bufferedMatches` is empty
   * `bufferedMatches` 中缓冲的行的连接键，如果 `bufferedMatches` 为空，则为 null。
   */
  private[this] var matchJoinKey: InternalRow = _
  /** Buffered rows from the buffered side of the join. This is empty if there are no matches.
   * Buffered侧的缓冲行。 如果没有匹配项，则为空。
   **/
  private[this] val bufferedMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // Initialization (note: do _not_ want to advance streamed here).
  // 初始化（注意：不要在这里推进Streamed侧）。
  advancedBufferedToRowWithNullFreeJoinKey()

  // --- Public methods ---------------------------------------------------------------------------

  // 提供给外界调用的获取当前满足Join条件的单个streamedRow的方法
  def getStreamedRow: InternalRow = streamedRow

  // 提供给外界调用的获取当前满足Join条件的多个bufferedRow的方法
  def getBufferedMatches: ArrayBuffer[InternalRow] = bufferedMatches

  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   *
   * 得到满足Inner Join条件的数据。
   *
   * @return true if matching rows have been found and false otherwise. If this returns true, then
   *         [[getStreamedRow]] and [[getBufferedMatches]] can be called to construct the join
   *         results.
   */
  final def findNextInnerJoinRows(): Boolean = {
    while (advancedStreamed() && streamedRowKey.anyNull) {
      // Advance the streamed side of the join until we find the next row whose join key contains
      // no nulls or we hit the end of the streamed iterator.
      /**
       * 推进Streamed侧，直到找到一条数据的Join Keys不包含Null值的行，或者到Streamed Iterator末尾。
       */
    }
    if (streamedRow == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      // 已消费完streamedRow，不会有更多的匹配行了，将相关属性重置，返回false
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
      // The new streamed row has the same join key as the previous row, so return the same matches.
      // 能够消费到新的streamedRow，但是这个新行与前一行的Join key相同，因此不用处理，直接返回匹配的行即可；返回false
      true
    } else if (bufferedRow == null) {
      // The streamed row's join key does not match the current batch of buffered rows and there are
      // no more rows to read from the buffered iterator, so there can be no more matches.
      // streamedRow连接键与当前批次的bufferedRow不匹配，并且没有更多行要从Buffered迭代器读取，因此不能再有匹配项。
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      // Advance both the streamed and buffered iterators to find the next pair of matching rows.
      // 同时推进Streamed和Buffered侧的迭代器，找到下一个匹配的行

      // 比较当前streamedRow和bufferedRow的Join keys
      var comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)

      /**
       * while遍历过程中会择机推进Streamed Iterator和Buffered Iterator，终止条件如下：
       * 1. streamedRow为null，说明Streamed侧已经没数据了，需要终止循环。
       * 2. bufferedRow为null，说明Buffered侧已经没数据了，需要终止循环。
       * 3. 在streamedRow和bufferedRow都不为null，且比较后Join keys相等，comp等于0，说明得到了匹配行，需要终止循环。
       */
      do {
        if (streamedRowKey.anyNull) {
          // streamedRow的Join keys存在Null值，就推进Streamed Iterator
          advancedStreamed()
        } else {
          assert(!bufferedRowKey.anyNull) // bufferedRow的Join keys不可有Null值

          // 比较当前streamedRow和bufferedRow的Join keys
          comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)

          // 如果streamedRow的Join keys大于bufferedRow的Join keys，说明bufferedRow是落后的，需要向前推进
          if (comp > 0) advancedBufferedToRowWithNullFreeJoinKey()
          // 否则说明streamedRow是落后的，需要向前推进
          else if (comp < 0) advancedStreamed()
        }
      } while (streamedRow != null && bufferedRow != null && comp != 0)

      /**
       * 走到此处，两种可能：
       * 1. Streamed Iterator或Buffered Iterator没有多余的数据了。
       * 2. 找到了一个匹配行。
       */
      if (streamedRow == null || bufferedRow == null) {
        // We have either hit the end of one of the iterators, so there can be no more matches.
        // 此处命中第一种情况，重置相关属性，返回false。
        matchJoinKey = null
        bufferedMatches.clear()
        false
      } else {
        // The streamed row's join key matches the current buffered row's join, so walk through the
        // buffered iterator to buffer the rest of the matching rows.
        // 此处命中第二种情况，因此遍历Buffered Iterator以缓冲其余匹配行。
        assert(comp == 0)
        bufferMatchingRows()
        true
      }
    }
  }

  /**
   * Advances the streamed input iterator and buffers all rows from the buffered input that
   * have matching keys.
   *
   * 得到满足Outer Join条件的数据。只处理Left Outer或Right Outer类型的Join。
   * Full Outer Join参考 [[SortMergeFullOuterJoinScanner]]。
   *
   * @return true if the streamed iterator returned a row, false otherwise. If this returns true,
   *         then [[getStreamedRow]] and [[getBufferedMatches]] can be called to produce the outer
   *         join results.
   */
  final def findNextOuterJoinRows(): Boolean = {
    if (!advancedStreamed()) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      // 未匹配到行时，Streamed Row是需要保留的，而Buffered Row需要填充为Null，因此迭代的终止条件只会是Streamed Iterator迭代到末尾。
      // 已消费完streamedRow，不会有更多的匹配行了，将相关属性重置，返回false
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
        // Matches the current group, so do nothing.
        // Streamed Row在Buffered Iterator中匹配到了行
      } else {
        // The streamed row does not match the current group.
        // Streamed Row在Buffered Iterator中未匹配到行，清空相关属性
        matchJoinKey = null
        bufferedMatches.clear()

        if (bufferedRow != null && !streamedRowKey.anyNull) {
          // The buffered iterator could still contain matching rows, so we'll need to walk through
          // it until we either find matches or pass where they would be found.
          // Buffered Iterator仍然可以包含匹配的行，因此我们需要继续遍历它，直到找到匹配行或者跳过未匹配的行。
          var comp = 1

          /**
           * while循环的终止条件，任意一个满足即可：
           * 1. Streamed Row的连接键小于或等于Buffered Row的连接键。
           * 2. Buffered Iterator已经没有多余的行了。
           */
          do {
            comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          } while (comp > 0 && advancedBufferedToRowWithNullFreeJoinKey())

          if (comp == 0) {
            // We have found matches, so buffer them (this updates matchJoinKey)
            // 如果comp为0，说明Buffered Iterator里找到了匹配行，将剩余所有匹配的Buffered Row缓存到bufferedMatches里
            bufferMatchingRows()
          } else {
            // We have overshot the position where the row would be found, hence no matches.
          }
        }
      }
      // If there is a streamed input then we always return true
      true
    }
  }

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the streamed iterator and compute the new row's join key.
   *
   * Stream表迭代器移动得到新的streamedRow。
   *
   * @return true if the streamed iterator returned a row and false otherwise.
   *         表示Stream表是否还有数据。
   */
  private def advancedStreamed(): Boolean = {
    if (streamedIter.advanceNext()) { // 推进StreamIter，更新Stream表的Row和Join Key
      // 行赋值给streamedRow持有
      streamedRow = streamedIter.getRow
      // Join keys由streamedRowKey持有
      streamedRowKey = streamedKeyGenerator(streamedRow)
      true
    } else {
      // 已经迭代到末尾了，相关属性置为null，直接返回false
      streamedRow = null
      streamedRowKey = null
      false
    }
  }

  /**
   * Advance the buffered iterator until we find a row with join key that does not contain nulls.
   *
   * Buffered表迭代器移动得到新的bufferedRow。
   *
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  private def advancedBufferedToRowWithNullFreeJoinKey(): Boolean = {
    var foundRow: Boolean = false

    // 迭代bufferedIter迭代器
    while (!foundRow && bufferedIter.advanceNext()) {
      // 能够找到bufferedRow，赋值给属性进行记录
      bufferedRow = bufferedIter.getRow

      // 提取bufferedRow的Join keys
      bufferedRowKey = bufferedKeyGenerator(bufferedRow)
      foundRow = !bufferedRowKey.anyNull // 跳过有任意Join列的值为null的行
    }

    // 如果一直没有找到列全为Null的行，就将属性都置为Null，返回false；否则返回true
    if (!foundRow) {
      bufferedRow = null
      bufferedRowKey = null
      false
    } else {
      true
    }
  }

  /**
   * Called when the streamed and buffered join keys match in order to buffer the matching rows.
   *
   * 在streamedTable与bufferedTable两个数据表进行迭代时，如果当前streamedRow和bufferedRow能够满足Join条件，
   * 那么将继续移动bufferedIter，将bufferedTable中满足条件的所有数据行一次性找出，存储到bufferedMatches中，
   * bufferMatchingRows方法实现了该逻辑。
   */
  private def bufferMatchingRows(): Unit = {
    // 确保各类条件满足
    assert(streamedRowKey != null)
    assert(!streamedRowKey.anyNull)
    assert(bufferedRowKey != null)
    assert(!bufferedRowKey.anyNull)
    assert(keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)

    // This join key may have been produced by a mutable projection, so we need to make a copy:
    // 匹配的streamedRow
    matchJoinKey = streamedRowKey.copy()

    // 将bufferedMatches缓冲区清空
    bufferedMatches.clear()

    // 不断迭代，将Buffered Iterator中从当前bufferedRow开始的所有剩余匹配行全部加到bufferedMatches缓冲区中。
    do {
      bufferedMatches += bufferedRow.copy() // need to copy mutable rows before buffering them
      advancedBufferedToRowWithNullFreeJoinKey()
    } while (bufferedRow != null && keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
  }
}

/**
 * An iterator for outputting rows in left outer join.
 */
private class LeftOuterIterator(
    smjScanner: SortMergeJoinScanner,
    rightNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric)
  extends OneSideOuterIterator(
    smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
}

/**
 * An iterator for outputting rows in right outer join.
 */
private class RightOuterIterator(
    smjScanner: SortMergeJoinScanner,
    leftNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric)
  extends OneSideOuterIterator(smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
}

/**
 * An abstract iterator for sharing code between [[LeftOuterIterator]] and [[RightOuterIterator]].
 *
 * Each [[OneSideOuterIterator]] has a streamed side and a buffered side. Each row on the
 * streamed side will output 0 or many rows, one for each matching row on the buffered side.
 * If there are no matches, then the buffered side of the joined output will be a null row.
 *
 * In left outer join, the left is the streamed side and the right is the buffered side.
 * In right outer join, the right is the streamed side and the left is the buffered side.
 *
 * 迭代器抽象类，主要是LeftOuterIterator和RightOuterIterator的共享代码。
 *
 * 每个OneSideOuterIterator有一个Streamed侧和一个Buffered侧。
 * Streamed侧的每行数据会输出0行或多行数据，Buffered侧的每个匹配行对应一个行。
 * 如果没有匹配行，Buffered侧会填充为Null。
 *
 * 在Left Outer Join里，左表是Streamed侧，右表是Buffered侧。
 * 在Right Outer Join里，右表是Streamed侧，左表是Buffered侧。
 *
 * @param smjScanner a scanner that streams rows and buffers any matching rows
 *                   用于获取Streamed Row和Buffered Row的Scanner。
 * @param bufferedSideNullRow the default row to return when a streamed row has no matches
 *                            当Streamed Row没有匹配行时Buffered侧返回的默认行。
 * @param boundCondition an additional filter condition for buffered rows
 *                       对Buffered Row进行过滤的额外条件。
 * @param resultProj how the output should be projected
 *                   生成输出结果的Projection。
 * @param numOutputRows an accumulator metric for the number of rows output
 *                      记录输出行的度量累加器。
 */
private abstract class OneSideOuterIterator(
    smjScanner: SortMergeJoinScanner,
    bufferedSideNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric) extends RowIterator {

  // A row to store the joined result, reused many times
  // 用于存储Join结果的行，会被重复利用
  protected[this] val joinedRow: JoinedRow = new JoinedRow()

  // Index of the buffered rows, reset to 0 whenever we advance to a new streamed row
  // Buffered Row的索引，当推进到一个新的Streamed Row时，该索引会被置为0
  private[this] var bufferIndex: Int = 0

  // This iterator is initialized lazily so there should be no matches initially
  assert(smjScanner.getBufferedMatches.length == 0)

  // Set output methods to be overridden by subclasses
  // 设置output输出的方法，会被子类复写。
  protected def setStreamSideOutput(row: InternalRow): Unit
  protected def setBufferedSideOutput(row: InternalRow): Unit

  /**
   * Advance to the next row on the stream side and populate the buffer with matches.
   *
   * 推进下一行Streamed Row，同时填充匹配的Buffered
   *
   * @return whether there are more rows in the stream to consume.
   */
  private def advanceStream(): Boolean = {
    bufferIndex = 0

    // 使用SortMergeJoinScanner尝试推进下一个Streamed Row，返回true表示还有数据，返回false表示Join处理完了
    if (smjScanner.findNextOuterJoinRows()) { // 推进到下一个Streamed Row
      // 将推进得到的Streamed Row设置Streamed侧的输出
      setStreamSideOutput(smjScanner.getStreamedRow)

      // 获取当前Streamed Row匹配的Buffered Rows
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching rows in the buffer, so return the null row
        // 不存在Buffered Rows，设置Buffered侧为Null值。
        setBufferedSideOutput(bufferedSideNullRow)
      } else {
        // Find the next row in the buffer that satisfied the bound condition
        // 存在Buffered Rows，需要不断推进，直到判断条件不满足，就将Buffered侧设置为Null值。
        if (!advanceBufferUntilBoundConditionSatisfied()) {
          setBufferedSideOutput(bufferedSideNullRow)
        }
      }
      true
    } else {
      // Stream has been exhausted
      false
    }
  }

  /**
   * Advance to the next row in the buffer that satisfies the bound condition.
   *
   * 推进获取下一个Buffered侧满足连接条件的行
   *
   * @return whether there is such a row in the current buffer.
   */
  private def advanceBufferUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false

    // 不断遍历SortMergeJoinScanner的bufferedMatches，直到找到匹配的行或bufferedMatches为空
    while (!foundMatch && bufferIndex < smjScanner.getBufferedMatches.length) {

      // 设置Buffered Row为Buffered侧的输出
      setBufferedSideOutput(smjScanner.getBufferedMatches(bufferIndex))

      // 判断是否满足连接条件
      foundMatch = boundCondition(joinedRow)
      bufferIndex += 1
    }
    foundMatch
  }

  // 推进到下一行
  override def advanceNext(): Boolean = {
    /**
     * 推进Buffered Row，直到无法满足条件，此时会推进Streamed Row。
     */
    val r = advanceBufferUntilBoundConditionSatisfied() || advanceStream()
    if (r) numOutputRows += 1
    r
  }

  // 获取下一行数据
  override def getRow: InternalRow = resultProj(joinedRow)
}

private class SortMergeFullOuterJoinScanner(
    leftKeyGenerator: Projection,
    rightKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    leftIter: RowIterator,
    rightIter: RowIterator,
    boundCondition: InternalRow => Boolean,
    leftNullRow: InternalRow,
    rightNullRow: InternalRow)  {
  // 用于返回的组合Row
  private[this] val joinedRow: JoinedRow = new JoinedRow()

  // 左表当前行
  private[this] var leftRow: InternalRow = _
  // 左表当前行的连接键
  private[this] var leftRowKey: InternalRow = _
  // 右表当前行
  private[this] var rightRow: InternalRow = _
  // 右表当前行的连接键
  private[this] var rightRowKey: InternalRow = _

  // 左表索引
  private[this] var leftIndex: Int = 0
  // 右表索引
  private[this] var rightIndex: Int = 0

  // 左表匹配行缓冲区
  private[this] val leftMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  // 右表匹配行缓冲区
  private[this] val rightMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // 记录左表中匹配的行
  private[this] var leftMatched: BitSet = new BitSet(1)
  // 记录右表中匹配的行
  private[this] var rightMatched: BitSet = new BitSet(1)

  // 初始化时推动左右表获取下一行
  advancedLeft()
  advancedRight()

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the left iterator and compute the new row's join key.
   *
   * 推动左表迭代器，同时计算新行的连接键
   *
   * @return true if the left iterator returned a row and false otherwise.
   *         如果左表能返回一个行，则返回true
   */
  private def advancedLeft(): Boolean = {
    if (leftIter.advanceNext()) { // 推动获取下一行
      // 记录左表下一行数据和连接键
      leftRow = leftIter.getRow
      leftRowKey = leftKeyGenerator(leftRow)
      true
    } else {
      leftRow = null
      leftRowKey = null
      false
    }
  }

  /**
   * Advance the right iterator and compute the new row's join key.
   *
   * 推动右表迭代器，同时计算新行的连接键
   *
   * @return true if the right iterator returned a row and false otherwise.
   *         如果右表能返回一个行，则返回true
   */
  private def advancedRight(): Boolean = {
    if (rightIter.advanceNext()) { // 推动获取下一行
      // 记录右表下一行数据和连接键
      rightRow = rightIter.getRow
      rightRowKey = rightKeyGenerator(rightRow)
      true
    } else {
      rightRow = null
      rightRowKey = null
      false
    }
  }

  /**
   * Populate the left and right buffers with rows matching the provided key.
   * This consumes rows from both iterators until their keys are different from the matching key.
   *
   * 使用与提供的键匹配的行填充左右缓冲区。 这会消耗来自两个迭代器的行，直到它们的键与匹配键不同。
   */
  private def findMatchingRows(matchingKey: InternalRow): Unit = {
    // 清理缓冲区，重置相关变量
    leftMatches.clear()
    rightMatches.clear()
    leftIndex = 0
    rightIndex = 0

    // 左表连接键不为空，且与匹配键相同，就一直将左表数据行加入到leftMatches缓冲区里
    while (leftRowKey != null && keyOrdering.compare(leftRowKey, matchingKey) == 0) {
      leftMatches += leftRow.copy()
      advancedLeft()
    }

    // 右表连接键不为空，且与匹配键相同，就一直将左表数据行加入到rightMatches缓冲区里
    while (rightRowKey != null && keyOrdering.compare(rightRowKey, matchingKey) == 0) {
      rightMatches += rightRow.copy()
      advancedRight()
    }

    // 限定leftMatched BitSet的大小，让其刚好满足leftMatches缓冲区的大小
    if (leftMatches.size <= leftMatched.capacity) {
      leftMatched.clearUntil(leftMatches.size)
    } else {
      leftMatched = new BitSet(leftMatches.size)
    }

    // 限定rightMatched BitSet的大小，让其刚好满足rightMatches缓冲区的大小
    if (rightMatches.size <= rightMatched.capacity) {
      rightMatched.clearUntil(rightMatches.size)
    } else {
      rightMatched = new BitSet(rightMatches.size)
    }
  }

  /**
   * Scan the left and right buffers for the next valid match.
   *
   * Note: this method mutates `joinedRow` to point to the latest matching rows in the buffers.
   * If a left row has no valid matches on the right, or a right row has no valid matches on the
   * left, then the row is joined with the null row and the result is considered a valid match.
   *
   * 扫描左表或右表缓冲区，以找到下一个合法的匹配。
   *
   * 注意：这个方法改变了 `joinedRow` ，让它指向缓冲区中最后的匹配行。
   * 如果左表的行没有匹配到合法的右表的行，或者右表的行没有匹配到合法的左表的行，
   * 那么会把没匹配到的部分置为Null，同时认为是合法的匹配。
   *
   * @return true if a valid match is found, false otherwise.
   */
  private def scanNextInBuffered(): Boolean = {
    while (leftIndex < leftMatches.size) { // 外层遍历左表匹配的行
      while (rightIndex < rightMatches.size) { // 内存遍历右表匹配的行
        // 组合为JoinedRow
        joinedRow(leftMatches(leftIndex), rightMatches(rightIndex))

        // 进行条件判断
        if (boundCondition(joinedRow)) {
          // 满足条件，将左右表当前匹配的索引记录下来
          leftMatched.set(leftIndex)
          rightMatched.set(rightIndex)

          // 匹配到了，右表匹配缓冲区右表索引自增1
          rightIndex += 1

          // 终止遍历，返回true
          return true
        }
        // 未匹配到，右表匹配缓冲区右表索引也自增1
        rightIndex += 1
      }

      // 当前右表匹配缓冲区已经遍历完了，将右表匹配缓冲区索引置为0
      rightIndex = 0

      // 如果左表的行没有匹配当右表的行，需要将右表部分的字段设置为Null
      if (!leftMatched.get(leftIndex)) {
        // the left row has never matched any right row, join it with null row
        joinedRow(leftMatches(leftIndex), rightNullRow)
        leftIndex += 1
        return true
      }
      leftIndex += 1
    }

    // 遍历到这里，说明左表匹配行遍历完了，需要处理剩余右表的匹配数据

    // 遍历右表剩余数据
    while (rightIndex < rightMatches.size) {
      if (!rightMatched.get(rightIndex)) { // 如果存在未匹配上的右表的行，就将左表部分的字段设置为Null
        // the right row has never matched any left row, join it with null row
        joinedRow(leftNullRow, rightMatches(rightIndex))
        rightIndex += 1

        // 终止遍历，返回true
        return true
      }
      rightIndex += 1
    }

    // There are no more valid matches in the left and right buffers
    false
  }

  // --- Public methods --------------------------------------------------------------------------

  // 用于获取当前连接的结果行
  def getJoinedRow(): JoinedRow = joinedRow

  // 用于推动获取下一个连接行
  def advanceNext(): Boolean = {
    // If we already buffered some matching rows, use them directly
    // 如果已经缓存了一些匹配行，直接使用它们
    if (leftIndex <= leftMatches.size || rightIndex <= rightMatches.size) {
      if (scanNextInBuffered()) {
        return true
      }
    }

    // 走到这里，说明之前在两个表的缓冲区内都没有匹配行


    if (leftRow != null && (leftRowKey.anyNull || rightRow == null)) {
      /**
       * 当满足下面的条件，则保留左表的当前行，右表的字段置为Null即可
       * 1. 左表当前行不为null。
       * 2. 左表当前行的连接键有Null值，或者右表当前行为null。
       */
      joinedRow(leftRow.copy(), rightNullRow)
      advancedLeft()
      true
    } else if (rightRow != null && (rightRowKey.anyNull || leftRow == null)) {
      /**
       * 当满足下面的条件，则保留右表的当前行，左表的字段置为Null即可
       * 1. 右表当前行不为null。
       * 2. 右表当前行的连接键有Null值，或者左表当前行为null。
       */
      joinedRow(leftNullRow, rightRow.copy())
      advancedRight()
      true
    } else if (leftRow != null && rightRow != null) {
      // Both rows are present and neither have null values,
      // so we populate the buffers with rows matching the next key

      /**
       * 左右表的当前行都不为空，需要比较当前行的连接键的大小：
       * 1. 如果左表当前行连接键小于等于右表当前行连接键，则推进左表，将推进的数据全部加入左表缓冲区，直到遇到比当前左表连接键大的行。
       * 2. 如果右表当前行连接键大于右表当前行连接键，则推进右表，将推进的数据全部加入右表缓冲区，直到遇到比当前右表连接键大的行。
       */
      val comp = keyOrdering.compare(leftRowKey, rightRowKey)
      if (comp <= 0) {
        findMatchingRows(leftRowKey.copy())
      } else {
        findMatchingRows(rightRowKey.copy())
      }

      // 两表推进完成后，在缓冲区中进行匹配判断，以得到连接好的组合行
      scanNextInBuffered()
      true
    } else {
      // Both iterators have been consumed
      false
    }
  }
}

private class FullOuterIterator(
    smjScanner: SortMergeFullOuterJoinScanner,
    resultProj: InternalRow => InternalRow,
    numRows: SQLMetric) extends RowIterator {

  // 使用SortMergeFullOuterJoinScanner的getJoinedRow()方法获取当前连接的行数据
  private[this] val joinedRow: JoinedRow = smjScanner.getJoinedRow()

  override def advanceNext(): Boolean = {
    // 使用SortMergeFullOuterJoinScanner的advanceNext()方法推动下一行，返回true表示能获取到
    val r = smjScanner.advanceNext()
    if (r) numRows += 1
    r
  }

  // 返回下一行数据
  override def getRow: InternalRow = resultProj(joinedRow)
}
