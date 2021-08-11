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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegralType, LongType}

trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression] // 连接左键
  val rightKeys: Seq[Expression] // 连接右键
  val joinType: JoinType // 连接类型
  val buildSide: BuildSide // 构建侧
  val condition: Option[Expression] // 连接条件
  val left: SparkPlan // 左表物理计划
  val right: SparkPlan // 右表物理计划

  // 输出的列，由Join类型决定。
  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }

  // 输出分区由流式表的输出分区决定。
  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  /**
   * 构建表在Join过程中会创建一个HashMap，用来支持数据的查找，属于“静态”的一方。
   * 流式表在Join过程中，一行一行地在构建表对应的HashMap中查找数据，属于“动态”的一方。
   */
  // 用来区分参与Join的两个数据表（构建表和流式表）的角色。
  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  // 用来区分参与Join的两个数据表（构建表和流式表）的角色。
  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys).map(BindReferences.bindReference(_, left.output))
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
      .map(BindReferences.bindReference(_, right.output))
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }



  protected def buildSideKeyGenerator(): Projection =
    UnsafeProjection.create(buildKeys)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedKeys)

  // 用来判断一行数据是否满足Join条件。
  @transient private[this] lazy val boundCondition = if (condition.isDefined) {
    newPredicate(condition.get, streamedPlan.output ++ buildPlan.output).eval _
  } else {
    (r: InternalRow) => true
  }


  /**
   * 当Join类型为LeftExistence时，创建的Projection的schema和output相同；
   * 否则，采用的schema为streamedPlan的输出加上buildPlan的输出。
   */
  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }

  // Inner join
  private def innerJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = { // hashedRelation构建表，起到HashMap的作用
    val joinRow = new JoinedRow
    val joinKeys = streamSideKeyGenerator()
    streamIter.flatMap { srow =>
      joinRow.withLeft(srow)
      val matches = hashedRelation.get(joinKeys(srow)) // 从构建表中获取key对应的行
      if (matches != null) {
        matches.map(joinRow.withRight(_)).filter(boundCondition)
      } else {
        Seq.empty
      }
    }
  }

  // Outer join
  private def outerJoin(
      streamedIter: Iterator[InternalRow],
    hashedRelation: HashedRelation): Iterator[InternalRow] = { // hashedRelation构建表，起到HashMap的作用
    val joinedRow = new JoinedRow()
    val keyGenerator = streamSideKeyGenerator()
    val nullRow = new GenericInternalRow(buildPlan.output.length)

    streamedIter.flatMap { currentRow =>
      val rowKey = keyGenerator(currentRow)
      joinedRow.withLeft(currentRow)
      val buildIter = hashedRelation.get(rowKey) // 从构建表中获取key对应的行
      new RowIterator {
        private var found = false
        override def advanceNext(): Boolean = {
          while (buildIter != null && buildIter.hasNext) {
            val nextBuildRow = buildIter.next()
            if (boundCondition(joinedRow.withRight(nextBuildRow))) {
              found = true
              return true
            }
          }
          if (!found) {
            joinedRow.withRight(nullRow)
            found = true
            return true
          }
          false
        }
        override def getRow: InternalRow = joinedRow
      }.toScala
    }
  }

  // Semi Join
  private def semiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = { // hashedRelation构建表，起到HashMap的作用
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key) // 从构建表中获取key对应的行
      !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })
    }
  }

  // Existence Join
  private def existenceJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = { // hashedRelation构建表，起到HashMap的作用
    val joinKeys = streamSideKeyGenerator()
    val result = new GenericInternalRow(Array[Any](null))
    val joinedRow = new JoinedRow
    streamIter.map { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key) // 从构建表中获取key对应的行
      val exists = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })
      result.setBoolean(0, exists)
      joinedRow(current, result)
    }
  }

  // Anti join
  private def antiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = { // hashedRelation构建表，起到HashMap的作用
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key) // 从构建表中获取key对应的行
      key.anyNull || buildIter == null || (condition.isDefined && !buildIter.exists {
        row => boundCondition(joinedRow(current, row))
      })
    }
  }

  // 根据不同的JoinType进行实际的Join方法调用
  protected def join(
      streamedIter: Iterator[InternalRow],
      hashed: HashedRelation, // 构建表，起到HashMap的作用
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    val joinedIter = joinType match {
      case _: InnerLike =>
        innerJoin(streamedIter, hashed)
      case LeftOuter | RightOuter =>
        outerJoin(streamedIter, hashed)
      case LeftSemi =>
        semiJoin(streamedIter, hashed)
      case LeftAnti =>
        antiJoin(streamedIter, hashed)
      case j: ExistenceJoin =>
        existenceJoin(streamedIter, hashed)
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastHashJoin should not take $x as the JoinType")
    }

    val resultProj = createResultProjection
    joinedIter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }
}

object HashJoin {
  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   *
   * If not, returns the original expressions.
   */
  private[joins] def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    // TODO: support BooleanType, DateType and TimestampType
    if (keys.exists(!_.dataType.isInstanceOf[IntegralType])
      || keys.map(_.dataType.defaultSize).sum > 8) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      Cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }
}
