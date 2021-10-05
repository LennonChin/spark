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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(j) =>
      UnsafeProjection.create(output, output)
    case other =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      newPredicate(condition.get, streamed.output ++ broadcast.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  /**
   * The implementation for InnerJoin.
   */
  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    // 遍历流式表的分区
    streamed.execute().mapPartitionsInternal { streamedIter =>

      // 获取构建表数据行
      val buildRows = relation.value

      // 连接的结果行
      val joinedRow = new JoinedRow

      // 遍历流式表分区里的行数据
      streamedIter.flatMap { streamedRow =>

        // 每行流式表数据都与构建表里的每行数据构成JoinedRow
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        if (condition.isDefined) { // 存在判断条件，就进行过滤
          joinedRows.filter(boundCondition)
        } else { // 不存在判断条件，就直接返回
          joinedRows
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  private def outerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {

    // 遍历流式表所有分区
    streamed.execute().mapPartitionsInternal { streamedIter =>

      // 获取构建表
      val buildRows = relation.value

      // 用于组合两表连接的行数据
      val joinedRow = new JoinedRow

      // 构建表填充为Null时的通用Row
      val nulls = new GenericInternalRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        // 当前流式表的行
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        // 是否能够找到与当前流式表匹配的行
        private var foundMatch: Boolean = false
        // the matched result row
        // 用于返回的匹配的结果行
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        // 构建表中下一个查找的行的索引
        private var nextIndex: Int = 0

        // 获取下一个匹配行，如果能获取到就返回true，且结果数据存放在resultRow中
        private def findNextMatch(): Boolean = {

          // 流式表当前行为空
          if (streamRow == null) {

            // 如果流式表没有剩余数据，直接返回false
            if (!streamedIter.hasNext) {
              return false
            }
            // 流式表后移一行，将相关属性置为初始值
            streamRow = streamedIter.next()
            nextIndex = 0
            foundMatch = false
          }

          // 遍历构建表
          while (nextIndex < buildRows.length) {
            // 将构建表每一行和流式表组合在一起
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            // 索引自增
            nextIndex += 1

            // 判断组合的数据是否满足条件，如果满足，就将foundMatch置为true，返回true
            if (boundCondition(resultRow)) {
              foundMatch = true
              return true
            }
          }

          if (!foundMatch) { // 如果没有匹配到
            // 就将构建表的列填为Null值，将streamRow置为null，下一次就会迭代获取流式表的下一行
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            // 返回true
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext(): Boolean = {
          resultRow != null || findNextMatch()
        }

        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftSemi with BuildRight
   *   Anti with BuildRight
   *
   * 处理Left Semi / Anti Join，其中右表为构建表
   */
  private def leftExistenceJoin(
      relation: Broadcast[Array[InternalRow]],
      exists: Boolean): RDD[InternalRow] = { // Semi Join时exists为true，Anti Join时exists为false
    assert(buildSide == BuildRight)

    // 遍历流式表的分区
    streamed.execute().mapPartitionsInternal { streamedIter =>

      // 获取构建表的行
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) { // 条件不为空

        // 从流式表中过滤数据，判断是否需要保留
        streamedIter.filter(l =>
          buildRows.exists(r => boundCondition(joinedRow(l, r))) == exists
        )

      } else if (buildRows.nonEmpty == exists) {
        /**
         * 此时condition条件为空，
         * 对于Semi Join，只要构建表存在数据，就应该返回流式表的数据
         * 对于Anti Join，只要构建表不存在数据，就应该返回流式表的数据
         */
        streamedIter
      } else {
        Iterator.empty
      }
    }
  }

  private def existenceJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    assert(buildSide == BuildRight)

    // 遍历流式表的每个分区
    streamed.execute().mapPartitionsInternal { streamedIter =>

      // 获取构建表数据
      val buildRows = relation.value

      // 用于组合两表连接的行数据
      val joinedRow = new JoinedRow

      if (condition.isDefined) { // 存在判断条件

        // 结果行，只有一个Boolean字段
        val resultRow = new GenericInternalRow(Array[Any](null))

        // 遍历流式表分区内每条数据
        streamedIter.map { row =>

          // 根据条件判断是否Exists
          val result = buildRows.exists(r => boundCondition(joinedRow(row, r)))

          // 将是否Exists结果存入resultRow
          resultRow.setBoolean(0, result)

          // 组合结果返回
          joinedRow(row, resultRow)
        }
      } else { // 不存在判断条件

        // 结果行，只有一个Boolean字段，值由构建表是否为空决定
        val resultRow = new GenericInternalRow(Array[Any](buildRows.nonEmpty))

        // 遍历流式表分区内每条数据
        streamedIter.map { row =>
          // 组合结果返回
          joinedRow(row, resultRow)
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildLeft：此状态下，流式表是空值填充表
   *   RightOuter with BuildRight：此状态下，流式表是空值填充表
   *   FullOuter：此状态下，两个表都是空值填充表
   *   LeftSemi with BuildLeft：此状态下构建表数据需要保留，流式表数据用于判断，不保留
   *   LeftAnti with BuildLeft：此状态下构建表数据需要保留，流式表数据用于判断，不保留
   *   ExistenceJoin with BuildLeft：此状态下构建表数据需要保留，流式表数据用于判断，不保留
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    /** All rows that either match both-way, or rows from streamed joined with nulls.
     * 所有行要么匹配，要么流式表的列置为Null。
     **/
    val streamRdd = streamed.execute()

    // 记录可以匹配上的构建表的行数据索引
    val matchedBuildRows: RDD[BitSet] = streamRdd.mapPartitionsInternal { streamedIter =>

      // 获取构建表的数据
      val buildRows = relation.value

      // 用BitSet结构来记录构建表里那些行需要保留
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      // 对流式表进行
      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          // 不断循环构建表，判断是否匹配，如果匹配就将索引记录在BitSet里。
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matched.set(i)
          }
          i += 1
        }
      }
      Seq(matched).toIterator
    }

    // 转换为单个的BitSet实例
    val matchedBroadcastRows: BitSet = matchedBuildRows.fold(
      new BitSet(relation.value.length)
    )(_ | _)

    // 根据不同的Join类型分别处理，此处只处理Left Semi / Anti Join 和 Existence Join
    joinType match {
      case LeftSemi => // Left Semi Join，只要构建表匹配上的，就需要保留，不需要流式表数据
        assert(buildSide == BuildLeft)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (matchedBroadcastRows.get(i)) {
            buf += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case j: ExistenceJoin => // Exists Join，需要记录exists标记
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          // 记录构建表中每条数据是否存在的exists标标记
          val result = new GenericInternalRow(Array[Any](matchedBroadcastRows.get(i)))
          // 疯转范围JoinedRow返回，构建表的数据全部需要，并且携带exists标记
          buf += new JoinedRow(rel(i).copy(), result)
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case LeftAnti => // Left Anti Join，只要构建表未匹配上的，就需要保留，不需要流式表数据
        val notMatched: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (!matchedBroadcastRows.get(i)) {
            notMatched += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(notMatched)
      case o =>
    }

    // 剩余的情况都是Outer Join了，此时需要保留构建表所有数据，Full Outer Join还需要考虑保留构建表里的Null值数据

    /**
     * 计算流式表中未匹配上的行，此时得到的数据如下：
     * 构建表保留行，流式表对应的列置为Null。
     */
    val notMatchedBroadcastRows: Seq[InternalRow] = {
      val nulls = new GenericInternalRow(streamed.output.size)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      val joinedRow = new JoinedRow
      // 左表置为Null
      joinedRow.withLeft(nulls)
      var i = 0
      val buildRows = relation.value

      // 遍历未匹配上的构建表所有的行
      while (i < buildRows.length) {
        if (!matchedBroadcastRows.get(i)) { // 构建表未匹配上，也需要保留
          // 构建表的数据放在右侧，左侧都是Null值
          buf += joinedRow.withRight(buildRows(i)).copy()
        }
        i += 1
      }
      buf
    }

    /**
     * 计算能够匹配上的两表数据，得到的数据如下：
     * 1. 流式表和构建表都不为Null的情况，这种是肯定要处理的。
     * 2. 如果是Full Outer Join，还要处理流式表保留，构建表为Null的情况。
     */
    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      // 遍历所有流式表的数据
      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          // 能匹配上，就记录
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
          i += 1
        }

        // 上面一条也没有匹配上，且是Full Outer Join，需要处理流式表保留，构建表填充Null的行
        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    /**
     * 将上述处理的两部分数据Union起来：
     * 1. 构建表保留行，流式表对应的列置为Null。
     * 2. 流式表和构建表都不为Null的情况，这种是肯定要处理的。
     * 3. 如果是Full Outer Join，还要处理流式表保留，构建表为Null的情况。
     *
     * 前两种数据是为了应对Left / Right Outer Join，最后一种数据是为了应对Full Outer Join
     */
    sparkContext.union(
      matchedStreamRows,
      sparkContext.makeRDD(notMatchedBroadcastRows)
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {

    // 获取广播的构建表Relation
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation)
      // Left Semi/Anti Join中，对于exists的要求是不一样的
      case (LeftSemi, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = true)
      case (LeftAnti, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = false)
      case (j: ExistenceJoin, BuildRight) =>
        existenceJoin(broadcastedRelation)
      case _ =>
        /**
         * LeftOuter with BuildLeft
         * RightOuter with BuildRight
         * FullOuter
         * LeftSemi with BuildLeft
         * LeftAnti with BuildLeft
         * ExistenceJoin with BuildLeft
         */
        defaultJoin(broadcastedRelation)
    }

    val numOutputRows = longMetric("numOutputRows")

    // 对结果进行投影
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }
}
