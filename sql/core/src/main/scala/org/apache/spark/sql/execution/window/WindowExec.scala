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

package org.apache.spark.sql.execution.window

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

/**
 * This class calculates and outputs (windowed) aggregates over the rows in a single (sorted)
 * partition. The aggregates are calculated for each row in the group. Special processing
 * instructions, frames, are used to calculate these aggregates. Frames are processed in the order
 * specified in the window specification (the ORDER BY ... clause). There are four different frame
 * types:
 * - Entire partition: The frame is the entire partition, i.e.
 *   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING. For this case, window function will take all
 *   rows as inputs and be evaluated once.
 * - Growing frame: We only add new rows into the frame, i.e. UNBOUNDED PRECEDING AND ....
 *   Every time we move to a new row to process, we add some rows to the frame. We do not remove
 *   rows from this frame.
 * - Shrinking frame: We only remove rows from the frame, i.e. ... AND UNBOUNDED FOLLOWING.
 *   Every time we move to a new row to process, we remove some rows from the frame. We do not add
 *   rows to this frame.
 * - Moving frame: Every time we move to a new row to process, we remove some rows from the frame
 *   and we add some rows to the frame. Examples are:
 *     1 PRECEDING AND CURRENT ROW and 1 FOLLOWING AND 2 FOLLOWING.
 * - Offset frame: The frame consist of one row, which is an offset number of rows away from the
 *   current row. Only [[OffsetWindowFunction]]s can be processed in an offset frame.
 *
 * Different frame boundaries can be used in Growing, Shrinking and Moving frames. A frame
 * boundary can be either Row or Range based:
 * - Row Based: A row based boundary is based on the position of the row within the partition.
 *   An offset indicates the number of rows above or below the current row, the frame for the
 *   current row starts or ends. For instance, given a row based sliding frame with a lower bound
 *   offset of -1 and a upper bound offset of +2. The frame for row with index 5 would range from
 *   index 4 to index 6.
 * - Range based: A range based boundary is based on the actual value of the ORDER BY
 *   expression(s). An offset is used to alter the value of the ORDER BY expression, for
 *   instance if the current order by expression has a value of 10 and the lower bound offset
 *   is -3, the resulting lower bound for the current row will be 10 - 3 = 7. This however puts a
 *   number of constraints on the ORDER BY expressions: there can be only one expression and this
 *   expression must have a numerical data type. An exception can be made when the offset is 0,
 *   because no value modification is needed, in this case multiple and non-numeric ORDER BY
 *   expression are allowed.
 *
 * This is quite an expensive operator because every row for a single group must be in the same
 * partition and partitions must be sorted according to the grouping and sort order. The operator
 * requires the planner to take care of the partitioning and sorting.
 *
 * The operator is semi-blocking. The window functions and aggregates are calculated one group at
 * a time, the result will only be made available after the processing for the entire group has
 * finished. The operator is able to process different frame configurations at the same time. This
 * is done by delegating the actual frame processing (i.e. calculation of the window functions) to
 * specialized classes, see [[WindowFunctionFrame]], which take care of their own frame type:
 * Entire Partition, Sliding, Growing & Shrinking. Boundary evaluation is also delegated to a pair
 * of specialized classes: [[RowBoundOrdering]] & [[RangeBoundOrdering]].
 *
 * 此类计算并输出（窗口化的）单个（已排序的）分区中的行的聚合。为组中的每一行计算聚合。
 * 特殊的处理指令，即帧，用于计算这些聚合。帧按照窗口规范（ORDER BY ... 子句）中指定的顺序进行处理。
 * 有四种不同的帧类型：
 *
 * - 整个分区：框架是整个分区，例如`UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`。 对于这种情况，窗口函数会将所有行作为输入并进行一次评估。
 * - 增长框架：我们只向框架中添加新行，例如`UNBOUNDED PRECEDING AND ....`，每次移动到新行进行处理时，我们都会向框架中添加一些行。 我们不会从此框架中删除行。
 * - 收缩框架：我们只从框架中删除行，例如`... AND UNBOUNDED FOLLOWING`，每次移动到新行进行处理时，我们从框架中删除一些行。 我们不向此框架添加行。
 * - 移动框架：每次我们移动到要处理的新行时，我们都会从框架中删除一些行，并向框架中添加一些行。
 *            例如`1 PRECEDING AND CURRENT ROW and 1 FOLLOWING AND 2 FOLLOWING`。
 * - 偏移帧：该帧由一行组成，即距当前行的偏移行数。偏移框仅适用于 [[OffsetWindowFunction]] 类型的窗口函数。
 *
 * 不同的帧边界可用于生长、收缩和移动帧。帧边界可以是基于行或范围的：
 *
 * - 基于行：基于行的边界基于行在分区内的位置。偏移量表示当前行上方或下方的行数，当前行的帧开始或结束。
 *          例如，给定一个基于行的滑动框架，其下限偏移为 -1，上限偏移为 +2。具有索引5的行的框架范围从索引4到索引6。
 * - 基于范围：基于范围的边界基于ORDER BY表达式的实际值。偏移量用于更改ORDER BY表达式的值，
 *          例如，如果当前order by表达式的值为10且下限偏移量为-3，则当前行的结果下限将为10 - 3 = 7。
 *          然而，这对ORDER BY表达式施加了许多限制：只能有一个表达式，并且该表达式必须具有数字数据类型。
 *          当偏移量为0时可以例外，因为不需要修改值，在这种情况下允许多个和非数字ORDER BY表达式。
 *
 * 这是一个非常昂贵的运算算子，因为单个组的每一行都必须在同一个分区中，并且分区必须根据分组和排序顺序进行排序。算子需要规划者来负责划分和排序。
 *
 * 运算算子是半阻塞的。窗口函数和聚合每次一组计算，只有在整个组的处理完成后才能提供结果。运算算子能够同时处理不同的帧配置。
 * 这是通过将实际的帧处理（例如窗口函数的计算）委托给专门的类来完成的，参见 [[WindowFunctionFrame]] ，
 * 它们负责处理自己的帧类型：整个分区、滑动、增长和收缩。边界评估也委托给一对专门的类：[[RowBoundOrdering]] 及 [[RangeBoundOrdering]]。
 *
 * 此类计算并输出（窗口化）单个（已排序）分区中的行的聚合。
 * 为组中的每一行计算聚合。 特殊的处理指令，即帧，用于计算这些聚合。
 * 帧按照窗口规范（ORDER BY ... 子句）中指定的顺序进行处理。 有四种不同的帧类型
 */
case class WindowExec(
    windowExpression: Seq[NamedExpression], // 窗口函数表达式
    partitionSpec: Seq[Expression], // 窗框内分区表达式列表
    orderSpec: Seq[SortOrder], // 窗框内排序节点列表
    child: SparkPlan) // 子节点
  extends UnaryExecNode {

  // 该节点的输出包括子节点的输出和窗口函数列的输出
  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)

  // 规定了输入数据分布情况，需要按照窗框的分区表达式在全局范围内分布
  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  /**
   * 规定了输入数据有序性。
   * 要求子节点输出的数据先按窗框内分区列升序排序，再按窗框内排序列排序。
   */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  // 输出数据的有序性与子节点输出数据的有序性一致
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // 输出数据的数据分布与子节点输出数据的数据分布一致
  override def outputPartitioning: Partitioning = child.outputPartitioning

  /**
   * Create a bound ordering object for a given frame type and offset. A bound ordering object is
   * used to determine which input row lies within the frame boundaries of an output row.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * 根据给定的FrameType和Offset创建BoundOrdering。
   * BoundOrdering对象用于确定哪个输入行位于输出行的帧边界内。
   *
   * 该方法使用了Code Generation，它只能在Executor端使用。
   *
   *
   * @param frameType to evaluate. This can either be Row or Range based.
   * @param offset with respect to the row.
   * @return a bound ordering object.
   */
  private[this] def createBoundOrdering(frameType: FrameType, offset: Int): BoundOrdering = {
    frameType match {
      // 匹配RangeFrame类型
      case RangeFrame =>
        val (exprs, current, bound) = if (offset == 0) {
          // Use the entire order expression when the offset is 0.
          // offset为0，使用整个排序表达式
          val exprs = orderSpec.map(_.child)
          val buildProjection = () => newMutableProjection(exprs, child.output)
          (orderSpec, buildProjection(), buildProjection())
        } else if (orderSpec.size == 1) {
          // Use only the first order expression when the offset is non-null.
          /**
           * 当offset不为0时，只能满足只有一个排序表达式窗口定义，即如果出现下面这种窗口定义：
           * ... ORDER BY a, b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
           * 这种情况是不支持的。
           */
          val sortExpr = orderSpec.head // 取第一个排序表达式
          val expr = sortExpr.child
          // Create the projection which returns the current 'value'.
          val current = newMutableProjection(expr :: Nil, child.output)
          // Flip the sign of the offset when processing the order is descending
          // 根据排序表达式的排序方式决定实际的offset
          val boundOffset = sortExpr.direction match {
            case Descending => -offset
            case Ascending => offset
          }
          // Create the projection which returns the current 'value' modified by adding the offset.
          // 创建offset对应的边界计算表达式，该表达式会根据当前行排序列的值，计算offset偏移量位置的值
          val boundExpr = Add(expr, Cast(Literal.create(boundOffset, IntegerType), expr.dataType))
          // 增加一个Projection节点
          val bound = newMutableProjection(boundExpr :: Nil, child.output)
          (sortExpr :: Nil, current, bound)
        } else {
          // 其他情况不支持，会报错
          sys.error("Non-Zero range offsets are not supported for windows " +
            "with multiple order expressions.")
        }
        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        // 创建排序节点，用于将当前值投影的结果与绑定值投影的结果进行比较。这是手动完成的，因为我们要使用Code Generation（如果已启用）。
        val sortExprs = exprs.zipWithIndex.map { case (e, i) =>
          SortOrder(BoundReference(i, e.dataType, e.nullable), e.direction)
        }
        val ordering = newOrdering(sortExprs, Nil)

        // 返回RangeBoundOrdering
        RangeBoundOrdering(ordering, current, bound)
      // 匹配RowFrame类型，由于是根据行偏移量进行匹配，不需要进行任何大小比较，直接返回RowBoundOrdering
      case RowFrame => RowBoundOrdering(offset)
    }
  }

  /**
   * Collection containing an entry for each window frame to process. Each entry contains a frames'
   * WindowExpressions and factory function for the WindowFrameFunction.
   *
   * 包含要处理的每个窗框的条目的集合。
   * 每个条目都包含一个窗框的 WindowExpressions 和 WindowFrameFunction 的工厂函数。
   */
  private[this] lazy val windowFrameExpressionFactoryPairs: Seq[(mutable.Buffer[Expression], InternalRow => WindowFunctionFrame)] = {
    type FrameKey = (String, FrameType, Option[Int], Option[Int])
    type ExpressionBuffer = mutable.Buffer[Expression]

    // 窗框函数
    val framedFunctions = mutable.Map.empty[FrameKey, (ExpressionBuffer, ExpressionBuffer)]

    // Add a function and its function to the map for a given frame.
    /**
     * 收集所有窗口函数的窗框信息到framedFunctions中
     * @param tpe 类型
     * @param fr 窗框定义
     * @param e 窗口表达式
     * @param fn 窗口函数
     */
    def collect(tpe: String, fr: SpecifiedWindowFrame, e: Expression, fn: Expression): Unit = {
      // 根据类型（AGGREGATE或OFFSET）、窗框类型（RANGE或ROW）、窗框起始边界作为键
      val key = (tpe, fr.frameType, FrameBoundary(fr.frameStart), FrameBoundary(fr.frameEnd))

      // 根据自定义键key去framedFunctions中查找，没有查找到就创建一个空的元组(ArrayBuffer, ArrayBuffer)
      val (es, fns) = framedFunctions.getOrElseUpdate(
        key, (ArrayBuffer.empty[Expression], ArrayBuffer.empty[Expression]))

      // 将窗口表达式和窗口函数加入到数组中
      es += e
      fns += fn
    }

    // Collect all valid window functions and group them by their frame.
    // 收集所有有效的窗口函数，根据它们的窗框进行分类
    windowExpression.foreach { x =>
      x.foreach {
        case e @ WindowExpression(function, spec) =>
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          // 根据窗口函数进行匹配
          function match {
            // 普通聚合函数
            case AggregateExpression(f, _, _, _) => collect("AGGREGATE", frame, e, f)
            // 窗口聚合函数，CumeDist、DenseRank、NTile、PercentRank、Rank、RowNumber
            case f: AggregateWindowFunction => collect("AGGREGATE", frame, e, f)
            // 偏移量函数，Lead和Lag两类
            case f: OffsetWindowFunction => collect("OFFSET", frame, e, f)
            // 其他情况不支持
            case f => sys.error(s"Unsupported window function: $f")
          }
        case _ =>
      }
    }

    // Map the groups to a (unbound) expression and frame factory pair.
    var numExpressions = 0

    /**
     * 遍历收集到的窗框函数，按照5种窗框类型进行分类。返回值是一个元组，
     * 元组第2个元素是匹配得到的InternalRow => WindowFunctionFrame函数，代表这个框处理行数据应该如何输入和输出。
     * 元组第1个元素是使用对应窗框类型WindowFunctionFrame所需要执行的窗口函数。
     */
    framedFunctions.toSeq.map { // 每一个元组的类型都是 (复合的Key, ArrayBuffer[WindowExpression], ArrayBuffer[FunctionExpression])
      case (key, (expressions, functionSeq)) =>
        val ordinal = numExpressions
        // 聚合函数数组
        val functions = functionSeq.toArray

        // Construct an aggregate processor if we need one.
        // 创建一个AggregateProcessor
        def processor = AggregateProcessor(
          functions, // 聚合函数数组
          ordinal, // 当前Processor需要处理的聚合函数在聚合函数数组内的索引
          child.output, // 子节点输出数据
          (expressions, schema) =>
            newMutableProjection(expressions, schema, subexpressionEliminationEnabled))

        // Create the factory
        /**
         * 根据具体的窗框创建工厂，有以下几类：
         * - OffsetWindowFunctionFrame：偏移框
         * - UnboundedPrecedingWindowFunctionFrame：扩张框
         * - UnboundedFollowingWindowFunctionFrame：收缩框
         * - SlidingWindowFunctionFrame：滑动狂
         * - UnboundedWindowFunctionFrame：全部数据分区
         */
        val factory = key match {
          // Offset Frame
          /**
           * 偏移框
           * OFFSET类型，RowFrame，框的起始和结束相同，仅包含一条数据，LAG、LEAD
           */
          case ("OFFSET", RowFrame, Some(offset), Some(h)) if offset == h =>
            target: InternalRow =>
              new OffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunctions.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  newMutableProjection(expressions, schema, subexpressionEliminationEnabled),
                offset)

          // Growing Frame.
          /**
           * 扩张框
           * AGGREGATE类型，RowFrame / RangeFrame，UNBOUNDED PRECEDING AND ...
           */
          case ("AGGREGATE", frameType, None, Some(high)) =>
            target: InternalRow => {
              new UnboundedPrecedingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, high))
            }

          // Shrinking Frame.
          /**
           * 收缩框
           * AGGREGATE，RowFrame / RangeFrame，... AND UNBOUNDED FOLLOWING
           */
          case ("AGGREGATE", frameType, Some(low), None) =>
            target: InternalRow => {
              new UnboundedFollowingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, low))
            }

          // Moving Frame.
          /**
           * 移动框
           * AGGREGATE，RowFrame / RangeFrame，low PRECEDING AND high FOLLOWING
           */
          case ("AGGREGATE", frameType, Some(low), Some(high)) =>
            target: InternalRow => {
              new SlidingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, low),
                createBoundOrdering(frameType, high))
            }

          // Entire Partition Frame.
          /**
           * 全部数据
           * AGGREGATE，RowFrame / RangeFrame，UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           */
          case ("AGGREGATE", frameType, None, None) =>
            target: InternalRow => {
              new UnboundedWindowFunctionFrame(target, processor)
            }
        }

        // Keep track of the number of expressions. This is a side-effect in a map...
        // 记录窗口表达式的数量
        numExpressions += expressions.size

        // Create the Frame Expression - Factory pair.
        // 返回 (ArrayBuffer[WindowExpression], WindowFunctionFrame) 元组
        (expressions, factory)
    }
  }

  /**
   * Create the resulting projection.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * 创建用于结果数据的Projection。
   *
   * 这个方法会使用Code Generation。它仅能在Executor端使用。
   *
   * @param expressions unbound ordered function expressions.
   * @return the final resulting projection.
   */
  private[this] def createResultProjection(expressions: Seq[Expression]): UnsafeProjection = {
    // 根据聚合函数表达式创建BoundReference，会把聚合函数得到的结果列放在子节点输出的结果列后面。
    val references = expressions.zipWithIndex.map{ case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      // 窗口函数表达式的结果列会处于子节点输出列的右边。
      BoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = expressions.zip(references).toMap

    // 遍历窗口函数产生的列表列，从生成的匹配Seq[BoundReference]列表中匹配对应的BoundReference
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToRefMap))

    // 创建Projection
    UnsafeProjection.create(
      child.output ++ patchedWindowExpression,
      child.output)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Unwrap the expressions and factories from the map.
    // 获取所有的窗口函数
    val expressions = windowFrameExpressionFactoryPairs.flatMap(_._1)
    // 获取所有的InternalRow => WindowFunctionFrame工厂
    val factories = windowFrameExpressionFactoryPairs.map(_._2).toArray

    // Start processing.
    /**
     * 遍历子节点输出的每个分区。
     * 注意，WindowExec子节点的输出每个分区内的数据是按照WindowExec的要求已经进行了全局分区和列排序了，
     * 每个分区内的数据先按照PARTITION BY的列排序，再按照ORDER BY的列排序，
     * 因此在顺序遍历子节点输出的单个分区数据时，可以保证相同的PARTITION BY列的数据是连续的，且是按照ORDER BY的顺序排序的。
     */
    child.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {

        // Get all relevant projections.
        // 创建结果数据的Projection
        val result = createResultProjection(expressions)

        // 创建分组键的Projection，用于从子节点输出数据中Project窗口定义里的PARTITION BY列
        val grouping = UnsafeProjection.create(partitionSpec, child.output)

        // Manage the stream and the grouping.
        var nextRow: UnsafeRow = null // 下一行
        var nextGroup: UnsafeRow = null // 下一个分组，结构是由分组列构成的行
        var nextRowAvailable: Boolean = false // 是否还有下一行

        // 获取分组内的下一行
        private[this] def fetchNextRow() {
          // 判断是否还有下一行
          nextRowAvailable = stream.hasNext
          if (nextRowAvailable) { // 存在下一行
            // 获取下一行，和具体的分组
            nextRow = stream.next().asInstanceOf[UnsafeRow]
            nextGroup = grouping(nextRow)
          } else {
            nextRow = null
            nextGroup = null
          }
        }
        fetchNextRow()

        // Manage the current partition.
        // 管理当前分组（是PARTITION BY的分区）
        // 存放当前分组内数据行的ArrayBuffer
        val rows = ArrayBuffer.empty[UnsafeRow]
        // 子节点输入列的数量
        val inputFields = child.output.length
        // 外排器
        var sorter: UnsafeExternalSorter = null
        // 行缓冲区
        var rowBuffer: RowBuffer = null
        // 窗口函数的结果行，在聚合过程中，窗口函数得到的聚合结果列都会存放在这个行中。
        val windowFunctionResult = new SpecificInternalRow(expressions.map(_.dataType))
        // 根据窗口函数的结果行，获取WindowFunctionFrame列表，此时每个WindowFunctionFrame都已经持有windowFunctionResult属性了
        val frames = factories.map(_(windowFunctionResult))
        val numFrames = frames.length

        // 获取下一个分区
        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup.
          // 记录当前分组
          val currentGroup = nextGroup.copy()

          // clear last partition
          // 清理上一个分组使用的External Sorter使用的资源
          if (sorter != null) {
            // the last sorter of this task will be cleaned up via task completion listener
            sorter.cleanupResources()
            sorter = null
          } else {
            rows.clear()
          }

          // 遍历当前分组内的行
          while (nextRowAvailable && nextGroup == currentGroup) {
            if (sorter == null) { // External Sorter为空，说明此时还未超过4096阈值

              // 先使用ArrayBuffer存放新的行
              rows += nextRow.copy()

              if (rows.length >= 4096) { // 如果ArrayBuffer数据量大于4096，则切换成UnsafeExternalSorter
                // We will not sort the rows, so prefixComparator and recordComparator are null.
                // 创建UnsafeExternalSorter
                sorter = UnsafeExternalSorter.create(
                  TaskContext.get().taskMemoryManager(),
                  SparkEnv.get.blockManager,
                  SparkEnv.get.serializerManager,
                  TaskContext.get(),
                  null, // 记录比较器为空
                  null, // 前缀比较器为空
                  1024,
                  SparkEnv.get.memoryManager.pageSizeBytes,
                  SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
                    UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD), // 8G
                  false)

                // 将ArrayBuffer中已存在的数据拷贝到该UnsafeExternalSorter中
                rows.foreach { r =>
                  sorter.insertRecord(r.getBaseObject, r.getBaseOffset, r.getSizeInBytes, 0, false)
                }
                // 清理ArrayBuffer
                rows.clear()
              }
            } else {
              // 存在UnsafeExternalSorter，说明已经超过了4096阈值，开始使用未安排其
              sorter.insertRecord(nextRow.getBaseObject, nextRow.getBaseOffset,
                nextRow.getSizeInBytes, 0, false)
            }
            // 获取下一行数据
            fetchNextRow()
          }

          // 当前分组数据已经全部遍历完，根据数据存储的方式构造行缓冲区
          if (sorter != null) {
            // 使用UnsafeExternalSorter存储行数据，使用ExternalRowBuffer作为缓冲区
            rowBuffer = new ExternalRowBuffer(sorter, inputFields)
          } else {
            // 使用ArrayBuffer存储行数据，使用ArrayRowBuffer作为缓冲区
            rowBuffer = new ArrayRowBuffer(rows)
          }

          // Setup the frames.
          // 遍历WindowFunctionFrame，调用prepare方法进行聚合前的准备
          var i = 0
          while (i < numFrames) {
            frames(i).prepare(rowBuffer.copy())
            i += 1
          }

          // Setup iteration
          // 处理完当前分区后，设置某些遍历用于遍历
          rowIndex = 0 // 当前分组内遍历到的行序号
          rowsSize = rowBuffer.size // 当前分组里的行数
        }

        // Iteration
        var rowIndex = 0
        var rowsSize = 0L

        // 判断是否还有数据可迭代，该方法由父节点调用
        override final def hasNext: Boolean = rowIndex < rowsSize || nextRowAvailable

        val join = new JoinedRow

        // 迭代获取下一条已经聚合好的数据，该方法由父节点调用
        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          // 当前分组行索引超过分组内行总数了，说明需要加载下一个分组
          if (rowIndex >= rowsSize && nextRowAvailable) {
            fetchNextPartition()
          }

          if (rowIndex < rowsSize) { // 当前分组行索引小于分组内行总数，说明分组内还有未迭代的数据
            // Get the results for the window frames.
            var i = 0

            /**
             * 使用每个WindowFunctionFrame的write方法，
             * 逐条处理rowBuffer中的数据，得到聚合结果，
             * write方法内部最终调用AggregateProcessor的update方法完成计算处理。
             */

            // 获取缓冲区内下一条行数据
            val current = rowBuffer.next()
            // 迭代所有WindowFunctionFrame，使用write方法写出
            while (i < numFrames) {
              frames(i).write(rowIndex, current)
              i += 1
            }

            // 'Merge' the input row with the window function result
            // 将聚合列和其他列合并为一个完整的行
            join(current, windowFunctionResult)
            rowIndex += 1 // 迭代索引自增

            // Return the projection.
            // 返回Projection包装的完整行
            result(join)
          } else throw new NoSuchElementException
        }
      }
    }
  }
}
