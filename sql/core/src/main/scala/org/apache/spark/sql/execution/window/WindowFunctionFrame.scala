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

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp


/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
 *
 * 一个WindowFunctionFrame计算一个窗框的多个窗口函数的结果。
 * 在使用Frame之前，必须通过将当前分区中的所有行数据传递给它以做准备。
 * 准备好后，可以调用update方法来填充输出行。
 */
private[window] abstract class WindowFunctionFrame {
  /**
   * Prepare the frame for calculating the results for a partition.
   *
   * 准备WindowFunctionFrame以计算单个分组的结果
   *
   * @param rows to calculate the frame results for.
   *             用于计算结果的行缓冲区，里面是当前分组的行
   */
  def prepare(rows: RowBuffer): Unit

  /**
   * Write the current results to the target row.
   *
   * 将当前结果写入目标行。
   */
  def write(index: Int, current: InternalRow): Unit
}

/**
 * The offset window frame calculates frames containing LEAD/LAG statements.
 *
 * OffsetWindowFunctionFrame用于计算包含LEAD或LAG的窗框。
 * OffsetWindowFunctionFrame不需要AggregateProcessor，因为只是取前后行中的列数据，不需要聚合。
 *
 * @param target to write results to.
 *               需要写入结果的目标行
 * @param ordinal the ordinal is the starting offset at which the results of the window frame get
 *                written into the (shared) target row. The result of the frame expression with
 *                index 'i' will be written to the 'ordinal' + 'i' position in the target row.
 *                ordinal是窗口框架的结果写入共享目标行的起始偏移量。
 *                索引为 'i' 的帧表达式的结果将写入目标行中的 'ordinal' + 'i' 位置。
 * @param expressions to shift a number of rows.
 *                    用于定位行号的OffsetWindowFunction
 * @param inputSchema required for creating a projection.
 *                    输入Schema，用于创建Projection
 * @param newMutableProjection function used to create the projection.
 *                             用于创建Projection的函数
 * @param offset by which rows get moved within a partition.
 *               用于标记移动到了分区中的哪一行。该值有可能是负值，例如LAG(col, 1)，此时要取往上1行的col列的值，因此offset是-1
 */
private[window] final class OffsetWindowFunctionFrame(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed.
   * 当前处理的分区的行数据缓冲区
   **/
  private[this] var input: RowBuffer = null

  /** Index of the input row currently used for output.
   * 用于输出的当前输入行的索引
   **/
  private[this] var inputIndex = 0

  /**
   * Create the projection used when the offset row exists.
   * Please note that this project always respect null input values (like PostgreSQL).
   *
   * 创建当偏移行存在时使用的投影。
   * 请注意，该投影始终维护null输入值（如 PostgreSQL）。
   */
  private[this] val projection = {
    // Collect the expressions and bind them.
    val inputAttrs = inputSchema.map(_.withNullability(true))
    // Ordinal之前的列是NoOp，即不会有任何投影操作，只投影聚合列
    val boundExpressions = Seq.fill(ordinal)(NoOp) ++ expressions.toSeq.map { e =>
      BindReferences.bindReference(e.input, inputAttrs)
    }

    // Create the projection.
    newMutableProjection(boundExpressions, Nil).target(target)
  }

  /** Create the projection used when the offset row DOES NOT exists.
   * 创建当偏移行不存在时使用的投影，使用列的默认值。
   **/
  private[this] val fillDefaultValue = {
    // Collect the expressions and bind them.
    val inputAttrs = inputSchema.map(_.withNullability(true))

    // Ordinal之前的列是NoOp，即不会有任何投影操作，聚合列投影为默认值或Null字面值，只有LEAD和LAG有默认值
    val boundExpressions = Seq.fill(ordinal)(NoOp) ++ expressions.toSeq.map { e =>
      if (e.default == null || e.default.foldable && e.default.eval() == null) {
        // The default value is null.
        Literal.create(null, e.dataType)
      } else {
        // The default value is an expression.
        BindReferences.bindReference(e.default, inputAttrs)
      }
    }

    // Create the projection.
    newMutableProjection(boundExpressions, Nil).target(target)
  }

  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    // drain the first few rows if offset is larger than zero
    inputIndex = 0

    // 当offset > 0时，说明需要往下取第offset行，因此前offset行都可以跳过
    while (inputIndex < offset) {
      input.next()
      inputIndex += 1
    }
    inputIndex = offset
  }

  override def write(index: Int, current: InternalRow): Unit = {
    if (inputIndex >= 0 && inputIndex < input.size) {
      // 当还有剩余数据行，使用projection进行投影获取聚合函数的结果，即LEAD或LAG的结果
      val r = input.next()
      projection(r)
    } else {
      // Use default values since the offset row does not exist.
      // 当没有剩余行，使用默认值填充
      fillDefaultValue(current)
    }
    inputIndex += 1
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 *
 * 移动窗框处理，类似 ... BETWEEN 1 PRECEDING AND 1 FOLLOWING 的SQL。
 *
 * @param target to write results to.
 *               需要写入结果的目标行。
 * @param processor to calculate the row values with.
 *                  处理行数据的AggregateProcessor
 * @param lbound comparator used to identify the lower bound of an output row.
 *               用于定位输出行的下边界的比较器
 * @param ubound comparator used to identify the upper bound of an output row.
 *               用于定位输出行的上边界的比较器
 */
private[window] final class SlidingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed.
   * 当前处理的分区的行数据缓冲区。
   **/
  private[this] var input: RowBuffer = null

  /** The next row from `input`.
   * 下一个输入行
   **/
  private[this] var nextRow: InternalRow = null

  /** The rows within current sliding window.
   * 当前滑动窗口的行数据，是一个队列，包含多行。
   **/
  private[this] val buffer = new util.ArrayDeque[InternalRow]()

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   *
   * 第一个行值大于上边界的输入行的索引。
   */
  private[this] var inputHighIndex = 0

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   *
   * 第一个行值大于等于下边界的输入行的索引。
   */
  private[this] var inputLowIndex = 0

  /** Prepare the frame for calculating a new partition. Reset all variables.
   * 为计算新的分组准备WindowFunctionFrame，重置所有变量。
   **/
  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    nextRow = rows.next()

    // 值都置为0，滑动窗口缓冲区清空
    inputHighIndex = 0
    inputLowIndex = 0
    buffer.clear()
  }

  /** Write the frame columns for the current row to the given target row.
   * 将当前行的窗框列聚合值写入给定的目标行。
   **/
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    // 将输入行中所有排序列值小于等于输出行的上边界的行添加到缓冲区中，上边界可以理解为区间的右边界，例如[20, 22]，上边界就是22。
    while (nextRow != null && ubound.compare(nextRow, inputHighIndex, current, index) <= 0) {
      // 添加进入到上边界的行
      buffer.add(nextRow.copy())
      // 推进到下一行
      nextRow = input.next()
      // 维护计数器和更新标记
      inputHighIndex += 1
      bufferUpdated = true
    }

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    // 将输入行中所有排序列值小于输出行的下边界的行从缓冲区中移除，下边界可以理解为区间的左边界，例如[20, 22]，上边界就是20。
    while (!buffer.isEmpty && lbound.compare(buffer.peek(), inputLowIndex, current, index) < 0) {
      // 移除超出下边界的行
      buffer.remove()
      // 维护计数器和更新标记
      inputLowIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) { // bufferUpdated为true，说明缓冲区发生了变化，需要进行计算
      // 初始化AggregateProcessor
      processor.initialize(input.size)

      // 迭代缓冲区中的行，使用AggregateProcessor的update方法处理
      val iter = buffer.iterator()
      while (iter.hasNext) {
        processor.update(iter.next())
      }

      // 将计算结果填充到target中
      processor.evaluate(target)
    }
  }
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * 分组的全部数据处理，类似于 ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING 或者没有定义窗框的SQL
 *
 * Its results are the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 *
 * 它的结果对于分组中的每一行都是相同的。 此类可以看作是滑动窗口的特殊情况，但针对无界的情况进行了优化。
 *
 * @param target to write results to.
 *               需要写入结果的目标行。
 * @param processor to calculate the row values with.
 *                  处理行数据的AggregateProcessor
 */
private[window] final class UnboundedWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor)
  extends WindowFunctionFrame {

  /** Prepare the frame for calculating a new partition. Process all rows eagerly.
   *
   * 为计算新的分组准备WindowFunctionFrame，直接立即处理所有行。
   **/
  override def prepare(rows: RowBuffer): Unit = {
    val size = rows.size

    // 初始化
    processor.initialize(size)

    // 迭代行数据，使用AggregateProcessor的update方法处理。
    var i = 0
    while (i < size) {
      processor.update(rows.next())
      i += 1
    }
  }

  /** Write the frame columns for the current row to the given target row.
   * 将当前行的窗口函数聚合列写入给定的目标行。
   **/
  override def write(index: Int, current: InternalRow): Unit = {
    // Unfortunately we cannot assume that evaluation is deterministic. So we need to re-evaluate
    // for each row.
    processor.evaluate(target)
  }
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *
 * 扩张框的数据处理，类似于 ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 的SQL。
 *
 * There is only an upper bound. Very common use cases are for instance running sums or counts
 * (row_number). Technically this is a special case of a sliding window. However a sliding window
 * has to maintain a buffer, and it must do a full evaluation everytime the buffer changes. This
 * is not the case when there is no lower bound, given the additive nature of most aggregates
 * streaming updates and partial evaluation suffice and no buffering is needed.
 *
 * 只有一个上界。 非常常见的用例是例如计算总和或计数 (row_number)。
 * 从技术上讲，这是滑动窗口的特例。 然而，滑动窗口必须维护一个缓冲区，并且每次缓冲区更改时都必须进行完整评估。
 * 当没有下限时，情况并非如此，因为大多数聚合流更新和部分评估的附加性质就足够了，并且不需要缓冲。
 *
 * @param target to write results to.
 *               需要写入结果的目标行。
 * @param processor to calculate the row values with.
 *                  处理行数据的AggregateProcessor
 * @param ubound comparator used to identify the upper bound of an output row.
 *               用于定位输出行的上边界的比较器
 */
private[window] final class UnboundedPrecedingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed.
   * 当前处理的分区的行数据缓冲区。
   **/
  private[this] var input: RowBuffer = null

  /** The next row from `input`.
   * 下一个输入行
   **/
  private[this] var nextRow: InternalRow = null

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   *
   * 第一个行值大于上边界的输入行的索引。
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition.
   * 为计算新的分组准备WindowFunctionFrame。
   **/
  override def prepare(rows: RowBuffer): Unit = {
    // 持有输入行缓冲区
    input = rows
    // 获取下一行
    nextRow = rows.next()
    inputIndex = 0

    // 初始化AggregateProcessor
    processor.initialize(input.size)
  }

  /** Write the frame columns for the current row to the given target row.
   * 将当前行的窗框列聚合值写入给定的目标行。
   **/
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    // 将输入行中所有排序列值小于等于输出行的上边界的行使用AggregateProcessor的update进行计算
    // 上边界可以理解为区间的右边界，例如[∞, 22]，上边界就是22。
    while (nextRow != null && ubound.compare(nextRow, inputIndex, current, index) <= 0) {
      // 使用AggregateProcessor的update方法进行聚合
      processor.update(nextRow)
      nextRow = input.next()
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      // 将计算结果填充到target中
      processor.evaluate(target)
    }
  }
}

/**
 * The UnboundFollowing window frame calculates frames with the following SQL form:
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 *
 * 缩减框的数据处理，类似于 ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING 的SQL。
 *
 * There is only an upper bound. This is a slightly modified version of the sliding window. The
 * sliding window operator has to check if both upper and the lower bound change when a new row
 * gets processed, where as the unbounded following only has to check the lower bound.
 *
 * 只有一个上限。 这是滑动窗口的稍微修改版本。 当处理新行时，滑动窗口运算符必须检查上限和下限是否都发生了变化，而无界跟随者只需检查下限。
 *
 * This is a very expensive operator to use, O(n * (n - 1) /2), because we need to maintain a
 * buffer and must do full recalculation after each row. Reverse iteration would be possible, if
 * the commutativity of the used window functions can be guaranteed.
 *
 * 这是一个使用起来非常昂贵的运算符，O(n * (n - 1) /2)，因为我们需要维护一个缓冲区，并且必须在每一行之后进行完整的重新计算。
 * 如果可以保证所用窗口函数的可交换性，则可以进行反向迭代。
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[window] final class UnboundedFollowingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed.
   * 当前处理的分区的行数据缓冲区。
   **/
  private[this] var input: RowBuffer = null

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   *
   * 第一个行值大于等于下边界的输入行的索引。
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition.
   * 为计算新的分组准备WindowFunctionFrame。
   **/
  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    inputIndex = 0
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Duplicate the input to have a new iterator
    // 复制输入数据
    val tmp = input.copy()

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    // 从缓冲区中跳过那些输入行的值小于输出行下边界的行，下边界可以理解为区间的左边界，例如[20, ∞]，下边界就是20。
    tmp.skip(inputIndex)
    var nextRow = tmp.next()
    while (nextRow != null && lbound.compare(nextRow, inputIndex, current, index) < 0) {
      nextRow = tmp.next()
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      // 初始化AggregateProcessor
      processor.initialize(input.size)

      // 迭代输入行，使用AggregateProcessor的update方法处理
      while (nextRow != null) {
        processor.update(nextRow)
        nextRow = tmp.next()
      }

      // 将计算结果填充到target中
      processor.evaluate(target)
    }
  }
}
