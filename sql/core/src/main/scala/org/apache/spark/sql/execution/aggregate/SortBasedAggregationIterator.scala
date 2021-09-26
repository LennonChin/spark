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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * An iterator used to evaluate [[AggregateFunction]]. It assumes the input rows have been
 * sorted by values of [[groupingExpressions]].
 *
 * 对应SortAggregateExec
 *
 * @param groupingExpressions 分组表达式
 * @param valueAttributes 值属性，一般是Aggregate子节点的输出属性
 * @param inputIterator 数据输入迭代器
 * @param aggregateExpressions 聚合表达式
 * @param aggregateAttributes 聚合属性
 * @param initialInputBufferOffset 初始化的InputBuffer的Offset
 * @param resultExpressions 结果表达式
 * @param newMutableProjection 新的可变Projection，是一个函数类型，用于聚合多个缓冲区的数据
 * @param numOutputRows 输出行数度量器
 */
class SortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    valueAttributes: Seq[Attribute],
    inputIterator: Iterator[InternalRow], //
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    numOutputRows: SQLMetric)
  extends AggregationIterator(
    groupingExpressions,
    valueAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   *
   * 创建一个新的Aggregation Buffer并为所有聚合函数初始化缓冲值。
   */
  private def newBuffer: InternalRow = {
    // 取得所有聚合函数的BufferSchema
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    // 每行的Buffer大小
    val bufferRowSize: Int = bufferSchema.length

    // 可变Buffer
    val genericMutableBuffer = new GenericInternalRow(bufferRowSize)

    // Unsafe Buffer，当Schema中所有格的属性类型都可以使用Mutable时，就用UnsafeBuffer
    // 可用类型NullType、BooleanType、ByteType、ShortType、IntegerType、LongType、FloatType、DoubleType、DateType、TimestampType、DecimalType
    val useUnsafeBuffer = bufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)

    val buffer = if (useUnsafeBuffer) {
      // 得到UnsafeRow
      val unsafeProjection =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
      unsafeProjection.apply(genericMutableBuffer)
    } else {
      // 得到GenericInternalRow
      genericMutableBuffer
    }
    // 初始化Buffer，该操作会为所有聚合函数设定初始结果值
    initializeBuffer(buffer)
    buffer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  // 当前分组表达式
  private[this] var currentGroupingKey: UnsafeRow = _

  // The partition key of next partition.
  // 下一个分组表达式
  private[this] var nextGroupingKey: UnsafeRow = _

  // The first row of next partition.
  // 下一个分组的第一行数据
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  // 标记在有序的输入迭代器中还存在新的组
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  // 聚合缓冲区
  private[this] val sortBasedAggregationBuffer: InternalRow = newBuffer

  // This safe projection is used to turn the input row into safe row. This is necessary
  // because the input row may be produced by unsafe projection in child operator and all the
  // produced rows share one byte array. However, when we update the aggregate buffer according to
  // the input row, we may cache some values from input row, e.g. `Max` will keep the max value from
  // input row via MutableProjection, `CollectList` will keep all values in an array via
  // ImperativeAggregate framework. These values may get changed unexpectedly if the underlying
  // unsafe projection update the shared byte array. By applying a safe projection to the input row,
  // we can cut down the connection from input row to the shared byte array, and thus it's safe to
  // cache values from input row while updating the aggregation buffer.
  /**
   * 此安全投影用于将输入行转换为安全行。
   * 这是必要的，因为输入行可能是由子运算符中的不安全投影产生的，并且所有产生的行共享一个字节数组。
   * 但是，当我们根据输入行更新聚合缓冲区时，我们可能会缓存输入行中的一些值，
   * 例如 `Max` 将通过 MutableProjection 保留输入行的最大值，`CollectList` 将通过 ImperativeAggregate 框架将所有值保存在一个数组中。
   * 如果底层不安全投影更新共享字节数组，这些值可能会意外更改。
   * 通过对输入行应用安全投影，我们可以减少从输入行到共享字节数组的连接，因此在更新聚合缓冲区时缓存输入行中的值是安全的。
   *
   * 该方法使用GenerateSafeProjection来创建安全投影，最终是通过Codegen生成SpecificSafeProjection类的实例。
   */
  private[this] val safeProj: Projection = FromUnsafeProjection(valueAttributes.map(_.dataType))

  // 初始化基本信息
  protected def initialize(): Unit = {
    if (inputIterator.hasNext) { // 还存在输入数据

      // 初始化Aggregation Buffer
      initializeBuffer(sortBasedAggregationBuffer)

      // 获取下一行输入数据（InternalRow类型）
      val inputRow = inputIterator.next()
      // 使用根据分组表达式创建的UnsafeProjection对这行数据进行Project，得到分组键
      nextGroupingKey = groupingProjection(inputRow).copy()
      // 保存这行数据为firstRowInNextGroup
      firstRowInNextGroup = inputRow.copy()

      // 将可迭代标记置为true
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      // 无输入数据，可迭代标记为false
      sortedInputHasNewGroup = false
    }
  }

  // 首先会调用该初始化方法
  initialize()

  /** Processes rows in the current group. It will stop when it find a new group.
   *
   * 得到最终的聚合结果，依赖Aggregation Iterator的功能。
   *
   * 处理当前分组的数据。
   **/
  protected def processCurrentSortedGroup(): Unit = {
    // 开始遍历，将当前Grouping Key指向下一个Group Key
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    // 标记是否找到下一个分区
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    // 处理当前分组的第一行数据，数据处理之前首先通过safeProj将currentRow从Unsafe类型转换为Safe类型。
    processRow(sortBasedAggregationBuffer, safeProj(firstRowInNextGroup))

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // 还未迭代到下一个分区，且迭代器还有数据
    while (!findNextPartition && inputIterator.hasNext) {
      // Get the grouping key.
      // 获取当前数据
      val currentRow = inputIterator.next()
      // 获取Grouping key
      val groupingKey = groupingProjection(currentRow)

      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) { // Grouping key与当前Grouping key一直，处理数据
        processRow(sortBasedAggregationBuffer, safeProj(currentRow))
      } else { // 否则可能是找到一个新组，对变量进行更新
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = sortedInputHasNewGroup

  override final def next(): UnsafeRow = {
    if (hasNext) { // 还有未处理完的数据行
      // Process the current group.
      // 处理当前分组
      processCurrentSortedGroup()
      // Generate output row for the current group.
      // 生成当前分组的聚合值
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      // 初始化下一个分组的缓冲值
      initializeBuffer(sortBasedAggregationBuffer)
      numOutputRows += 1

      // 返回当前分组的聚合结果
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer)
  }
}
