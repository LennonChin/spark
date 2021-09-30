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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, UnsafeKVExternalSorter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

/**
 * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If
 * this map cannot allocate memory from memory manager, it spills the map into disk
 * and creates a new one. After processed all the input, then merge all the spills
 * together using external sorter, and do sort-based aggregation.
 *
 * The process has the following step:
 *  - Step 0: Do hash-based aggregation.
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create an external sorter based on the spilled sorted map entries and reset the map.
 *  - Step 3: Get a sorted [[KVIterator]] from the external sorter.
 *  - Step 4: Repeat step 0 until no more input.
 *  - Step 5: Initialize sort-based aggregation on the sorted iterator.
 * Then, this iterator works in the way of sort-based aggregation.
 *
 * The code of this class is organized as follows:
 *  - Part 1: Initializing aggregate functions.
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
 *  - Part 3: Methods and fields used by hash-based aggregation.
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
 *  - Part 5: Methods and fields used by sort-based aggregation.
 *  - Part 6: Loads input and process input rows.
 *  - Part 7: Public methods of this iterator.
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 *
 *
 *
 * 用于评估执行函数的迭代器。 它在 [[UnsafeRow]] 上运行。
 *
 * 这个迭代器首先使用基于散列的聚合来处理输入行。 它使用HashMap来存储组及其相应的聚合缓冲区。
 * 如果这个映射不能从内存管理器分配内存，它会将Map溢出到磁盘并创建一个新的。
 * 处理完所有输入后，然后使用外部排序器将所有溢出合并在一起，并进行基于排序的聚合。
 *
 * 该过程有以下步骤：
 * 1. 处理基于Hash的聚合。
 * 2. 对HashMap中所有的键值对基于分组表达式进行排序，并溢写到磁盘上。
 * 3. 根据溢出的有序Map中的键值对创建外部排序器并重置HashMap。
 * 4. 从外部排序器获取排好序的KVIterator。
 * 5. 重复第1步直到没有输入记录。
 * 5. 在已排序的迭代器上初始化基于排序的聚合。
 *
 * 然后，这个迭代器以基于排序的聚合方式工作。
 *
 * 该类的代码组织如下：
 * 1. 初始化聚合函数。
 * 2. 用于设置聚合缓冲区的方法和字段，处理从inputIter输入的数据行，生成输出数据行。
 * 3. 基于Hash的聚合器使用的方法和字段。
 * 4. 当切换到基于排序的聚合器使用方法和字段。
 * 5. 基于排序的聚合器使用的方法和字段。
 * 6. 加载输入并处理输入行。
 * 7. 迭代器的公共方法.
 * 8. 当没有输入数据和分组表达式时用于生成结果的工具函数.
 *
 * @param groupingExpressions
 *   expressions for grouping keys
 * @param aggregateExpressions
 * [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]],
 * [[PartialMerge]], or [[Final]].
 * @param aggregateAttributes the attributes of the aggregateExpressions'
 *   outputs when they are stored in the final aggregation buffer.
 * @param resultExpressions
 *   expressions for generating output rows.
 * @param newMutableProjection
 *   the function used to create mutable projections.
 * @param originalInputAttributes
 *   attributes of representing input rows from `inputIter`.
 * @param inputIter
 *   the iterator containing input [[UnsafeRow]]s.
 *
 * 对应于HashAggregateExec
 */
class TungstenAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[(Int, Int)],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric)
  extends AggregationIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  // 初始化聚合函数。
  ///////////////////////////////////////////////////////////////////////////

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  /**
   * 在执行这个算子之前记住这个任务的溢出数据大小，这样我们就可以算出我们为这个算子溢出了多少字节。
   */
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  // 用于设置聚合缓冲区的方法和字段，处理从inputIter输入的数据行，生成输出数据行。
  ///////////////////////////////////////////////////////////////////////////

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  /**
   * 创建一个新的聚合缓冲区并初始化缓冲区的值。
   * 这个函数最多只能被调用两次（当我们创建HashMap时，以及当我们为基于排序的聚合创建重用的缓冲区时）。
   */
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericInternalRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to generate output rows.
  // 创建用于生成输出行的函数
  override protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct

    /**
     * 对于非Final或非Complete的阶段性聚合，需要将分组键一并返回，
     * 其他情况可以直接交给父类处理。
     */
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)

      // SpecificUnsafeRowJoiner类型，Codegen生成
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    } else {
      super.generateResultProjection()
    }
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  // 包含初始值的聚合缓冲区，用于初始化其他聚合缓冲区。
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get().taskMemoryManager(),
    1024 * 16, // initial capacity，16KB
    TaskContext.get().taskMemoryManager().pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  // 触发聚合操作，得到最终的聚合结果，依赖AggregationIterator的功能。
  private def processInputs(fallbackStartsAt: (Int, Int)): Unit = {
    if (groupingExpressions.isEmpty) { // 分组表达式为空，只要在获取输入数据的迭代过程中不断调用processRow函数来处理数据即可
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      // 不断迭代并处理数据
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(buffer, newInput)
      }
    } else { // 分组表达式不为空
      var i = 0 // 记录分配失败的次数
      while (inputIter.hasNext) { // 不断取出输入数据
        val newInput = inputIter.next()
        // 获得Grouping key
        val groupingKey = groupingProjection.apply(newInput)
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt._2) {
          // 得到Grouping key在UnsafeFixedWidthAggregationMap中对应的Buffer
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }
        if (buffer == null) { // Buffer为空，意味着该Map内存空间已满
          // 尝试将内存数据spill到磁盘以释放内存空间。
          val sorter = hashMap.destructAndCreateExternalSorter()
          if (externalSorter == null) {
            externalSorter = sorter
          } else {
            externalSorter.merge(sorter)
          }
          i = 0
          // 再次获取Buffer，如果还是获取不到，说明内存不够，就抛出OOM异常
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
          if (buffer == null) {
            // failed to allocate the first page
            throw new OutOfMemoryError("No enough memory for aggregation")
          }
        }
        // 处理数据
        processRow(buffer, newInput)
        i += 1
      }

      /**
       * 如果externalSorter不为空，意味着聚合操作过程因为内存不足出现了磁盘溢写，聚合未成功。
       * 因此需要切换到SortBasedAggregation，
       * 先将溢写到磁盘的数据和Map中剩余的数据进行合并，释放Map空间，然后进行合并方式的切换。
       */
      if (externalSorter != null) {
        // 将剩余的数据溢写到磁盘
        val sorter = hashMap.destructAndCreateExternalSorter()
        // 与externalSorter中之前溢写的数据进行合并
        externalSorter.merge(sorter)
        // 释放Map
        hashMap.free()

        // 切换到SortBasedAggregation
        switchToSortBasedAggregation()
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  // 标记aggregationBufferMapIterator迭代器中是否还有键值对
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.
  private[this] var externalSorter: UnsafeKVExternalSorter = null

  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
   */
  private def switchToSortBasedAggregation(): Unit = {
    logInfo("falling back to sort based aggregation.")

    // Basically the value of the KVIterator returned by externalSorter
    // will be just aggregation buffer, so we rewrite the aggregateExpressions to reflect it.
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    sortBasedProcessRow = generateProcessRow(newExpressions, newFunctions, newInputAttributes)

    // Step 5: Get the sorted iterator from the externalSorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  // 标记是否使用基于排序的聚合。由于一开始是尝试使用基于Hash的聚合，所以该属性初始值为false。
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  // 标记在排序输入的迭代器中是否还有新的分组。
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // The function used to process rows in a group
  private[this] var sortBasedProcessRow: (InternalRow, InternalRow) => Unit = null

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    sortBasedProcessRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        sortBasedProcessRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey)
        firstRowInNextGroup.copyFrom(inputAggregationBuffer)
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.
   * 开始处理行数据
   */
  processInputs(testFallbackStartsAt.getOrElse((Int.MaxValue, Int.MaxValue)))

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  // 在processInputs方法中没有切换到基于Sort的聚合，可以从Map中预加载第一对键值对（用于让hasNext方法幂等）
  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    // 获取UnsafeFixedWidthAggregationMap的迭代器
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    // 预加载第一对键值对
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) { // 没有剩余的键值对了，就释放HashMap
      hashMap.free()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  // 判断是否还有未输出的分组
  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || // 使用基于排序的聚合时使用的判断
      (!sortBased && mapIteratorHasNext) // 使用基于Hash的聚合时使用的判断
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      // 需要判断是否切换为基于Sort的聚合，分别处理
      val res = if (sortBased) { // 进入if分支表示切换为基于Sort的聚合，需使用基于Sort的聚合方式处理组的聚合
        // Process the current group.
        // 处理当前分组
        processCurrentSortedGroup()
        // Generate output row for the current group.
        // 生成当前分组的输出行，这里需要把正常的列和聚合列合并起来
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        // 为下一个分组重置聚合缓冲区
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        // 没有切换为基于Sort的聚合，先生成输入行，这里需要把正常的列和聚合列合并起来
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        // 从KVIterator预加载下一行键值对，保持hasNext幂等
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          // 如果没有下一条记录，需要释放HashMap
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      // If this is the last record, update the task's peak memory usage. Since we destroy
      // the map to create the sorter, their memory usages should not overlap, so it is safe
      // to just use the max of the two.
      /**
       * 如果这是最后一条记录，则更新Task的峰值内存使用量。
       * 由于我们销毁HashMap以创建排序器，因此它们的内存使用不应重叠，因此仅使用两者中的最大值是安全的。
       */
      if (!hasNext) {
        // 取HashMap和ExternalSorter中峰值内存的较大者为使用的最大峰值内存
        val mapMemory = hashMap.getPeakMemoryUsedBytes
        val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
        val maxMemory = Math.max(mapMemory, sorterMemory)
        val metrics = TaskContext.get().taskMetrics()
        peakMemory += maxMemory

        // 记录溢写数据量
        spillSize += metrics.memoryBytesSpilled - spillSizeBefore
        metrics.incPeakExecutionMemory(maxMemory)
      }
      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create an output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
