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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._


/**
 * This class prepares and manages the processing of a number of [[AggregateFunction]]s within a
 * single frame. The [[WindowFunctionFrame]] takes care of processing the frame in the correct way,
 * this reduces the processing of a [[AggregateWindowFunction]] to processing the underlying
 * [[AggregateFunction]]. All [[AggregateFunction]]s are processed in [[Complete]] mode.
 *
 * [[SizeBasedWindowFunction]]s are initialized in a slightly different way. These functions
 * require the size of the partition processed, this value is exposed to them when the processor is
 * constructed.
 *
 * Processing of distinct aggregates is currently not supported.
 *
 * The implementation is split into an object which takes care of construction, and a the actual
 * processor class.
 *
 * 此类准备和管理单个窗框内多个 [[AggregateFunction]] 的处理。
 * [[WindowFunctionFrame]] 负责以正确的方式处理窗框，这减少了处理 [[AggregateWindowFunction]] 以处理底层 [[AggregateFunction]]。
 * 所有 [[AggregateFunction]] 都在 [[Complete]] 模式下处理。
 *
 * [[SizeBasedWindowFunction]] 的初始化方式略有不同。 这些函数需要处理的分区的大小，这个值在构造处理器时暴露给它们。
 *
 * 当前不支持处理不同的聚合。
 *
 * 该实现分为一个负责构造AggregateProcessor的伴生对象和一个实际的AggregateProcessor类。
 */
private[window] object AggregateProcessor {
  def apply(
      functions: Array[Expression], // 聚合函数数组
      ordinal: Int, // 当前Processor需要处理的聚合函数在聚合函数数组内的索引
      inputAttributes: Seq[Attribute], // 子节点输出数据
      newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
    : AggregateProcessor = {

    // 聚合缓冲属性
    val aggBufferAttributes = mutable.Buffer.empty[AttributeReference]

    // 初始化表达式
    val initialValues = mutable.Buffer.empty[Expression]

    // 更新表达式
    val updateExpressions = mutable.Buffer.empty[Expression]

    // 执行表达式，默认为NoOp
    val evaluateExpressions = mutable.Buffer.fill[Expression](ordinal)(NoOp)

    // ImperativeAggregate类型的聚合表达式数组
    val imperatives = mutable.Buffer.empty[ImperativeAggregate]

    // SPARK-14244: `SizeBasedWindowFunction`s are firstly created on driver side and then
    // serialized to executor side. These functions all reference a global singleton window
    // partition size attribute reference, i.e., `SizeBasedWindowFunction.n`. Here we must collect
    // the singleton instance created on driver side instead of using executor side
    // `SizeBasedWindowFunction.n` to avoid binding failure caused by mismatching expression ID.
    /**
     * SPARK-14244：首先在Driver端创建`SizeBasedWindowFunction`s，然后序列化到Executor端。
     * 这些函数都引用了一个全局单例窗口分区大小属性引用，即`SizeBasedWindowFunction.n`。
     * 这里我们必须收集Driver创建的单例实例，而不是使用Executor的`SizeBasedWindowFunction.n`，以避免表达式ID不匹配导致绑定失败。
     */
    val partitionSize: Option[AttributeReference] = {
      val aggs = functions.flatMap(_.collectFirst { case f: SizeBasedWindowFunction => f })
      aggs.headOption.map(_.n)
    }

    // Check if there are any SizeBasedWindowFunctions. If there are, we add the partition size to
    // the aggregation buffer. Note that the ordinal of the partition size value will always be 0.
    // 检查是否有任何 SizeBasedWindowFunctions。 如果有，我们将分区大小添加到聚合缓冲区。 请注意，分区大小值的序号将始终为 0。
    partitionSize.foreach { n =>
      aggBufferAttributes += n
      initialValues += NoOp
      updateExpressions += NoOp
    }

    // Add an AggregateFunction to the AggregateProcessor.
    // 向AggregateProcessor添加聚合函数
    functions.foreach {
      case agg: DeclarativeAggregate =>
        aggBufferAttributes ++= agg.aggBufferAttributes
        initialValues ++= agg.initialValues
        updateExpressions ++= agg.updateExpressions
        evaluateExpressions += agg.evaluateExpression
      case agg: ImperativeAggregate =>
        val offset = aggBufferAttributes.size
        val imperative = BindReferences.bindReference(agg
          .withNewInputAggBufferOffset(offset)
          .withNewMutableAggBufferOffset(offset),
          inputAttributes)
        imperatives += imperative
        aggBufferAttributes ++= imperative.aggBufferAttributes
        val noOps = Seq.fill(imperative.aggBufferAttributes.size)(NoOp)
        initialValues ++= noOps
        updateExpressions ++= noOps
        evaluateExpressions += imperative
      case other =>
        sys.error(s"Unsupported Aggregate Function: $other")
    }

    // Create the projections.
    val initialProj = newMutableProjection(initialValues, partitionSize.toSeq)
    val updateProj = newMutableProjection(updateExpressions, aggBufferAttributes ++ inputAttributes)
    val evalProj = newMutableProjection(evaluateExpressions, aggBufferAttributes)

    // Create the processor
    new AggregateProcessor(
      aggBufferAttributes.toArray,
      initialProj,
      updateProj,
      evalProj,
      imperatives.toArray,
      partitionSize.isDefined)
  }
}

/**
 * This class manages the processing of a number of aggregate functions. See the documentation of
 * the object for more information.
 */
private[window] final class AggregateProcessor(
    private[this] val bufferSchema: Array[AttributeReference], // 聚合缓冲区属性
    private[this] val initialProjection: MutableProjection, // 初始化操作的Projection
    private[this] val updateProjection: MutableProjection, // 更新操作的Projection
    private[this] val evaluateProjection: MutableProjection, // 最终执行获取结果的Projection
    private[this] val imperatives: Array[ImperativeAggregate], // ImperativeAggregate类型的聚合函数列表
    private[this] val trackPartitionSize: Boolean) { // 是否需要跟踪分组大小

  private[this] val join = new JoinedRow

  // ImperativeAggregate类型的聚合列的数量
  private[this] val numImperatives = imperatives.length

  // 创建聚合缓冲区
  private[this] val buffer = new SpecificInternalRow(bufferSchema.toSeq.map(_.dataType))

  // 将初始化操作和更新操作的Projection作用到聚合缓冲区上
  initialProjection.target(buffer)
  updateProjection.target(buffer)

  /** Create the initial state. */
  def initialize(size: Int): Unit = {
    // Some initialization expressions are dependent on the partition size so we have to
    // initialize the size before initializing all other fields, and we have to pass the buffer to
    // the initialization projection.
    if (trackPartitionSize) {
      // 0位置的列记录分区大小
      buffer.setInt(0, size)
    }

    // 初始化缓冲区
    initialProjection(buffer)

    // 迭代ImperativeAggregate聚合函数，用它们的initialize方法初始化缓冲区
    var i = 0
    while (i < numImperatives) {
      imperatives(i).initialize(buffer)
      i += 1
    }
  }

  /** Update the buffer.
   * 更新缓冲区
   **/
  def update(input: InternalRow): Unit = {
    // 使用更新操作Projection根据输入行更新缓冲区中DeclarativeAggregate类型的聚合列
    updateProjection(join(buffer, input))
    var i = 0

    // 迭代ImperativeAggregate聚合函数，用它们的update方法更新缓冲区中ImperativeAggregate类型的聚合列
    while (i < numImperatives) {
      imperatives(i).update(buffer, input)
      i += 1
    }
  }

  /** Evaluate buffer.
   * 使用evaluateProjection获取最终聚合结果，填充到传入的target行中
   **/
  def evaluate(target: InternalRow): Unit =
  evaluateProjection.target(target)(buffer)
}
