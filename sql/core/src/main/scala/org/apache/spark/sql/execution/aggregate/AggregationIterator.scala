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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
 * The base class of [[SortBasedAggregationIterator]] and [[TungstenAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 *    is used to generate result.
 *
 * SortBasedAggregationIterator和TungstenAggregationIterator的几类。
 * 它主要包含两部分：
 * 1. 初始化聚合函数。
 * 2. 基于聚合函数的聚合模式创建`processRow`和`generateOutput`两个函数，
 *    `processRow`是用于处理输入的函数，`generateOutput` 是用于生成结果的函数。
 *
 * 聚合执行框架指的是聚合过程中抽象出来的通用功能，包括聚合函数的初始化、聚合缓冲区更新合并函数和聚合结果生成函数等。
 * 这些功能都在聚合迭代器（Aggregation Iterator）中得到了实现。
 *
 * @param groupingExpressions 分组表达式
 * @param inputAttributes 输入属性
 * @param aggregateExpressions 聚合表达式
 * @param aggregateAttributes 聚合属性
 * @param initialInputBufferOffset 初始化的InputBuffer的Offset
 * @param resultExpressions 结果表达式
 * @param newMutableProjection 新的可变Projection，是一个函数类型
 */
abstract class AggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection)
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * The following combinations of AggregationMode are supported:
   * - Partial
   * - PartialMerge (for single distinct)
   * - Partial and PartialMerge (for single distinct)
   * - Final
   * - Complete (for SortAggregate with functions that does not support Partial)
   * - Final and Complete (currently not used)
   *
   * TODO: AggregateMode should have only two modes: Update and Merge, AggregateExpression
   * could have a flag to tell it's final or not.
   *
   * 对于聚合模式，下面的组合是被支持的：
   * - Partial
   * - PartialMerge（用于单个去重）
   * - Partial和PartialMerge（用于单个去重）
   * - Final
   * - Complete（用于不支持Partial模式的SortAggregate聚合函数）
   * - Final和Complete（目前没有被使用）
   */
  {
    // 对模式进行去重，要求模式数量小于等于2，在数量为2的情况下最多支持Partial和PartialMerge、Final和Complete两种组合
    val modes = aggregateExpressions.map(_.mode).distinct.toSet
    require(modes.size <= 2,
      s"$aggregateExpressions are not supported because they have more than 2 distinct modes.")
    require(modes.subsetOf(Set(Partial, PartialMerge)) || modes.subsetOf(Set(Final, Complete)),
      s"$aggregateExpressions can't have Partial/PartialMerge and Final/Complete in the same time.")
  }

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  // 聚合函数初始化
  protected def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression],
      startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0 // mutableBufferOffset的起始值从0开始
    var inputBufferOffset: Int = startingInputBufferOffset // inputBufferOffset由于可能存在其他信息，因此它的inputBufferOffset的起始值是不确定的
    val expressionsLength = expressions.length
    val functions = new Array[AggregateFunction](expressionsLength)
    var i = 0
    while (i < expressionsLength) { // 遍历表达式
      val func = expressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = expressions(i).mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] => // 针对Partial和Complete模式的ImperativeAggregate聚合函数
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          /**
           * 如果该函数不是基于表达式的聚合函数（不支持代码生成），并且该函数的模式是 Partial 或 Complete，
           * 则需要创建 BoundReferences，因为我们将在它的 update 方法中调用该函数子节点的 eval 聚合函数。
           * 这些 eval 调用需要 BoundReferences 才能工作。
           *
           * AttributeReference表达式会转换为BoundReference表达式。
           * 输出为每个AttributeReference的格式是：
           * BoundReference(AttributeReference的数据列索引, AttributeReference的类型, AttributeReference是否可为空)
           *
           * ImperativeAggregate(BindReference(ordinal, attribute type, nullable), ...)
           */
          BindReferences.bindReference(func, inputAttributes) // 返回还是ImperativeAggregate函数
        case _ => // 针对其他聚合函数
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          // 只需要在PartialMerge和Final聚合模式中为ImperativeAggregate聚合函数设置inputBufferOffset。

          // 需要时设置聚合函数的inputBufferOffset
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset) // 此时inputAggBufferOffset中记录了inputBufferOffset值
            case function => function
          }
          // 更新偏移量，这个操作主要是为了保证下一个聚合函数的inputBufferOffset在当前聚合函数之后；
          // aggBufferSchema是StructType类型，length即是其内部StructField的数量
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          /**
           * 为ImperativeAggregate类型的聚合函数设置mutableBufferOffset。
           * 在潜在的BindReference操作之后设置mutableBufferOffset是重要的，
           * 因此BindReference操作会为聚合函数构造一个新的实例。
           */

          // 设置ImperativeAggregate函数聚合缓冲区的偏移量（withNewMutableAggBufferOffset）
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      // 更新偏移量，这个操作主要是为了保证下一个聚合函数的mutableBufferOffset在当前聚合函数之后；
      // aggBufferSchema是StructType类型，length即是其内部StructField的数量
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  // 实例构造时就会对聚合函数进行初始化
  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2]
  /**
   * ImperativeAggregate类型的聚合函数在 `allAggregateFunctions` 中的位置。
   * 例如，我们有fun1、fun2、fun3、fun4四个聚合函数，fun2和fun3是ImperativeAggregate类型的聚合函数，
   * 那么ImperativeAggregateFunctionPositions即是[1, 2]。
   */
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // The projection used to initialize buffer values for all expression-based aggregates.
  // 用于对基于表达式的聚合中的Buffer进行初始化的Projection
  protected[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues // 聚合函数初始值
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      // 对于ImperativeAggregate类型的聚合函数相对应的位置，我们将使用NoOp表达式，而这些表达式在Projection Codegen过程中会被忽略。
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)
  }

  // All imperative AggregateFunctions.
  // 所有ImperativeAggregate类型的聚合函数
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  // Initializing functions used to process a row.
  /**
   * 数据处理函数生成
   * @return (InternalRow, InternalRow) => Unit类型的数据处理函数
   *         参数分别代表当前的聚合缓冲区和输入数据行
   */
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression], // 聚合表达式
      functions: Seq[AggregateFunction], // 聚合函数
      inputAttributes: Seq[Attribute]): (InternalRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      // 合并操作表达式
      val mergeExpressions: Seq[Expression] = functions.zipWithIndex.flatMap {
        case (ae: DeclarativeAggregate, i) =>
          expressions(i).mode match {
            case Partial | Complete => ae.updateExpressions // Partial或Complete，处理的原始输入数据，使用update expression
            case PartialMerge | Final => ae.mergeExpressions // PartialMerge或Final，处理的是聚合缓冲区，使用merge expression
          }
        case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp) // Seq[NoOp]
      }
      // 更新函数
      val updateFunctions: Array[(InternalRow, InternalRow) => Unit] = functions.zipWithIndex.collect {
        case (ae: ImperativeAggregate, i) =>
          expressions(i).mode match {
            case Partial | Complete => // Partial或Complete，调用的是update方法
              (buffer: InternalRow, row: InternalRow) => ae.update(buffer, row)
            case PartialMerge | Final => // PartialMerge或Final，调用的是merge方法
              (buffer: InternalRow, row: InternalRow) => ae.merge(buffer, row)
          }
      }.toArray
      // This projection is used to merge buffer values for all expression-based aggregates.
      // 该Projection用于合并所有基于表达式的聚合操作的缓冲值

      // 获取聚合缓冲区的Schema
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)

      // 这里会根据合并表达式及列属性，通过Codegen生成MutableProject对象。
      val updateProjection =
        newMutableProjection(mergeExpressions, aggregationBufferSchema ++ inputAttributes)

      // 最终返回的函数
      (currentBuffer: InternalRow, row: InternalRow) => {
        // Process all expression-based aggregate functions.
        /**
         * 设定updateProjection存储结果的InternalRow，
         * 处理所有基于Expression的聚合函数，即DeclarativeAggregate类型的聚合函数，
         * 这里会调用updateProjection的apply方法，传入joinedRow(currentBuffer, row)，
         * 在updateProjection内部，会根据传入的JoinedRow中的两行数据，进行合并计算，
         * (currentBuffer, row)中currentBuffer代表当前缓冲区，row代表输入数据行，
         * 最终合并计算的结果又会存入到currentBuffer中。
         */
        updateProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions.
        // 处理所有ImperativeAggregate类型的聚合函数。
        var i = 0
        while (i < updateFunctions.length) {
          updateFunctions(i)(currentBuffer, row)
          i += 1
        }
      }
    } else {
      // Grouping only. 如果没有聚合表达式，只需要分组即可
      (currentBuffer: InternalRow, row: InternalRow) => {}
    }
  }

  protected val processRow: (InternalRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes)

  /**
   * 创建Grouping Projection，
   * groupingExpressions表示分组表达式列表。
   * inputAttributes表示子节点输入数据的属性列表。
   *
   * 该Projection是UnsafeProjection，最终由Codegen生成具体的代码。
   * 用于对InternalRow中具体的列进行Project操作，生成UnsafeRow。
   */
  protected val groupingProjection: UnsafeProjection =
    UnsafeProjection.create(groupingExpressions, inputAttributes)

  // 分组列属性
  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)

  // Initializing the function used to generate the output row.
  /**
   * 聚合结果输出函数生成。
   * 对于Partial或PartialMerge模式的聚合函数，因为只是中间结果，所以需要保存grouping语句与buffer中所有的属性；
   * 对于Final和Complete聚合模式，直接对应resultExpressions表达式。
   * 特别注意，如果不包含任何聚合函数且只有分组操作，则直接创建projection。
   * @return (UnsafeRow, InternalRow) => UnsafeRow
   */
  protected def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val joinedRow = new JoinedRow

    // 找出所有的聚合模式
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)

    if (modes.contains(Final) || modes.contains(Complete)) { // Final/Complete阶段

      // 执行表达式
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression // 最终结果生成表达式
        case agg: AggregateFunction => NoOp
      }

      // 聚合结果行
      val aggregateResult = new SpecificInternalRow(aggregateAttributes.map(_.dataType))

      // 根据结果生成表达式生成聚合结果Projection，并将结果存入到aggregateResult
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)
      expressionAggEvalProjection.target(aggregateResult)

      // 最终结果表达式
      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        // Generate results for all expression-based aggregate functions.
        // 对所有基于表达式的聚合函数生成结果（DeclarativeAggregate）
        expressionAggEvalProjection(currentBuffer)

        // Generate results for all imperative aggregate functions.
        // 对所有ImperativeAggregate聚合函数生成结果
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer))
          i += 1
        }
        // 生成JoinedRow的Project最终结果
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) { // Partial/PartialMerge阶段
      // 因为只是中间结果，所以需要保存grouping语句（分组列）与buffer中所有的属性（聚合列）
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ bufferAttributes,
        groupingAttributes ++ bufferAttributes)

      // TypedImperativeAggregate stores generic object in aggregation buffer, and requires
      // calling serialization before shuffling. See [[TypedImperativeAggregate]] for more info.
      val typedImperativeAggregates: Array[TypedImperativeAggregate[_]] = {
        aggregateFunctions.collect {
          case (ag: TypedImperativeAggregate[_]) => ag
        }
      }

      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        // Serializes the generic object stored in aggregation buffer
        var i = 0
        while (i < typedImperativeAggregates.length) {
          typedImperativeAggregates(i).serializeAggregateBufferInPlace(currentBuffer)
          i += 1
        }
        // 生成JoinedRow的Project中间结果
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else { // 无聚合函数
      // Grouping-only: we only output values based on grouping expressions.
      // 如果不包含任何聚合函数且只有分组操作，则直接创建projection。
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  protected val generateOutput: (UnsafeRow, InternalRow) => UnsafeRow =
    generateResultProjection()

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: InternalRow): Unit = {
    // 使用空的Row在buffer中初始化每个列的列名
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allImperativeAggregateFunctions.length) {
      // 使用具体的聚合函数节点来初始化对应列的初始值
      allImperativeAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }
}
