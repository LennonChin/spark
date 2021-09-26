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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.{StateStoreRestoreExec, StateStoreSaveExec}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object AggUtils {

  // 不支持Partial聚合，无Distinct
  def planAggregateWithoutPartial(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map(_.resultAttribute)
    SortAggregateExec(
      requiredChildDistributionExpressions = Some(groupingExpressions), // 子节点的数据输出分布需要按照分组表达式来进行
      groupingExpressions = groupingExpressions, // 分组表达式
      aggregateExpressions = completeAggregateExpressions, // 聚合表达式列表（Complete模式）
      aggregateAttributes = completeAggregateAttributes, // 聚合属性列表
      initialInputBufferOffset = 0, // 初始化的InputBufferOffset
      resultExpressions = resultExpressions, // 结果表达式列表，其实也是聚合表达式列表
      child = child // 子节点
    ) :: Nil
  }

  // 创建AggregateExec节点
  private def createAggregate(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    // 判断是否支持HashAggregate
    val useHash = HashAggregateExec.supportsAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      SortAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    }
  }

  // 支持Partial聚合，无Distinct
  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use HashAggregate.

    // 1. Create an Aggregate Operator for partial aggregations.
    // 1. 为Partial聚合创建Aggregation操作

    // 分组表达式的属性列表
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 根据聚合表达式，创建Partial模式的聚合表达式副本
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))

    // Partial模式聚合表达式的聚合缓冲区的数据列信息，对应缓冲区数据列名。
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    // 结果表达式列表，是分组表达式列表和聚合表达式列表的并集
    val partialResultExpressions =
      groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    // 创建Partial Aggregate节点
    val partialAggregate = createAggregate(
        requiredChildDistributionExpressions = None, // Partial模式对子节点的输出数据分布没有要求
        groupingExpressions = groupingExpressions, // 分组表达式列表
        aggregateExpressions = partialAggregateExpressions, // 聚合表达式列表
        aggregateAttributes = partialAggregateAttributes, // 聚合表达式属性
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions, // 结果表达式列表
        child = child)

    // 2. Create an Aggregate Operator for final aggregations.
    // 2. 为Final聚合创建Aggregation操作
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

    val finalAggregate = createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = partialAggregate) // Partial阶段的节点会作为Final阶段节点的子节点，即先执行Partial阶段，再执行Final阶段。

    finalAggregate :: Nil
  }

  // 支持Partial聚合，有一个Distance
  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[NamedExpression], // 分组表达式列表
      functionsWithDistinct: Seq[AggregateExpression], // 包含Distinct操作的聚合表达式列表
      functionsWithoutDistinct: Seq[AggregateExpression], // 不包含Distinct操作的聚合表达式列表
      resultExpressions: Seq[NamedExpression], // 结果表达式列表
      child: SparkPlan): Seq[SparkPlan] = {

    // functionsWithDistinct is guaranteed to be non-empty. Even though it may contain more than one
    // DISTINCT aggregate function, all of those functions will have the same column expressions.
    // For example, it would be valid for functionsWithDistinct to be
    // [COUNT(DISTINCT foo), MAX(DISTINCT foo)], but [COUNT(DISTINCT bar), COUNT(DISTINCT foo)] is
    // disallowed because those two distinct aggregates have different column expressions.
    /**
     * 包含Distinct操作的函数一定是存在的。
     * 即使它可能包含多个 DISTINCT 聚合函数，所有这些函数都将具有相同的列表达式。
     * 举个例子，[COUNT(DISTINCT foo), MAX(DISTINCT foo)]是允许的。
     * 但是[COUNT(DISTINCT bar), COUNT(DISTINCT foo)] 是不允许的，因为两个Distinct聚合使用了不同的列。
     */

    // 取第一个包含去重的聚合表达式，并对其进行命名（可能会在上面加一个Alias节点）
    val distinctExpressions = functionsWithDistinct.head.aggregateFunction.children
    val namedDistinctExpressions = distinctExpressions.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    // 得到去重的列
    val distinctAttributes = namedDistinctExpressions.map(_.toAttribute)
    // 得到分组的列
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 1. Create an Aggregate Operator for partial aggregations.
    // 1. 为Partial聚合阶段创建Aggregate节点
    val partialAggregate: SparkPlan = {
      // 未包含去重的聚合表达式作为Partial阶段的聚合
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      // We will group by the original grouping expression, plus an additional expression for the
      // DISTINCT column. For example, for AVG(DISTINCT value) GROUP BY key, the grouping
      // expressions will be [key, value].
      /**
       * 我们将按原始分组表达式以及 DISTINCT 列的附加表达式进行分组。
       * 例如，对于 AVG(DISTINCT value) GROUP BY 键，分组表达式将为 [key, value]。
       *
       * Partial这一步的操作，会将Distinct聚合表达式也作为分组表达式，参与分组
       */
      createAggregate(
        groupingExpressions = groupingExpressions ++ namedDistinctExpressions, // 此处添加了Distinct聚合表达式作为分组表达式
        aggregateExpressions = aggregateExpressions, // 不包含Distinct的聚合表达式作为Partial阶段的聚合表达式
        aggregateAttributes = aggregateAttributes, // 聚合表达式属性
        resultExpressions = groupingAttributes ++ distinctAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes), // 输出结果为分组表达式（包含Distinct的聚合表达式）以及聚合表达式的并集
        child = child)
    }

    // 2. Create an Aggregate Operator for partial merge aggregations.
    // 2. 为PartialMerge阶段创建Aggregate节点（针对非Distinct聚合，分组表达式中包含Distinct聚合的列）
    val partialMergeAggregate: SparkPlan = {
      // 不包含Distinct的聚合表达式列表，在PartialMerge阶段进行聚合
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregate(
        requiredChildDistributionExpressions =
          Some(groupingAttributes ++ distinctAttributes), // 要求子节点的输出数据分布按照分组表达式和Distinct聚合表达式进行分布
        groupingExpressions = groupingAttributes ++ distinctAttributes, // 此处添加了Distinct聚合表达式作为分组表达式
        aggregateExpressions = aggregateExpressions, // 聚合表达式列表
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length, // 此时输入缓冲区的起始offset是在分组表达式和聚合表达式之后
        resultExpressions = groupingAttributes ++ distinctAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes), // 结果中有分组表达式列表、包含Distinct的聚合表达式列表和其他聚合表达式列表。
        child = partialAggregate) // 上一阶段的Partial Aggregate作为子节点
    }

    // 3. Create an Aggregate operator for partial aggregation (for distinct)
    // 3. 为Partial阶段创建Aggregate节点（针对Distinct聚合，分组表达式中不包含Distinct聚合的列）

    // Distinct聚合表达式与属性构造为Map
    val distinctColumnAttributeLookup = distinctExpressions.zip(distinctAttributes).toMap

    // 重写Distinct函数
    val rewrittenDistinctFunctions = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      /**
       * 已经执行了带有 DISTINCT 关键字的 AggregateFunction 的子节点。
       * 在这里，我们需要将原始子节点替换为 AttributeReferences。
       */
      case agg @ AggregateExpression(aggregateFunction, mode, true, _) =>
        aggregateFunction.transformDown(distinctColumnAttributeLookup)
          .asInstanceOf[AggregateFunction]
    }

    // 构造PartialMerge阶段的Distinct聚合节点
    val partialDistinctAggregate: SparkPlan = {
      // 非Distinct使用PartialMerge聚合
      val mergeAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val mergeAggregateAttributes = mergeAggregateExpressions.map(_.resultAttribute)

      // Distinct使用Partial聚合
      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users look at the query plan,
          // they still can see distinct aggregations.
          val expr = AggregateExpression(func, Partial, isDistinct = true)
          // Use original AggregationFunction to lookup attributes, which is used to build
          // aggregateFunctionToAttribute
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
      }.unzip

      val partialAggregateResult = groupingAttributes ++
          mergeAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes) ++
          distinctAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      createAggregate(
        groupingExpressions = groupingAttributes, // 原始的聚合表达式列表
        aggregateExpressions = mergeAggregateExpressions ++ distinctAggregateExpressions, // 非Distinct的PartialMerge聚合表达式和Distinct的Partial聚合表达式
        aggregateAttributes = mergeAggregateAttributes ++ distinctAggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length,
        resultExpressions = partialAggregateResult,
        child = partialMergeAggregate) // 子节点为上一步的PartialMerge AggregateExec
    }

    // 4. Create an Aggregate Operator for the final aggregation.
    // 4. 为Final聚合阶段创建Aggregate节点。
    val finalAndCompleteAggregate: SparkPlan = {
      // 非Distinct的聚合使用Final模式
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      // Distinct的聚合也是用Final模式
      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users look at the query plan,
          // they still can see distinct aggregations.
          val expr = AggregateExpression(func, Final, isDistinct = true)
          // Use original AggregationFunction to lookup attributes, which is used to build
          // aggregateFunctionToAttribute
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
      }.unzip

      createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes), // 要求按照原始分组列进行数据分布
        groupingExpressions = groupingAttributes, // 按照原始分组列进行分组
        aggregateExpressions = finalAggregateExpressions ++ distinctAggregateExpressions, // 包含非Distinct和Distinct的聚合列
        aggregateAttributes = finalAggregateAttributes ++ distinctAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = partialDistinctAggregate) // 子节点是上一步的Partial Distinct AggregateExec
    }

    finalAndCompleteAggregate :: Nil
  }

  /**
   * Plans a streaming aggregation using the following progression:
   *  - Partial Aggregation
   *  - Shuffle
   *  - Partial Merge (now there is at most 1 tuple per group)
   *  - StateStoreRestore (now there is 1 tuple from this batch + optionally one from the previous)
   *  - PartialMerge (now there is at most 1 tuple per group)
   *  - StateStoreSave (saves the tuple for the next batch)
   *  - Complete (output the current result of the aggregation)
   */
  def planStreamingAggregation(
      groupingExpressions: Seq[NamedExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      // We will group by the original grouping expression, plus an additional expression for the
      // DISTINCT column. For example, for AVG(DISTINCT value) GROUP BY key, the grouping
      // expressions will be [key, value].
      createAggregate(
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = child)
    }

    val partialMerged1: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregate(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = partialAggregate)
    }

    val restored = StateStoreRestoreExec(groupingAttributes, None, partialMerged1)

    val partialMerged2: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregate(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = restored)
    }
    // Note: stateId and returnAllStates are filled in later with preparation rules
    // in IncrementalExecution.
    val saved =
      StateStoreSaveExec(
        groupingAttributes,
        stateId = None,
        outputMode = None,
        eventTimeWatermark = None,
        partialMerged2)

    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = saved)
    }

    finalAndCompleteAggregate :: Nil
  }
}
