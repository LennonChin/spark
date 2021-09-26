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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.Utils

/**
 * Sort-based aggregate operator.
 *
 * 基于Sort的聚合操作。
 *
 * @param requiredChildDistributionExpressions 要求子节点的数据分布
 * @param groupingExpressions 分组表达式列表
 * @param aggregateExpressions 聚合表达式列表
 * @param aggregateAttributes 聚合属性列表
 * @param initialInputBufferOffset 初始化的InputBufferOffset
 * @param resultExpressions 结果表达式列表
 * @param child 子节点
 */
case class SortAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  // 节点产生的属性
  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  // 构造度量字典
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil

      // 存在要求子节点的数据分布时，使用ClusteredDistribution
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  // 对输入数据的有序性做了约束
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    // 其中的每个表达式都必须满足升序排列
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  // 输出分区就是子节点的输出分区
  override def outputPartitioning: Partitioning = child.outputPartitioning

  // 输出数据排序是有序的
  override def outputOrdering: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    // 创建long型的度量值numOutputRows
    val numOutputRows = longMetric("numOutputRows")

    // 执行子节点，以获取分区数据迭代器
    child.execute().mapPartitionsInternal { iter => // 每一个迭代器都是一个分区
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        // 如果子节点没有输出数据，直接返回空迭代器
        Iterator[UnsafeRow]()
      } else {
        // 子节点输出了数据，返回基于Sort的聚合迭代器
        val outputIter = new SortBasedAggregationIterator(
          groupingExpressions,
          child.output,
          iter,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
          numOutputRows)
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.

          // 如果没有分组表达式，则不用聚合直接输出即可，否则返回迭代器
          numOutputRows += 1
          Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }

  override def simpleString: String = toString(verbose = false)

  override def verboseString: String = toString(verbose = true)

  private def toString(verbose: Boolean): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = Utils.truncatedString(groupingExpressions, "[", ", ", "]")
    val functionString = Utils.truncatedString(allAggregateExpressions, "[", ", ", "]")
    val outputString = Utils.truncatedString(output, "[", ", ", "]")
    if (verbose) {
      s"SortAggregate(key=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"SortAggregate(key=$keyString, functions=$functionString)"
    }
  }
}
