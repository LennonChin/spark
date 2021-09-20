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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.types.IntegerType

/**
 * Replaces ordinal in 'order by' or 'group by' with UnresolvedOrdinal expression.
 * spark.sql.orderByOrdinal：允许order by使用下标表示字段
 * spark.sql.groupByOrdinal：允许group by使用下标表示字段
 */
class SubstituteUnresolvedOrdinals(conf: CatalystConf) extends Rule[LogicalPlan] {
  private def isIntLiteral(e: Expression) = e match {
    case Literal(_, IntegerType) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    /**
     * 检查ORDER BY / SORT BY中使用下标的情况
     * 1. Sort节点。
     * 2. spark.sql.orderByOrdinal参数开启了。
     * 3. Sort的排序表达式中存在IntLiteral字面量。
     *
     * 例如SQL：
     *
     * SELECT name, age, country FROM person ORDER BY 2
     *
     * 计划转换如下：
     * == Parsed Logical Plan ==
     * 'Sort [2 ASC NULLS FIRST], true
     * +- 'Project ['name, 'age, 'country]
     *    +- 'UnresolvedRelation `person`
     *
     * == Analyzed Logical Plan ==
     * name: string, age: bigint, country: string
     * Sort [age#0L ASC NULLS FIRST], true
     * +- Project [name#4, age#0L, country#2]
     *    +- SubqueryAlias person, `person`
     *    +- Relation[age#0L,company_id#1L,country#2,id#3L,name#4] json
     *
     * 注意，Unresolved LogicalPlan中的Sort [2 ASC NULLS FIRST], true最终转换为了Sort [age#0L ASC NULLS FIRST], true，
     * 这个过程是先使用SubstituteUnresolvedOrdinals规则将Literal(2)转换为UnresolvedOrdinal(2)，
     * 然后使用ResolveOrdinalInOrderByAndGroupBy将UnresolvedOrdinal(2)转换为真实的列表引用age#0L。
     */
    case s: Sort if conf.orderByOrdinal && s.order.exists(o => isIntLiteral(o.child)) =>
      val newOrders = s.order.map { // 遍历所有的排序表达式，类型为SortOrder
        case order @ SortOrder(ordinal @ Literal(index: Int, IntegerType), _, _) =>
          // 将索引转换为UnresolvedOrdinal节点
          val newOrdinal: UnresolvedOrdinal = withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
          // 构造为新的SortOrder表达式
          withOrigin(order.origin)(order.copy(child = newOrdinal))
        case other => other
      }
      // 根据新的SortOrders构造新的Sort节点
      withOrigin(s.origin)(s.copy(order = newOrders))

    /**
     * 检查GROUP BY中使用下标的情况
     * 1. Aggregate节点。
     * 2. spark.sql.groupByOrdinal参数开启了。
     * 3. Aggregate的分组表达式中存在IntLiteral字面量。
     *
     * 例如SQL：
     *
     * SELECT country, avg(age) FROM person GROUP BY 1
     *
     * 计划转换如下：
     * == Parsed Logical Plan ==
     * 'Aggregate [1], ['country, unresolvedalias('avg('age), None)]
     * +- 'UnresolvedRelation `person`
     *
     * == Analyzed Logical Plan ==
     * country: string, avg(age): double
     * Aggregate [country#2], [country#2, avg(age#0L) AS avg(age)#216]
     * +- SubqueryAlias person, `person`
     *    +- Relation[age#0L,company_id#1L,country#2,id#3L,name#4] json
     *
     * 注意，Unresolved LogicalPlan中的Aggregate [1], ['country, unresolvedalias('avg('age), None)]最终转换为了Aggregate [country#2], [country#2, avg(age#0L) AS avg(age)#216]，
     * 这个过程是先使用SubstituteUnresolvedOrdinals规则将Literal(1)转换为UnresolvedOrdinal(1)，
     * 然后使用ResolveOrdinalInOrderByAndGroupBy将UnresolvedOrdinal(2)转换为真实的列表引用country#2。
     */
    case a: Aggregate if conf.groupByOrdinal && a.groupingExpressions.exists(isIntLiteral) =>
      val newGroups = a.groupingExpressions.map { // 遍历所有的分组表达式，类型为Expression
        // 将Literal表达式转换为UnresolvedOrdinal节点
        case ordinal @ Literal(index: Int, IntegerType) =>
          withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
        case other => other
      }
      // 根据新的Grouping expressions构造新的Aggregate节点
      withOrigin(a.origin)(a.copy(groupingExpressions = newGroups))
  }
}
