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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 */
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  def createOrderedJoin(input: Seq[(LogicalPlan, InnerLike)], conditions: Seq[Expression])
    : LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(
        e => !SubqueryExpression.hasCorrelatedSubquery(e))
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(
        e => e.references.subsetOf(joinedRefs) && !SubqueryExpression.hasCorrelatedSubquery(e))
      val joined = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      createOrderedJoin(input, conditions)
  }
}

/**
 * Elimination of outer joins, if the predicates can restrict the result sets so that
 * all null-supplying rows are eliminated
 *
 * - full outer -> inner if both sides have such predicates
 * - left outer -> inner if the right side has such predicates
 * - right outer -> inner if the left side has such predicates
 * - full outer -> left outer if only the left side has such predicates
 * - full outer -> right outer if only the right side has such predicates
 *
 * This rule should be executed before pushing down the Filter
 *
 * 消除Outer join，如果谓词可以约束最终的结果集中null值行可以被消除。
 * 这条规则即是常见查询优化中的Reject Null空值拒绝，即在Outer join中如果空值填充表使用了过滤Null值的谓词，
 * 那么Outer join是可以被转换为Inner join或更小粒度的Outer join的；具体有以下几种情况：
 *  1. 如果Full outer join两侧都有Reject Null谓词，那么可以转换为Inner join。
 *  2. 如果Left outer join中右侧的表存在Reject Null谓词，那么可以转换为Inner join。
 *  3. 如果Right outer join中左侧表存在Reject Null谓词，那么可以转换为Inner join。
 *  4. 如果Full outer join中只有左侧表存在Reject Null谓词，那么可以转换为Left outer join。
 *  5. 如果Full outer join中只有右侧表存在Reject Null谓词，那么可以转换为Right outer join。
 *
 */
object EliminateOuterJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether the expression returns null or false when all inputs are nulls.
   */
  private def canFilterOutNull(e: Expression): Boolean = {
    // 表达式为确定，或者表达式还存在子查询相关表达式，直接返回
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    // 获取表达式中的列引用
    val attributes = e.references.toSeq
    // 以列数量构建一个具有等同Null列的行
    val emptyRow = new GenericInternalRow(attributes.length)
    // 检查表达式中是否存在不可执行的表达式，如果存在就返回false
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.find(_.isInstanceOf[Unevaluable]).isDefined) return false
    // 传入值全为Null的空行进行测试
    val v = boundE.eval(emptyRow)
    // 得到的结果为null，或得到的结果是false，说明存在NOT NULL过滤
    v == null || v == false
  }

  private def buildNewJoinType(filter: Filter, join: Join): JoinType = {
    // 对Filter节点的过滤条件及约束条件进行整合
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    // 根据Filter过滤条件最终的输出列，判断这些列是否存在于Join左表的输出列中，如果存在说明这个Filter条件是与Join左表相关的
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    // 根据Filter过滤条件最终的输出列，判断这些列是否存在于Join右表的输出列中，如果存在说明这个Filter条件是与Join右表相关的
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    // 检查是否存在Reject Null空值拒绝的谓词
    val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    // 对Join类型进行匹配，根据Reject Null谓词，得到新的Join类型
    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if rightHasNonNullPredicate => RightOuter
      case o => o
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform { // 遍历所有节点
    // 匹配Filter节点，且其子节点是Outer Join中的一种
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _)) =>
      // 根据Filter和Join节点计算新的优化后的Join类型
      val newJoinType = buildNewJoinType(f, j)
      // 如果优化后的Join类型与旧的相同，直接返回原始节点，否则以新的Join类型构造新的Join节点，封装为Filter节点返回
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))
  }
}
