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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 *
 * Strategy是SparkStrategy的别名，see [[Strategy]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

/**
 * doExecute()方法没有实现，表示不支持执行，所起到的作用仅仅是占位，等待后续步骤处理。
 *
 * @param plan
 */
case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

/**
 * 内部提供一批SparkPlanner会用到的各种策略（Strategy）实现
 */
abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
          execution.TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case logical.Limit(
            IntegerLiteral(limit),
            logical.Project(projectList, logical.Sort(order, true, child))) =>
          execution.TakeOrderedAndProjectExec(
            limit, order, projectList, planLater(child)) :: Nil
        case logical.Limit(IntegerLiteral(limit), child) =>
          execution.CollectLimitExec(limit, planLater(child)) :: Nil
        case other => planLater(other) :: Nil
      }
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
        execution.TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case logical.Limit(
          IntegerLiteral(limit), logical.Project(projectList, logical.Sort(order, true, child))) =>
        execution.TakeOrderedAndProjectExec(
          limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on joining keys and size of logical plan.
   *
   * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
   * predicates can be evaluated by matching join keys. If found,  Join implementations are chosen
   * with the following precedence:
   *
   * - Broadcast: if one side of the join has an estimated physical size that is smaller than the
   *     user-configurable [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold
   *     or if that side has an explicit broadcast hint (e.g. the user applied the
   *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame), then that side
   *     of the join will be broadcasted and the other side will be streamed, with no shuffling
   *     performed. If both sides of the join are eligible to be broadcasted then the
   * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
   *     table.
   * - Sort merge: if the matching join keys are sortable.
   *
   * If there is no joining keys, Join implementations are chosen with the following precedence:
   * - BroadcastNestedLoopJoin: if one side of the join could be broadcasted
   * - CartesianProduct: for Inner join
   * - BroadcastNestedLoopJoin
   *
   * 根据Join的连接key、和左右表大小，选择可能的物理执行计划。
   *
   * 首先，使用 [[ExtractEquiJoinKeys]] 模式查找至少可以通过匹配连接键评估某些谓词的连接。
   * 如果找到，则按照以下优先级选择 Join 实现：
   *
   * - Broadcast：如果连接的一侧的估计物理大小小于用户可配置的 [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] 阈值，
   *              或者如果该侧具有显式广播提示（例如，用户应用了 [[org.apache.spark.sql.functions.broadcast()]] 函数到一个数据帧），
   *              那么这一侧的表将被广播，另一侧将被作为探测表，没有shuffle操作。 如果加入的双方都有资格被广播，那么
   * - Shuffle hash join：如果单个分区的平均大小足够小，可以用于构建Hash Map。
   * - Sort merge：如果连接键是排序的。
   *
   * 如果不存在连接键，Join的实现将依照下面的顺序选择：
   * - BroadcastNestedLoopJoin：如果有一侧的表可以被广播。
   * - CartesianProduct：Inner Join适用
   * - BroadcastNestedLoopJoin
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * Matches a plan whose output should be small enough to be used in broadcast join.
     * 判断某个表对应的逻辑计划能否广播。
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.statistics.isBroadcastable ||
        (plan.statistics.sizeInBytes >= 0 &&
          plan.statistics.sizeInBytes <= conf.autoBroadcastJoinThreshold) // 表的大小在 0 ~ spark.sql.autoBroadcastJoinThreshold之间
    }

    /**
     * Matches a plan whose single partition should be small enough to build a hash table.
     *
     * Note: this assume that the number of partition is fixed, requires additional work if it's
     * dynamic.
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      // 逻辑计划的数据量 < 广播阈值 * Shuffle分区数
      // Shuffle分区数由spark.sql.shuffle.partitions配置，默认200
      plan.statistics.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * Returns whether plan a is much smaller (3X) than plan b.
     *
     * The cost to build hash map is higher than sorting, we should only build hash map on a table
     * that is much smaller than other one. Since we does not have the statistic for number of rows,
     * use the size of bytes here as estimation.
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.statistics.sizeInBytes * 3 <= b.statistics.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // --- BroadcastHashJoin --------------------------------------------------------------------

      /**
       * - 等值Join
       * - 右表可构建
       * - 右表可广播
       *
       * => BroadcastHashJoinExec(..., BuildRight, ...)
       */
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBroadcast(right) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      /**
       * - 等值Join
       * - 左表可构建
       * - 左表可广播
       *
       * => BroadcastHashJoinExec(..., BuildLeft, ...)
       */
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBroadcast(left) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

      // --- ShuffledHashJoin ---------------------------------------------------------------------

      /**
       * - 等值Join
       * - spark.sql.join.preferSortMergeJoin参数为false
       * - 可构建右表
       * - 右表可构建为本地HashMap
       * - 右表比左表小很多
       *
       * 或者：
       * - 左表的连接键不要求有序性
       *
       * => ShuffledHashJoinExec(..., BuildRight, ...)
       */
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
           && muchSmaller(right, left) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      /**
       * - 等值Join
       * - spark.sql.join.preferSortMergeJoin参数为false
       * - 可构建左表
       * - 左表可构建为本地HashMap
       * - 左表比右表小很多
       *
       * 或者：
       * - 左表的连接键不要求有序性
       *
       * => ShuffledHashJoinExec(..., BuildLeft, ...)
       */
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
           && muchSmaller(left, right) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

      // --- SortMergeJoin ------------------------------------------------------------

      /**
       * - 等值Join
       * - 左表的连接键要求有序性
       * => SortMergeJoinExec(...)
       */
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>
        joins.SortMergeJoinExec(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      // --- Without joining keys ------------------------------------------------------------

      // Pick BroadcastNestedLoopJoin if one side could be broadcasted
      // 如果一侧可以广播，则使用BroadcastNestedLoopJoin。

      /**
       * - 可以是等值Join也可以是非等值Join
       * - 右表可构建
       * - 右表可广播
       *
       * => BroadcastNestedLoopJoinExec(..., BuildRight, ...)
       */
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildRight(joinType) && canBroadcast(right) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), BuildRight, joinType, condition) :: Nil

      /**
       * - 可以是等值Join也可以是非等值Join
       * - 左表可构建
       * - 左表可广播
       *
       * => BroadcastNestedLoopJoinExec(..., BuildLeft, ...)
       */
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildLeft(joinType) && canBroadcast(left) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), BuildLeft, joinType, condition) :: Nil

      // Pick CartesianProduct for InnerJoin
      /**
       * - InnerLike Join：Inner Join或Cross Join
       * => CartesianProductExec
       */
      case logical.Join(left, right, _: InnerLike, condition) =>
        joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

      /**
       * - 可以是等值Join也可以是非等值Join
       * - 右表比左表小很多
       * => BroadcastNestedLoopJoinExec(..., BuildRight, ...)
       *
       * - 左表比右表小很多
       * => BroadcastNestedLoopJoinExec(..., BuildLeft, ...)
       */
      case logical.Join(left, right, joinType, condition) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            BuildRight
          } else {
            BuildLeft
          }
        // This join could be very slow or OOM
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }

  /**
   * Used to plan aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        AggUtils.planStreamingAggregation(
          namedGroupingExpressions,
          aggregateExpressions,
          rewrittenResultExpressions,
          planLater(child))

      case _ => Nil
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      /**
       * 使用PhysicalAggregation进行解包，得到的四个参数分别是
       * (加上别名后的分组表达式, 去重后的聚合表达式, 重写后的聚合表达式, 子节点)
       */
      case PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child) =>

        // 将聚合表达式分为需要去重的（functionsWithDistinct）和不需要去重的（functionsWithoutDistinct）
        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)

        // 需要去重的函数内，参数不可超过1个。
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        val aggregateOperator =
          if (aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPartial)) {
            // 聚合表达式中存在不支持Partial的聚合函数
            // 不支持Partial聚合，但又存在Distinct函数，这种情况会打印错误日志。
            if (functionsWithDistinct.nonEmpty) {
              sys.error("Distinct columns cannot exist in Aggregate operator containing " +
                "aggregate functions which don't support partial aggregation.")
            } else {
              // 不支持Partial聚合，无Distinct
              AggUtils.planAggregateWithoutPartial(
                groupingExpressions,
                aggregateExpressions,
                resultExpressions,
                planLater(child))
            }
          } else if (functionsWithDistinct.isEmpty) {
            // 支持Partial聚合，无Distinct
            AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            // 支持Partial聚合，有一个Distance
            AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  /**
   * InMemoryScans主要针对的是InMemoryRelation这个LogicalPlan节点，
   * 匹配PhysicalOperation这个模式，最终生成InMemoryTableScanExec，
   * 并调用SparkPlanner中的pruneFilterProject方法对其进行过滤和列剪裁。
   */
  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        // 进行过滤和列剪裁
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  /**
   * 针对各种基本操作类型的LogicalPlan节点，例如排序、过滤等，
   * 这种情况下，一般一对一地进行映射即可（例如，Sort逻辑节点映射为SortExec物理计划）。
   */
  object BasicOperators extends Strategy {
    def numPartitions: Int = self.numPartitions

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(sink.schema)
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by semi-join in the optimizer")
      case logical.Except(left, right) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left), planLater(right)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchange(RoundRobinPartitioning(numPartitions), planLater(child)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.SortExec(sortExprs, global = false, child = planLater(child)) :: Nil
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Window(windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data) =>
        LocalTableScanExec(output, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(unionChildren.map(planLater)) :: Nil
      case g @ logical.Generate(generator, join, outer, _, _, child) =>
        execution.GenerateExec(
          generator, join = join, outer = outer, g.qualifiedGeneratorOutput,
          planLater(child)) :: Nil
      case logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case logical.RepartitionByExpression(expressions, child, nPartitions) =>
        exchange.ShuffleExchange(HashPartitioning(
          expressions, nPartitions.getOrElse(numPartitions)), planLater(child)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case BroadcastHint(child) => planLater(child) :: Nil
      case _ => Nil
    }
  }

  /**
   * DDLStrategy在Spark SQL中仅针对CreateTable与CreateTempViewUsing这两种类型的节点，
   * 这两种情况都直接生成ExecutedCommandExec类型的物理计划。
   */
  object DDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTable(tableDesc, mode, None)
        if tableDesc.provider.get == DDLUtils.HIVE_PROVIDER =>
        val cmd = CreateTableCommand(tableDesc, ifNotExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

      case CreateTable(tableDesc, mode, None) =>
        val cmd =
          CreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

      // CREATE TABLE ... AS SELECT ... for hive serde table is handled in hive module, by rule
      // `CreateTables`

      case CreateTable(tableDesc, mode, Some(query))
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER =>
        val cmd =
          CreateDataSourceTableAsSelectCommand(
            tableDesc,
            mode,
            query)
        ExecutedCommandExec(cmd) :: Nil

      case c: CreateTempViewUsing => ExecutedCommandExec(c) :: Nil

      case _ => Nil
    }
  }
}
