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

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions
import org.apache.spark.sql.execution.python.ExtractPythonUDFFromAggregate
import org.apache.spark.sql.internal.SQLConf

class SparkOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends Optimizer(catalog, conf) {

  override def batches: Seq[Batch] = super.batches :+
    /**
     * 用来优化执行过程中只需查找分区级别元数据的语句。
     * 需要注意的是，OptimizeMetadataOnlyQuery优化规则适用于扫描的所有列都是分区列且包含聚合算子的情形，而且聚合算子需要满足以下情况之一：
     *  - 聚合表达式是分区列；
     *  - 分区列的聚合函数有DISTINCT算子；
     *  - 分区列的聚合函数中是否有DISTINCT算子不影响结果。
     */
    Batch("Optimize Metadata Only Query", Once, OptimizeMetadataOnlyQuery(catalog, conf)) :+
    /**
     * 提取出聚合操作中的Python UDF函数。
     * 该规则主要针对的是采用PySpark提交查询的情形，将参与聚合的Python自定义函数提取出来，在聚合操作完成之后再执行。
     */
    Batch("Extract Python UDF from Aggregate", Once, ExtractPythonUDFFromAggregate) :+
    /**
     * 用来对数据文件中的分区进行剪裁操作。
     * 当数据文件中定义了分区信息且逻辑算子树中的LogicalRelation节点上方存在过滤算子时，
     * 该优化规则会尽可能地将过滤算子下推到存储层，这样可以避免读入无关的数据分区。
     *
     * 会涉及多对一组合转换PhysicalOperation模式的处理（匹配逻辑算子树中的Project和Filter等节点，返回投影列、过滤条件集合和子节点。）
     */
    Batch("Prune File Source Table Partitions", Once, PruneFileSourcePartitions) :+
    /**
     * 用于支持用户自定义的优化规则，其中ExperimentalMethods的extraOptimizations队列默认为空。
     * Spark SQL在逻辑算子树的转换阶段是高度可扩展的，用户只需要继承Rule[LogicalPlan]虚类，实现相应的转换逻辑就可以注册到优化规则队列中应用执行。
     */
    Batch("User Provided Optimizers", fixedPoint, experimentalMethods.extraOptimizations: _*)
}
