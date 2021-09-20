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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.  Distribution can be used to refer to two distinct physical
 * properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
 */
sealed trait Distribution

/**
 * Represents a distribution where no promises are made about co-location of data.
 *
 * 未指定分布，无需确定数据元组之间的位置关系。
 */
case object UnspecifiedDistribution extends Distribution

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 *
 * 只有一个分区，所有的数据元组存放在一起（Co-located）。
 */
case object AllTuples extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located. Based on the context, this
 * can mean such tuples are either co-located in the same partition or they will be contiguous
 * within a single partition.
 *
 * 数据根据clustering参数指定的多个表达式进行聚集，保证相同值的元组将位于同一位置。
 * 根据上下文，这可能意味着这些元组要么位于同一分区中，要么在单个分区中是连续的。
 *
 * @param clustering 起到了哈希函数的效果，数据经过clustering计算后，相同value的数据元组会被存放在一起（Co-located）。
 *                   如果有多个分区的情况，则相同数据会被存放在同一个分区中；如果只能是单个分区，则相同的数据会在分区内连续存放。
 */
case class ClusteredDistribution(clustering: Seq[Expression]) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]].  This is a strictly stronger guarantee than
 * [[ClusteredDistribution]] as an ordering will ensure that tuples that share the
 * same value for the ordering expressions are contiguous and will never be split across
 * partitions.
 *
 * 表示根据ordering参数指定的多个排序表达式对元组进行排序的数据。
 * 这是比ClusteredDistribution更严格的保证，因为排序将确保为排序表达式共享相同值的元组是连续的，并且永远不会跨分区拆分。
 *
 * 该分布意味着数据元组会根据ordering计算后的结果排序。
 *
 * @param ordering
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  // TODO: This is not really valid...
  def clustering: Set[Expression] = ordering.map(_.child).toSet
}

/**
 * Represents data where tuples are broadcasted to every node. It is quite common that the
 * entire set of tuples is transformed into different data structure.
 *
 * 广播分布，数据会被广播到所有节点上。将整个元组集转换为不同的数据结构是很常见的。
 *
 * @param mode 广播模式
 */
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution

/**
 * Describes how an operator's output is split across partitions. The `compatibleWith`,
 * `guarantees`, and `satisfies` methods describe relationships between child partitionings,
 * target partitionings, and [[Distribution]]s. These relations are described more precisely in
 * their individual method docs, but at a high level:
 *
 *  - `satisfies` is a relationship between partitionings and distributions.目标Partitioning和Distribution之间
 *  - `compatibleWith` is relationships between an operator's child output partitionings. 两个子Partitioning之间
 *  - `guarantees` is a relationship between a child's existing output partitioning and a target
 *     output partitioning. 目标Partitioning和子Partitioning之间
 *
 *  Diagrammatically:
 *
 *            +--------------+
 *            | Distribution |
 *            +--------------+
 *                    ^
 *                    |
 *               satisfies
 *                    |
 *            +--------------+                  +--------------+
 *            |    Child     |                  |    Target    |
 *       +----| Partitioning |----guarantees--->| Partitioning |
 *       |    +--------------+                  +--------------+
 *       |            ^
 *       |            |
 *       |     compatibleWith
 *       |            |
 *       +------------+
 *
 *
 * 用于描述一个操作的输出如何跨多个分区进行切割。
 * compatibleWith、guarantees和satisfies方法用于描述子Partitioning之间、目标Partitioning之间及数据分布Distribution之间的关系，
 * 这些关系在这三个方法各自的文档中描述得更准确，但是在高层次的理解上：
 *
 * - satisfies方法是数据分区Partitioning和数据分布Distribution之间的关系。
 * - compatibleWith是操作的子节点输出的数据分区Partitioning之间的关系。
 * - guarantees是子节点输出的数据分区Partitioning和目标Partitioning之间的关系。
 *
 * 定义了一个物理算子输出数据的分区方式，具体包括子Partitionging之间、目标Partitioning和Distribution之间的关系。
 */
sealed trait Partitioning {
  /**
   * Returns the number of partitions that the data is split across
   *
   * 该SparkPlan输出RDD的分区的数目。
   **/
  val numPartitions: Int

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   *
   * 如果当前Partitioning做出的guarantee足以满足required参数规定的分区方案，
   * 即当前数据集不需要为required参数指定的分布重新分区，则返回 true （可能需要重新组织分区内的元组）。
   * 当不满足时（结果为false），一般需要进行repartition操作，对数据进行重新组织。
   */
  def satisfies(required: Distribution): Boolean

  /**
   * Returns true iff we can say that the partitioning scheme of this [[Partitioning]]
   * guarantees the same partitioning scheme described by `other`.
   *
   * Compatibility of partitionings is only checked for operators that have multiple children
   * and that require a specific child output [[Distribution]], such as joins.
   *
   * Intuitively, partitionings are compatible if they route the same partitioning key to the same
   * partition. For instance, two hash partitionings are only compatible if they produce the same
   * number of output partitionings and hash records according to the same hash function and
   * same partitioning key schema.
   *
   * Put another way, two partitionings are compatible with each other if they satisfy all of the
   * same distribution guarantees.
   *
   * 如果当前Partitioning的分区方案保证与other参数描述的分区方案相同，则返回true。
   * 仅针对具有多个子项且需要特定子项输出Distribution（例如Join）的运算符检查分区的兼容性。
   * 直观地说，如果分区将相同的分区键路由到相同的分区，则它们是兼容的。
   * 例如，两个Hash分区仅在它们根据相同的散列函数和相同的分区键模式产生相同数量的输出分区和散列记录时才兼容。
   * 换句话说，如果两个Partitioning满足所有相同的分布保证，则它们彼此兼容。
   *
   * 当存在多个子节点时，需要判断不同的子节点的分区操作是否兼容。
   * 直观地看，只有当两个Partitioning能够将相同key的数据分发到相同的分区时，才能够兼容。
   */
  def compatibleWith(other: Partitioning): Boolean

  /**
   * Returns true iff we can say that the partitioning scheme of this [[Partitioning]] guarantees
   * the same partitioning scheme described by `other`. If a `A.guarantees(B)`, then repartitioning
   * the child's output according to `B` will be unnecessary. `guarantees` is used as a performance
   * optimization to allow the exchange planner to avoid redundant repartitionings. By default,
   * a partitioning only guarantees partitionings that are equal to itself (i.e. the same number
   * of partitions, same strategy (range or hash), etc).
   *
   * In order to enable more aggressive optimization, this strict equality check can be relaxed.
   * For example, say that the planner needs to repartition all of an operator's children so that
   * they satisfy the [[AllTuples]] distribution. One way to do this is to repartition all children
   * to have the [[SinglePartition]] partitioning. If one of the operator's children already happens
   * to be hash-partitioned with a single partition then we do not need to re-shuffle this child;
   * this repartitioning can be avoided if a single-partition [[HashPartitioning]] `guarantees`
   * [[SinglePartition]].
   *
   * The SinglePartition example given above is not particularly interesting; guarantees' real
   * value occurs for more advanced partitioning strategies. SPARK-7871 will introduce a notion
   * of null-safe partitionings, under which partitionings can specify whether rows whose
   * partitioning keys contain null values will be grouped into the same partition or whether they
   * will have an unknown / random distribution. If a partitioning does not require nulls to be
   * clustered then a partitioning which _does_ cluster nulls will guarantee the null clustered
   * partitioning. The converse is not true, however: a partitioning which clusters nulls cannot
   * be guaranteed by one which does not cluster them. Thus, in general `guarantees` is not a
   * symmetric relation.
   *
   * Another way to think about `guarantees`: if `A.guarantees(B)`, then any partitioning of rows
   * produced by `A` could have also been produced by `B`.
   *
   * 如果当前Partitioning的分区方案保证与other参数描述的分区方案相同，则返回true。
   * 如果是`A.guarantees(B)`，则不需要根据`B`重新划分子级的输出。
   * `guarantees` 用作性能优化，以允许交换规划器避免冗余重新分区。
   * 默认情况下，分区仅保证与自身相等的分区（即相同数量的分区、相同的策略（Range或Hash）等）。
   *
   * 为了实现更积极的优化，可以放宽这种严格的相等性检查。
   * 例如，假设Planner需要重新划分操作算子的所有子节点，以便它们满足AllTuples分布。
   * 一种方法是重新分区所有子级以具有SinglePartition分区。
   * 如果操作算子的其中一个孩子已经碰巧用单个分区进行了哈希分区，那么我们不需要重新洗牌这个孩子；
   * 即在单分区HashPartitioning `guarantees` SinglePartition的情况下，可以避免这种重新分区。
   *
   * 上面给出的 SinglePartition 示例并不是特别有趣；`guarantees`的真正价值出现在更高级的分区策略中。
   * SPARK-7871将引入null-safe Partitioning的概念，在该概念下，Partitioning可以指定分区键包含空值的行是否将被分组到同一分区中，或者它们是否具有未知/随机分布。
   * 如果Partitioning不需要对null值进行聚集，那么对null值进行聚集的Partitioning将保证null值聚集Partitioning。
   * 反之则不然：聚集null的Partitioning不能由不聚集它们的Partitioning来保证。 因此，一般来说“保证”不是对称关系。
   *
   * 考虑`guarantees`的另一种方式：如果`A.guarantees(B)`，则`A`产生的任何行分区也可能由`B`产生。
   *
   * 如果A.gurantees(B)能够为真，那么任何A进行分区操作所产生的数据行也能够被B产生。
   * 这样，B就不需要再进行重分区操作。该方法主要用来避免冗余的重分区操作带来的性能代价。
   * 在默认情况下，一个Partitioning仅能够gurantee（保证）等于它本身的Partitioning（相同的分区数目和相同的分区策略等）。
   */
  def guarantees(other: Partitioning): Boolean = this == other
}

object Partitioning {
  def allCompatible(partitionings: Seq[Partitioning]): Boolean = {
    // Note: this assumes transitivity
    partitionings.sliding(2).map {
      case Seq(a) => true // 仅1个Partitioning，直接返回true
      case Seq(a, b) => // 否则需要滑动依次判断
        if (a.numPartitions != b.numPartitions) {
          // a不兼容b，且b不兼容a
          assert(!a.compatibleWith(b) && !b.compatibleWith(a))
          false
        } else {
          // a兼容b且b兼容a
          a.compatibleWith(b) && b.compatibleWith(a)
        }
    }.forall(_ == true)
  }
}

/**
 * 不进行分区。
 * @param numPartitions 分区数
 */
case class UnknownPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true // 只满足未指定数据分布的情况
    case _ => false
  }

  // 不兼容任何分区
  override def compatibleWith(other: Partitioning): Boolean = false

  // 不保证与任何分区的数据具有相同的分布
  override def guarantees(other: Partitioning): Boolean = false
}

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 *
 * 在1 ~ numPartitions范围内轮询方式分区
 *
 * @param numPartitions 分区数
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true // 只满足未指定数据分布的情况
    case _ => false
  }

  // 不兼容任何分区
  override def compatibleWith(other: Partitioning): Boolean = false

  // 不保证与任何分区的数据具有相同的分布
  override def guarantees(other: Partitioning): Boolean = false
}

case object SinglePartition extends Partitioning {
  // 分区数为1，常量
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false // 不兼容广播数据分布
    case _ => true // 其他都兼容
  }

  // 兼容分区数也为1的分区
  override def compatibleWith(other: Partitioning): Boolean = other.numPartitions == 1

  // 保证与分区数为1的分区具有相同的分区分布
  override def guarantees(other: Partitioning): Boolean = other.numPartitions == 1
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 *
 * 基于哈希的分区方式
 *
 * @param expressions 用来计算Hash值的表达式列表
 * @param numPartitions 分区的数目
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true // 满足未指定数据分布的情况
    // ClusteredDistribution分布的函数表达式满足当前HashPartitioning中进行Hash值计算的表达式时，即满足
    case ClusteredDistribution(requiredClustering) =>
      expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case o: HashPartitioning => this.semanticEquals(o) // 兼容语义相同的HashPartitioning分区
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: HashPartitioning => this.semanticEquals(o) // 保证与语义相同的HashPartitioning分区具有相同的分区分布
    case _ => false
  }

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   *
   * 基于当前Partitioning的Hash表达式返回用于产生有效分区ID的表达式（例如：非负数并且小于分区总数）
   */
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))
}

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner the following
 * two conditions are guaranteed to hold:
 *  - All row where the expressions in `ordering` evaluate to the same values will be in the same
 *    partition.
 *  - Each partition will have a `min` and `max` row, relative to the given ordering.  All rows
 *    that are in between `min` and `max` in this `ordering` will reside in this partition.
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 *
 * 表示分区根据ordering参数中指定的表达式的某些全排序跨分区拆分行。当以这种方式对数据进行分区时，可以保证以下两个条件成立：
 * - ordering参数中的表达式计算为相同值的所有行都将位于同一分区中。
 * - 每个分区都有一个“min”和“max”行，与给定的排序方式相关。在ordering参数指定的排序中，`min`和`max`之间的所有行都将驻留在该分区中。
 *
 * 这个类主要用于扩展表达式，以便将对表达式的转换下降到其子节点。
 *
 * 基于范围的分区方式。
 *
 * @param ordering 排序方式
 * @param numPartitions 分区总数
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true // 满足未指定数据分布的情况
    /**
     * 排序数据分布情况下，不需要当前Partitioning的排序方式与数据分布的排序方式完全相同，
     * 只需要头部完全相同即可；
     * 例如数据分布的排序方式是Order by a, b, c，
     * 那么分区的排序方式可以是Order by a, b，可以是Order by a, b, c，也可以是Order by a, b, c, d，
     * 但不能是Order by b, a这种顺序不一致的。
     */
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    // ClusteredDistribution分布的函数表达式满足当前HashPartitioning中进行Hash值计算的表达式时，即满足
    case ClusteredDistribution(requiredClustering) =>
      ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
    case _ => false // 其他情况不满足
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this.semanticEquals(o) // 兼容语义相同的RangePartitioning分区
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this.semanticEquals(o) // 保证与语义相同的RangePartitioning分区具有相同的分区分布
    case _ => false
  }
}

/**
 * A collection of [[Partitioning]]s that can be used to describe the partitioning
 * scheme of the output of a physical operator. It is usually used for an operator
 * that has multiple children. In this case, a [[Partitioning]] in this collection
 * describes how this operator's output is partitioned based on expressions from
 * a child. For example, for a Join operator on two tables `A` and `B`
 * with a join condition `A.key1 = B.key2`, assuming we use HashPartitioning schema,
 * there are two [[Partitioning]]s can be used to describe how the output of
 * this Join operator is partitioned, which are `HashPartitioning(A.key1)` and
 * `HashPartitioning(B.key2)`. It is also worth noting that `partitionings`
 * in this collection do not need to be equivalent, which is useful for
 * Outer Join operators.
 *
 * Partitioning的集合，可用于描述物理运算符输出的分区方案。它通常用于有多个子节点的操作符。
 * 在这种情况下，此集合中的Partitioning描述了该运算符的输出如何根据子代的表达式进行分区。
 * 例如，对于两个表`A`和`B`上的Join操作符，连接条件为`A.key1 = B.key2`，假设我们使用HashPartitioning模式，
 * 有两个Partitioning可以用来描述这个Join运算符的输出是如何分区的：`HashPartitioning(A.key1)` 和 `HashPartitioning(B.key2)`。
 * 还值得注意的是，`partitionings`集合中Partitioning不需要是等价的，这对于Outer Join操作符很有用。
 *
 * 分区方式的集合，描述物理算子的输出。
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  // 集合内所有的Partitioning都需要有相同的分区总数
  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  // 所有Partitioning的分区总数相同
  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies(required: Distribution): Boolean =
    // 任意一个Partitioning满足即可
    partitionings.exists(_.satisfies(required))

  /**
   * Returns true if any `partitioning` of this collection is compatible with
   * the given [[Partitioning]].
   */
  override def compatibleWith(other: Partitioning): Boolean =
    // 任意一个Partitioning兼容即可
    partitionings.exists(_.compatibleWith(other))

  /**
   * Returns true if any `partitioning` of this collection guarantees
   * the given [[Partitioning]].
   */
  override def guarantees(other: Partitioning): Boolean =
    // 任意一个Partitioning可以保证即可
    partitionings.exists(_.guarantees(other))

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}

/**
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
 *
 * @param mode 广播模式，有Hash及原始分布两种模式
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  // 分区总数为1，常量
  override val numPartitions: Int = 1

  override def satisfies(required: Distribution): Boolean = required match {
    case BroadcastDistribution(m) if m == mode => true // 兼容广播模式相同的BroadcastPartitioning分区
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning(m) if m == mode => true // 保证与广播模式相同的BroadcastPartitioning分区具有相同的分区分布
    case _ => false
  }
}
