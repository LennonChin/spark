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
 * 广播分布，数据会被广播到所有节点上
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
   * 当前的Partitioning操作能否得到所需的数据分布（Required）。
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
 * @param numPartitions
 */
case class UnknownPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = false

  override def guarantees(other: Partitioning): Boolean = false
}

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 *
 * 在1 ~ numPartitions范围内轮询方式分区
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = false

  override def guarantees(other: Partitioning): Boolean = false
}

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }

  override def compatibleWith(other: Partitioning): Boolean = other.numPartitions == 1

  override def guarantees(other: Partitioning): Boolean = other.numPartitions == 1
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 *
 * 基于哈希的分区方式
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case ClusteredDistribution(requiredClustering) =>
      expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case o: HashPartitioning => this.semanticEquals(o)
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: HashPartitioning => this.semanticEquals(o)
    case _ => false
  }

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
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
 * 基于范围的分区方式
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    case ClusteredDistribution(requiredClustering) =>
      ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this.semanticEquals(o)
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this.semanticEquals(o)
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
 * 分区方式的集合，描述物理算子的输出
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  /**
   * Returns true if any `partitioning` of this collection is compatible with
   * the given [[Partitioning]].
   */
  override def compatibleWith(other: Partitioning): Boolean =
    partitionings.exists(_.compatibleWith(other))

  /**
   * Returns true if any `partitioning` of this collection guarantees
   * the given [[Partitioning]].
   */
  override def guarantees(other: Partitioning): Boolean =
    partitionings.exists(_.guarantees(other))

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}

/**
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies(required: Distribution): Boolean = required match {
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }

  override def compatibleWith(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning(m) if m == mode => true
    case _ => false
  }
}
