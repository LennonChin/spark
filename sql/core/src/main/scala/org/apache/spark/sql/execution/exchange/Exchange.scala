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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * Exchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.
 */
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * A wrapper for reused exchange to have different output, because two exchanges which produce
 * logically identical output will have distinct sets of output attribute ids, so we need to
 * preserve the original ids because they're what downstream operators are expecting.
 */
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {

  override def sameResult(plan: SparkPlan): Boolean = {
    // Ignore this wrapper. `plan` could also be a ReusedExchange, so we reverse the order here.
    plan.sameResult(child)
  }

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }
}

/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
case class ReuseExchange(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) { // spark.sql.exchange.reuse
      return plan
    }
    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    // 使用HashMap对Exchange进行散列标记
    val exchanges = mutable.HashMap[StructType, ArrayBuffer[Exchange]]()
    plan.transformUp { // 后序遍历所有的子节点
      case exchange: Exchange => // 遇到Exchange节点
        // the exchanges that have same results usually also have same schemas (same column names).
        /**
         * 从HashMap中根据Schema查找对应的Exchange数组，如果没找到就创建一个空数组。
         * 如果Exchange获得的结果相同，那么这些Exchange的Schema是一样（即有相同的列名）。
         */
        val sameSchema = exchanges.getOrElseUpdate(exchange.schema, ArrayBuffer[Exchange]())
        // 查找是否已经存在与自己语义相同的Exchange
        val samePlan = sameSchema.find { e =>
          exchange.sameResult(e)
        }
        // 如果存在，就构建ReusedExchangeExec节点返回
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          ReusedExchangeExec(exchange.output, samePlan.get)
        } else {
          sameSchema += exchange
          exchange
        }
    }
  }
}
