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

package org.apache.spark.sql.catalyst.rules

import scala.collection.JavaConverters._
import com.google.common.util.concurrent.AtomicLongMap
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.debugger.PlanTreeTraversal
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.util.Utils

object RuleExecutor {
  protected val timeMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules */
  def resetTime(): Unit = timeMap.clear()

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxSize = map.keys.map(_.toString.length).max
    map.toSeq.sortBy(_._2).reverseMap { case (k, v) =>
      s"${k.padTo(maxSize, " ").mkString} $v"
    }.mkString("\n", "\n", "")
  }
}

/**
 * 有了各种具体Rule规则后，还需要驱动程序来调用这些规则，在Catalyst中这个功能由RuleExecutor提供。
 * 凡是涉及树型结构的转换过程（如Analyzer逻辑算子树分析过程、Optimizer逻辑算子树的优化过程和后续物理算子树的生成过程等），
 * 都要实施规则匹配和节点处理，都继承自RuleExecutor[TreeType]抽象类。
 *
 * @tparam TreeType
 */
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /**
   * A batch of rules.
   *
   * 每个Batch代表一套规则，配备一个策略，该策略说明了迭代次数（一次还是多次）。
   **/
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /**
   * Defines a sequence of rule batches, to be overridden by the implementation.
   *
   * 该RuleExecutor的处理步骤。
   * 每个Batch代表一套规则，配备一个策略，该策略说明了迭代次数（一次还是多次）。
   **/
  protected def batches: Seq[Batch]


  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   *
   * 按照batches顺序和batch内的Rules顺序，对传入的plan里的节点进行迭代处理，处理逻辑由具体Rule子类实现。
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    PlanTreeTraversal.addTree(curPlan.nestedJsonValue, this.logName)

    /**
     * Substitution(100) -> Resolution(100) -> Nondeterministic(1) -> UDF(1) -> FixNullability(1) -> Cleanup(100)
     */
    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        // 此处的foldLeft具体逻辑如下：
        // foldLeft(TreeNode) {
        //    (TreeNode, Rule) => {
        //        使用Rule的apply方法将Rule运用到TreeNode上，
        //        返回TreeNode
        //    }
        // }
        // foldLeft最终返回值为TreeNode，且LogicalPlan继承自TreeNode
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            /**
             * 作用Rule到LogicalPlan上
             * 在LogicalPlan中，还会将Rule递归作用到自己的子节点上
             * 返回解析后的LogicalPlan
             */
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime

            // 记录时间
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)

            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
              PlanTreeTraversal.addTreeByRule(rule.ruleName, plan.nestedJsonValue, this.logName)
            }

            result
        }

        // 遍历次数 + 1
        iteration += 1
        /**
         * 如果遍历次数大于Batch的策略最大次数，就会停止while遍历
         * 下列if分支内仅仅是打印日志
         */
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            if (Utils.isTesting) {
              throw new TreeNodeException(curPlan, message, null)
            } else {
              logWarning(message)
            }
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      } // end while

      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
          |=== Result of Batch ${batch.name} ===
          |${sideBySide(plan.treeString, curPlan.treeString).mkString("\n")}
        """.stripMargin)
        PlanTreeTraversal.addTreeByBatchRule(batch.name, curPlan.nestedJsonValue, this.logName)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    } // end batches.foreach

    curPlan
  }
}
