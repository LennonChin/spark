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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Rule that resolves table-valued function references.
 * 解析Table-Valued函数引用
 */
object ResolveTableValuedFunctions extends Rule[LogicalPlan] {
  /**
   * List of argument names and their types, used to declare a function.
   */
  private case class ArgumentList(args: (String, DataType)*) {
    /**
     * Try to cast the expressions to satisfy the expected types of this argument list. If there
     * are any types that cannot be casted, then None is returned.
     *
     * 尝试转换表达式以满足参数的预期类型，如果转换失败就返回None。
     * 比如对于range(0, 10, 2, 3)生成Range(0, 10, 2, Some(3))，对应的的类型如下：
     * tvf("start" -> LongType, "end" -> LongType, "step" -> LongType, "numPartitions" -> IntegerType)
     *
     * 这个方法会对Range中的各个参数尝试转换。
     */
    def implicitCast(values: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.length == values.length) {
        val casted = values.zip(args).map { case (value, (_, expectedType)) =>
          TypeCoercion.ImplicitTypeCasts.implicitCast(value, expectedType)
        }
        if (casted.forall(_.isDefined)) {
          return Some(casted.map(_.get))
        }
      }
      None
    }

    override def toString: String = {
      args.map { a =>
        s"${a._1}: ${a._2.typeName}"
      }.mkString(", ")
    }
  }

  /**
   * A TVF maps argument lists to resolver functions that accept those arguments. Using a map
   * here allows for function overloading.
   */
  private type TVF = Map[ArgumentList, Seq[Any] => LogicalPlan]

  /**
   * TVF builder.
   */
  private def tvf(args: (String, DataType)*)(pf: PartialFunction[Seq[Any], LogicalPlan])
      : (ArgumentList, Seq[Any] => LogicalPlan) = {
    (ArgumentList(args: _*),
     pf orElse {
       case args =>
         throw new IllegalArgumentException(
           "Invalid arguments for resolved function: " + args.mkString(", "))
     })
  }

  /**
   * Internal registry of table-valued functions.
   */
  private val builtinFunctions: Map[String, TVF] = Map(
    "range" -> Map(
      /* range(end) -> Range(0, end, 1, None) */
      tvf("end" -> LongType) { case Seq(end: Long) =>
        Range(0, end, 1, None)
      },

      /* range(start, end) -> Range(start, end, 1, None)  */
      tvf("start" -> LongType, "end" -> LongType) { case Seq(start: Long, end: Long) =>
        Range(start, end, 1, None)
      },

      /* range(start, end, step) -> Range(start, end, step, None) */
      tvf("start" -> LongType, "end" -> LongType, "step" -> LongType) {
        case Seq(start: Long, end: Long, step: Long) =>
          Range(start, end, step, None)
      },

      /* range(start, end, step, numPartitions) -> Range(start, end, step, Some(numPartitions)) */
      tvf("start" -> LongType, "end" -> LongType, "step" -> LongType,
          "numPartitions" -> IntegerType) {
        case Seq(start: Long, end: Long, step: Long, numPartitions: Int) =>
          Range(start, end, step, Some(numPartitions))
      })
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // 匹配UnresolvedTableValuedFunction节点，同时它的函数参数都应该已经解析好了
    case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
      /**
       * 从builtinFunctions中根据函数名进行获取，目前只支持range函数
       * 该过程会对Range的start、end、step及numSlices四个参数进行匹配和填充，
       * 没有指定的会填充为默认值。
       * 1. start的默认值是0。
       * 2. step的默认值是1。
       * 3. numSlices的默认值是None。
       * 3. end必须指定。
       */
      builtinFunctions.get(u.functionName) match {
        case Some(tvf) =>
          val resolved = tvf.flatMap { case (argList: ResolveTableValuedFunctions.ArgumentList, resolver) =>
            // 对函数的各个参数尝试转换为正确的类型。
            argList.implicitCast(u.functionArgs) match {
              case Some(casted) =>
                Some(resolver(casted.map(_.eval())))
              case _ =>
                None
            }
          }
          resolved.headOption.getOrElse {
            val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
            u.failAnalysis(
              s"""error: table-valued function ${u.functionName} with alternatives:
                |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
                |cannot be applied to: (${argTypes})""".stripMargin)
          }
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }
  }
}
