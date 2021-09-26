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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  // Use special getter for primitive types (for UnsafeRow)
  // 根据索引从InternalRow行数据中查找对应索引位的列数据
  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(ordinal)) {
      // 找不到对应的列，返回null
      null
    } else {
      // 能找到对应的列，根据类型和索引进行获取
      dataType match {
        case BooleanType => input.getBoolean(ordinal)
        case ByteType => input.getByte(ordinal)
        case ShortType => input.getShort(ordinal)
        case IntegerType | DateType => input.getInt(ordinal)
        case LongType | TimestampType => input.getLong(ordinal)
        case FloatType => input.getFloat(ordinal)
        case DoubleType => input.getDouble(ordinal)
        case StringType => input.getUTF8String(ordinal)
        case BinaryType => input.getBinary(ordinal)
        case CalendarIntervalType => input.getInterval(ordinal)
        case t: DecimalType => input.getDecimal(ordinal, t.precision, t.scale)
        case t: StructType => input.getStruct(ordinal, t.size)
        case _: ArrayType => input.getArray(ordinal)
        case _: MapType => input.getMap(ordinal)
        case _ => input.get(ordinal, dataType)
      }
    }
  }

  // Code generate
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // 列数据类型对应的Java类型
    val javaType = ctx.javaType(dataType)

    // 获取列值
    val value = ctx.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)

    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      val oev = ctx.currentVars(ordinal)
      ev.isNull = oev.isNull // 是否可为空的标记字段名称
      ev.value = oev.value // 列值的字段名称
      val code = oev.code
      oev.code = ""
      ev.copy(code = code)
    } else if (nullable) {
      /**
       * 列值可为null，需要进行null值的判断。
       * 例如对于BindReference(2, String, true)，生成的代码如下：
       * boolean evIsnull = inputRow.isNullAt(ordinal);
       * String evValue = evIsnull ? "null" : evValue;
       */
      ev.copy(code = s"""
        boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
        $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($value);""")
    } else {
      ev.copy(code = s"""$javaType ${ev.value} = $value;""", isNull = "false")
    }
  }
}

object BindReferences extends Logging {

  /**
   * 该操作主要用于将参数expression中涉及列与input参数中对应的属性进行绑定。
   * 比如input参数表示 [a: Int, b: String, c: Decimal] 三个属性，而expression中用到了b属性，且b不为空，
   * 那么会生成 BoundReference(1, String, false) 返回，
   * 其中1表示expression中b在input中索引是1，String表示b的类型，false表示b不能为空。
   *
   * allowFailures参数用于控制没有找到的情况下如何处理，
   * 如果为true就直接返回原来的expression，否则输出错误日志。
   *
   * @param expression
   * @param input
   * @param allowFailures
   * @tparam A
   * @return
   */
  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference => // 先序遍历
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          }
        } else {
          // a的索引，a的数据类型，a是否可为空
          BoundReference(ordinal, a.dataType, input(ordinal).nullable)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }
}
