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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparator, PrefixComparators}

object SortPrefixUtils {

  /**
   * A dummy prefix comparator which always claims that prefixes are equal. This is used in cases
   * where we don't know how to generate or compare prefixes for a SortOrder.
   */
  private object NoOpPrefixComparator extends PrefixComparator {
    override def compare(prefix1: Long, prefix2: Long): Int = 0
  }

  /**
   * Dummy sort prefix result to use for empty rows.
   */
  private val emptyPrefix = new UnsafeExternalRowSorter.PrefixComputer.Prefix

  def getPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    // 根据排序值类型的不同创建比较器
    sortOrder.dataType match {
      case StringType => stringPrefixComparator(sortOrder)
      case BinaryType => binaryPrefixComparator(sortOrder)
      case BooleanType | ByteType | ShortType | IntegerType | LongType | DateType | TimestampType =>
        longPrefixComparator(sortOrder)
      case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
        longPrefixComparator(sortOrder)
      case FloatType | DoubleType => doublePrefixComparator(sortOrder)
      case dt: DecimalType => doublePrefixComparator(sortOrder)
      case _ => NoOpPrefixComparator
    }
  }

  private def stringPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    sortOrder.direction match {
      case Ascending if (sortOrder.nullOrdering == NullsLast) =>
        PrefixComparators.STRING_NULLS_LAST
      case Ascending =>
        PrefixComparators.STRING
      case Descending if (sortOrder.nullOrdering == NullsFirst) =>
        PrefixComparators.STRING_DESC_NULLS_FIRST
      case Descending =>
        PrefixComparators.STRING_DESC
    }
  }

  private def binaryPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    sortOrder.direction match {
      case Ascending if (sortOrder.nullOrdering == NullsLast) =>
        PrefixComparators.BINARY_NULLS_LAST
      case Ascending =>
        PrefixComparators.BINARY
      case Descending if (sortOrder.nullOrdering == NullsFirst) =>
        PrefixComparators.BINARY_DESC_NULLS_FIRST
      case Descending =>
        PrefixComparators.BINARY_DESC
    }
  }

  private def longPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    sortOrder.direction match {
      case Ascending if (sortOrder.nullOrdering == NullsLast) =>
        PrefixComparators.LONG_NULLS_LAST
      case Ascending =>
        PrefixComparators.LONG
      case Descending if (sortOrder.nullOrdering == NullsFirst) =>
        PrefixComparators.LONG_DESC_NULLS_FIRST
      case Descending =>
        PrefixComparators.LONG_DESC
    }
  }

  private def doublePrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    sortOrder.direction match {
      case Ascending if (sortOrder.nullOrdering == NullsLast) =>
        PrefixComparators.DOUBLE_NULLS_LAST
      case Ascending =>
        PrefixComparators.DOUBLE
      case Descending if (sortOrder.nullOrdering == NullsFirst) =>
        PrefixComparators.DOUBLE_DESC_NULLS_FIRST
      case Descending =>
        PrefixComparators.DOUBLE_DESC
    }
  }

  /**
   * Creates the prefix comparator for the first field in the given schema, in ascending order.
   */
  def getPrefixComparator(schema: StructType): PrefixComparator = {
    if (schema.nonEmpty) {
      // 使用Schema的第一个Field创建比较器
      val field = schema.head
      getPrefixComparator(SortOrder(BoundReference(0, field.dataType, field.nullable), Ascending))
    } else {
      // Schema为空，比较时总是返回0
      new PrefixComparator {
        override def compare(prefix1: Long, prefix2: Long): Int = 0
      }
    }
  }

  /**
   * Returns whether the specified SortOrder can be satisfied with a radix sort on the prefix.
   */
  def canSortFullyWithPrefix(sortOrder: SortOrder): Boolean = {
    sortOrder.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | DateType |
           TimestampType | FloatType | DoubleType =>
        true
      case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
        true
      case _ =>
        false
    }
  }

  /**
   * Returns whether the fully sorting on the specified key field is possible with radix sort.
   */
  def canSortFullyWithPrefix(field: StructField): Boolean = {
    canSortFullyWithPrefix(SortOrder(BoundReference(0, field.dataType, field.nullable), Ascending))
  }

  /**
   * Creates the prefix computer for the first field in the given schema, in ascending order.
   *
   * 根据给定的Schema中第一个字段创建前缀计算器，以升序方式
   */
  def createPrefixGenerator(schema: StructType): UnsafeExternalRowSorter.PrefixComputer = {
    if (schema.nonEmpty) { // schema不为空
      // 取第一个字段
      val boundReference = BoundReference(0, schema.head.dataType, nullable = true)

      // 创建排序的前缀Expression
      val prefixExpr = SortPrefix(SortOrder(boundReference, Ascending))

      // 对前缀Expression进行Projection封装
      val prefixProjection = UnsafeProjection.create(prefixExpr)

      new UnsafeExternalRowSorter.PrefixComputer {
        private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix

        // 对输入的行计算前缀
        override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {

          // 先使用前缀投影器包装数据行
          val prefix = prefixProjection.apply(row)

          // 根据投影后的值进行判断
          if (prefix.isNullAt(0)) {
            // 投影后前缀值为空
            result.isNull = true
            result.value = prefixExpr.nullValue
          } else {
            // 投影后前缀值不为空
            result.isNull = false
            // 前缀值统一为Long
            result.value = prefix.getLong(0)
          }
          result
        }
      }
    } else {
      // schema为空，每次计算返回的都是emptyPrefix，即UnsafeExternalRowSorter.PrefixComputer.Prefix
      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
          emptyPrefix
        }
      }
    }
  }
}
