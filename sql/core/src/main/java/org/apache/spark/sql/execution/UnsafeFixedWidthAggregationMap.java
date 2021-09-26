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

package org.apache.spark.sql.execution;

import java.io.IOException;

import org.apache.spark.SparkEnv;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This map supports a maximum of 2 billion keys.
 *
 * 基于Unsafe的HashMap，用于执行聚合操作，聚合的值是需要是定长的。
 * 这个Map最多支持20亿个键（即Int最大值）。
 */
public final class UnsafeFixedWidthAggregationMap {

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   *
   * 该属性就是Unsafe的getBytes()方法得到的字节数组。
   * 一个空的聚合缓冲区，会被编码为UnsafeRow格式。
   * 当向当前Map中插入一个新的键时，会拷贝这个缓冲区用于值。
   */
  private final byte[] emptyAggregationBuffer;

  // 聚合缓冲区Schema
  private final StructType aggregationBufferSchema;

  // 分组键Schema
  private final StructType groupingKeySchema;

  /**
   * Encodes grouping keys as UnsafeRows.
   *
   * 用于将分组键编码为UnsafeRow。
   */
  private final UnsafeProjection groupingKeyProjection;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   *
   * HashMap，存储bytearray类型的key和value。
   * BytesToBytesMap中键和值的字节会连续存储，每个键值对都记录了键和值的字节长度，以便可以在连续的字节数据中定位键和值。
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   *
   * 可重用的指向当前聚合缓冲区的指针。
   */
  private final UnsafeRow currentAggregationBuffer;

  private final boolean enablePerfMetrics;

  /**
   * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   *         如果UnsafeFixedWidthAggregationMap对给定的schema支持聚合缓冲，就返回true，否则返回false。
   *         判别条件是Schema中所有Field的类型都是NullType、BooleanType、ByteType、ShortType、IntegerType、LongType、
   *         FloatType、DoubleType、DateType、TimestampType、DecimalType其中一种。
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!UnsafeRow.isMutable(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   *                               新的键的默认值（对聚合函数所谓的“零值”）
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   *                                聚合缓冲区的Schema，用于行转换。
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   *                          分组键的Schema，用于行转换。
   * @param taskMemoryManager the memory manager used to allocate our Unsafe memory structures.
   *                          用于分配Unsafe内存结构的MemoryManager。
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   *                        Map的初始大小（避免Rehash操作的大小Hint）
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   *                      数据也大小，限制了最大记录的大小。
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   *                          如果为true，性能度量信息会被记录（有较小的性能影响）
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskMemoryManager taskMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.aggregationBufferSchema = aggregationBufferSchema;

    // 当前聚合缓冲，是一个UnsafeRow对象，列数量为聚合缓冲区Schema内列的数量。
    this.currentAggregationBuffer = new UnsafeRow(aggregationBufferSchema.length());

    // 根据分组键Schema创建UnsafeProjection投影器。
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;

    // 创建BytesToBytesMap
    this.map =
      new BytesToBytesMap(taskMemoryManager, initialCapacity, pageSizeBytes, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;

    // Initialize the buffer for aggregation value
    // 初始化聚合缓冲区
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object. If additional memory could not be allocated, then this method will
   * signal an error by returning null.
   *
   * 返回当前分组的聚合缓冲区。为了性能考虑，所有对该方法的调用都会返回同一个对象。
   * 如果没有更多的内存可以被分配，这个方法会通过返回null来告知外界产生了错误。
   *
   * 传入的分组键，使用UnsafeProjection得到新缓冲区的UnsafeRow
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

    return getAggregationBufferFromUnsafeRow(unsafeGroupingKeyRow);
  }

  /**
   * 该方法会通过给定的键，去BytesToBytesMap中查找值的位置，
   * 如果没有查找到，就尝试在BytesToBytesMap中添加该键，对应的值是emptyAggregationBuffer属性，
   * 然后将currentAggregationBuffer指针指向BytesToBytesMap中对应值的位置。
   */
  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key) {
    return getAggregationBufferFromUnsafeRow(key, key.hashCode());
  }

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key, int hash) {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      key.getBaseObject(),
      key.getBaseOffset(),
      key.getSizeInBytes(),
      hash);
    if (!loc.isDefined()) { // 没找到，尝试新建
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      // 这是第一次没有找到分组键，因此我们需要将当前空聚合缓冲区的拷贝插入到Map中。
      boolean putSucceeded = loc.append(
        key.getBaseObject(),
        key.getBaseOffset(),
        key.getSizeInBytes(),
        emptyAggregationBuffer, // byte[]类型聚合缓冲区
        Platform.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length // 聚合缓冲区长度
      );
      if (!putSucceeded) {
        return null; // 新建失败，返回null
      }
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    // 将currentAggregationBuffer指针重置，指向我们在BytesToBytesMap中存放的值的位置。
    currentAggregationBuffer.pointTo(
      loc.getValueBase(),
      loc.getValueOffset(),
      loc.getValueLength()
    );
    return currentAggregationBuffer;
  }

  /**
   * Returns an iterator over the keys and values in this map. This uses destructive iterator of
   * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
   * been called.
   *
   * For efficiency, each call returns the same object.
   *
   * 返回当前Map基于键值对的迭代器。
   * 使用了BytesToBytesMap的具有破坏性的迭代器。
   * 因此在该方法会调用后，再在当前Map上调用其他任何方法都是非法的。
   */
  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      // 获取BytesToBytesMap的迭代器
      private final BytesToBytesMap.MapIterator mapLocationIterator =
        map.destructiveIterator();

      // 创建用于引用对应键值的UnsafeRow对象。
      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());

      @Override
      public boolean next() {

        // 检查是否还存在键值对
        if (mapLocationIterator.hasNext()) {
          // 获取下一个键值对的位置
          final BytesToBytesMap.Location loc = mapLocationIterator.next();

          // 将键值的UnsafeRow对象定位到对应的位置
          key.pointTo(
            loc.getKeyBase(),
            loc.getKeyOffset(),
            loc.getKeyLength()
          );
          value.pointTo(
            loc.getValueBase(),
            loc.getValueOffset(),
            loc.getValueLength()
          );
          return true;
        } else {
          return false;
        }
      }

      // 获取键
      @Override
      public UnsafeRow getKey() {
        return key;
      }

      // 获取值
      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // Do nothing.
      }
    };
  }

  /**
   * Return the peak memory used so far, in bytes.
   *
   * 返回迄今为止使用的内存的峰值。
   */
  public long getPeakMemoryUsedBytes() {
    return map.getPeakMemoryUsedBytes();
  }

  /**
   * Free the memory associated with this map. This is idempotent and can be called multiple times.
   *
   * 释放BytesToBytesMap分配的内存，该方法多次调用时幂等的。
   */
  public void free() {
    map.free();
  }

  // 打印性能度量信息
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
  }

  /**
   * Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVExternalSorter]]
   *
   * Note that the map will be reset for inserting new records, and the returned sorter can NOT be
   * used to insert records.
   *
   * 对Map内的数据进行排序，然后溢写到磁盘，返回一个UnsafeKVExternalSorter
   *
   * Map会被重置并用于插入新的数据，返回的排序器不能插入数据。
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    return new UnsafeKVExternalSorter(
      groupingKeySchema, // 分组键Schema
      aggregationBufferSchema, // 聚合缓冲区Schema
      SparkEnv.get().blockManager(), // BlockManager
      SparkEnv.get().serializerManager(), // SerializerManager
      map.getPageSizeBytes(), // BytesToBytesMap页大小
      SparkEnv.get().conf().getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD), // 溢写元素数量阈值
      map); // BytesToBytesMap
  }
}
