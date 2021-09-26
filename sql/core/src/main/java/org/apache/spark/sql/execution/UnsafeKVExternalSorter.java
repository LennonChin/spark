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

import javax.annotation.Nullable;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.*;

/**
 * A class for performing external sorting on key-value records. Both key and value are UnsafeRows.
 *
 * Note that this class allows optionally passing in a {@link BytesToBytesMap} directly in order
 * to perform in-place sorting of records in the map.
 *
 * 用于对K-V结构的记录进行外部排序的实现类。Key和Value都是UnsafeRow类型的。
 *
 * 需要注意的是，这个类允许直接传入BytesToBytesMap以便对Map中的记录进行原地排序，这个操作是可选的。
 */
public final class UnsafeKVExternalSorter {

  // 键Schema，一般来说是分组键的Schema
  private final StructType keySchema;
  // 值Schema，一般来说是聚合缓冲区的Schema
  private final StructType valueSchema;

  // 前缀计算器
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;

  // Unsafe外部排序器
  private final UnsafeExternalSorter sorter;

  // 该构造方法没有被用到过
  public UnsafeKVExternalSorter(
      StructType keySchema,
      StructType valueSchema,
      BlockManager blockManager,
      SerializerManager serializerManager,
      long pageSizeBytes,
      long numElementsForSpillThreshold) throws IOException {
    this(keySchema, valueSchema, blockManager, serializerManager, pageSizeBytes,
      numElementsForSpillThreshold, null);
  }

  public UnsafeKVExternalSorter(
      StructType keySchema,
      StructType valueSchema,
      BlockManager blockManager,
      SerializerManager serializerManager,
      long pageSizeBytes,
      long numElementsForSpillThreshold,
      @Nullable BytesToBytesMap map) throws IOException {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;

    // 获取Task上下文对象
    final TaskContext taskContext = TaskContext.get();

    // 通过分组键创建前缀计算器、前缀比较器、比较顺序
    prefixComputer = SortPrefixUtils.createPrefixGenerator(keySchema);
    PrefixComparator prefixComparator = SortPrefixUtils.getPrefixComparator(keySchema);

    // Codegen返回SpecificOrdering
    BaseOrdering ordering = GenerateOrdering.create(keySchema);

    // 创建记录比较器
    KVComparator recordComparator = new KVComparator(ordering, keySchema.length());

    // 判断是否可以用基数排序，只有一个分组键，且分组键是基础类型是可用基数排序
    boolean canUseRadixSort = keySchema.length() == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(keySchema.apply(0));

    // Task内存管理器
    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();

    if (map == null) {
      // 传入的BytesToBytesMap为空，创建UnsafeExternalSorter排序器
      sorter = UnsafeExternalSorter.create(
        taskMemoryManager,
        blockManager,
        serializerManager,
        taskContext,
        recordComparator, // 记录比较器
        prefixComparator, // 前缀比较器
        SparkEnv.get().conf().getInt("spark.shuffle.sort.initialBufferSize",
                                     UnsafeExternalRowSorter.DEFAULT_INITIAL_SORT_BUFFER_SIZE), // Shuffle Sort缓冲区大小，默认4K
        pageSizeBytes, // 页大小
        numElementsForSpillThreshold,
        canUseRadixSort);
    } else {
      // The array will be used to do in-place sort, which require half of the space to be empty.
      // 当使用原地排序时，需要预留一半的空间保证排序的执行。
      assert(map.numKeys() <= map.getArray().size() / 2);
      // During spilling, the array in map will not be used, so we can borrow that and use it
      // as the underlying array for in-memory sorter (it's always large enough).
      // Since we will not grow the array, it's fine to pass `null` as consumer.
      /**
       * 创建UnsafeInMemorySorter用于在内存中排序。
       * 在溢写时，Map中的数组不会被使用，所以我们可以借用它作为内存排序底层的数组。
       * 由于我们不会要求该数组大小增长（即不会项TaskMemoryManager申请内存），因此Consumer会传入null。
       */
      final UnsafeInMemorySorter inMemSorter = new UnsafeInMemorySorter(
        null, taskMemoryManager, recordComparator, prefixComparator, map.getArray(),
        canUseRadixSort);

      // We cannot use the destructive iterator here because we are reusing the existing memory
      // pages in BytesToBytesMap to hold records during sorting.
      // The only new memory we are allocating is the pointer/prefix array.
      /**
       * 我们不会使用具有破坏性的迭代器，因为我们需要重用BytesToBytesMap中的内存页，以便在排序过程中存放记录。
       * 我们仅仅需要为存放指针和前缀的数组申请新的内存。
       */
      BytesToBytesMap.MapIterator iter = map.iterator();

      // 根据分组列的数量创建UnsafeRow对象
      final int numKeyFields = keySchema.size();
      UnsafeRow row = new UnsafeRow(numKeyFields);

      // 不断迭代BytesToBytesMap中的键值对
      while (iter.hasNext()) {

        // 获取键对应的Base Object
        final BytesToBytesMap.Location loc = iter.next();
        final Object baseObject = loc.getKeyBase();
        final long baseOffset = loc.getKeyOffset();

        // Get encoded memory address
        // baseObject + baseOffset point to the beginning of the key data in the map, but that
        // the KV-pair's length data is stored in the word immediately before that address
        /**
         * 获取编码的内存地址。
         * baseObject + baseOffset指向键数据在Map中的起始位置，
         * 但是KV对的长度数据是存放在位于该地址前面的一个字中（即前面的8字节）。
         */
        MemoryBlock page = loc.getMemoryPage();

        // 这里定位到了KV对的长度数据起始位置
        long address = taskMemoryManager.encodePageNumberAndOffset(page, baseOffset - 8);

        // Compute prefix
        // 将row定位到键的偏移量位置上，以计算前缀
        row.pointTo(baseObject, baseOffset, loc.getKeyLength());
        final UnsafeExternalRowSorter.PrefixComputer.Prefix prefix =
          prefixComputer.computePrefix(row);

        // 将地址，前缀等信息插入到InMemorySorter中。
        inMemSorter.insertRecord(address, prefix.value, prefix.isNull);
      }

      // 根据上面创建的InMemorySorter创建UnsafeExternalSorter
      sorter = UnsafeExternalSorter.createWithExistingInMemorySorter(
        taskMemoryManager,
        blockManager,
        serializerManager,
        taskContext,
        new KVComparator(ordering, keySchema.length()), // 键值比较器
        prefixComparator, // 前缀比较器
        SparkEnv.get().conf().getInt("spark.shuffle.sort.initialBufferSize",
                                     UnsafeExternalRowSorter.DEFAULT_INITIAL_SORT_BUFFER_SIZE),
        pageSizeBytes,
        numElementsForSpillThreshold,
        inMemSorter);

      // reset the map, so we can re-use it to insert new records. the inMemSorter will not used
      // anymore, so the underline array could be used by map again.
      /**
       * 重置BytesToBytesMap，这样我们可以从新使用它来插入新的记录。
       * InMemorySorter不再使用了，因此，底层的数组可以被Map重新使用。
       */
      map.reset();
    }
  }

  /**
   * Inserts a key-value record into the sorter. If the sorter no longer has enough memory to hold
   * the record, the sorter sorts the existing records in-memory, writes them out as partially
   * sorted runs, and then reallocates memory to hold the new record.
   *
   * 插入一个键值对记录到Sorter中。
   * 如果Sorter没有足够的内存来存放该记录，它会对内存中已经存放的记录进行排序，将它们进行部分排序并写出，
   * 然后重新申请内存以存放新的记录。
   */
  public void insertKV(UnsafeRow key, UnsafeRow value) throws IOException {
    // 根据键计算前缀
    final UnsafeExternalRowSorter.PrefixComputer.Prefix prefix =
      prefixComputer.computePrefix(key);
    // 向Sorter中插入
    sorter.insertKVRecord(
      key.getBaseObject(), key.getBaseOffset(), key.getSizeInBytes(),
      value.getBaseObject(), value.getBaseOffset(), value.getSizeInBytes(),
      prefix.value, prefix.isNull);
  }

  /**
   * Merges another UnsafeKVExternalSorter into `this`, the other one will be emptied.
   *
   * 合并传入的另一个UnsafeKVExternalSorter到当前的Sorter中并且清空它
   *
   * @throws IOException
   */
  public void merge(UnsafeKVExternalSorter other) throws IOException {
    sorter.merge(other.sorter);
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   *
   * 返回一个排序后的迭代器。
   * 当调用者消费完该迭代器后，应该调用`cleanupResources()`方法以清理资源
   */
  public KVSorterIterator sortedIterator() throws IOException {
    try {
      // 获取排序后的迭代器
      final UnsafeSorterIterator underlying = sorter.getSortedIterator();

      // 如果没有记录，直接清空迭代器
      if (!underlying.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      // 否则包装为KVSorterIterator迭代器返回
      return new KVSorterIterator(underlying);
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }

  /**
   * Return the total number of bytes that has been spilled into disk so far.
   *
   * 返回溢写到磁盘的数据总大小。
   */
  public long getSpillSize() {
    return sorter.getSpillSize();
  }

  /**
   * Return the peak memory used so far, in bytes.
   *
   * 返回迄今为止使用的内存峰值。
   */
  public long getPeakMemoryUsedBytes() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   *
   * 标记当前的内存页没有空间剩余，当发生这种情况后，有两种处理方式：
   * - 申请一个新的内存页。
   * - 当遇到下一条记录时进行溢写。
   */
  @VisibleForTesting
  void closeCurrentPage() {
    sorter.closeCurrentPage();
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   *
   * 释放排序器的InMemorySorter数据结构，并且清理溢写的文件。
   */
  public void cleanupResources() {
    sorter.cleanupResources();
  }

  // KV记录比较器
  private static final class KVComparator extends RecordComparator {
    private final BaseOrdering ordering;
    private final UnsafeRow row1;
    private final UnsafeRow row2;
    private final int numKeyFields;

    KVComparator(BaseOrdering ordering, int numKeyFields) {
      this.numKeyFields = numKeyFields;
      this.row1 = new UnsafeRow(numKeyFields);
      this.row2 = new UnsafeRow(numKeyFields);
      this.ordering = ordering;
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      // Note that since ordering doesn't need the total length of the record, we just pass -1
      // into the row.
      // 比较时不需要记录的长度，直接传-1
      row1.pointTo(baseObj1, baseOff1 + 4, -1);
      row2.pointTo(baseObj2, baseOff2 + 4, -1);
      return ordering.compare(row1, row2);
    }
  }

  // 键值对有序的迭代器
  public class KVSorterIterator extends KVIterator<UnsafeRow, UnsafeRow> {
    private UnsafeRow key = new UnsafeRow(keySchema.size());
    private UnsafeRow value = new UnsafeRow(valueSchema.size());
    private final UnsafeSorterIterator underlying;

    private KVSorterIterator(UnsafeSorterIterator underlying) {
      this.underlying = underlying;
    }

    @Override
    public boolean next() throws IOException {
      try {
        if (underlying.hasNext()) {
          // 加载下一条数据，得出的数据是 记录头（4 bytes (keyLen) + 4 bytes (valueLen) + 4 bytes） + key data (keyLen) + value data (valueLen)
          underlying.loadNext();

          // 获取BaseObject、BaseOffset，记录长度
          Object baseObj = underlying.getBaseObject();
          long recordOffset = underlying.getBaseOffset();
          int recordLen = underlying.getRecordLength();

          // Note that recordLen = keyLen + valueLen + 4 bytes (for the keyLen itself)
          // 从baseObject中根据记录偏移量获取4子节点的Int值，即是键的长度
          int keyLen = Platform.getInt(baseObj, recordOffset);

          // 记录总长度 - 键的长度 - 4字节头信息，即是值的长度
          int valueLen = recordLen - keyLen - 4;

          // 根据键值偏移量和长度定位键和值
          key.pointTo(baseObj, recordOffset + 4, keyLen);
          value.pointTo(baseObj, recordOffset + 4 + keyLen, valueLen);

          return true;
        } else {
          key = null;
          value = null;
          cleanupResources();
          return false;
        }
      } catch (IOException e) {
        cleanupResources();
        throw e;
      }
    }

    @Override
    public UnsafeRow getKey() {
      return key;
    }

    @Override
    public UnsafeRow getValue() {
      return value;
    }

    @Override
    public void close() {
      cleanupResources();
    }
  }
}
