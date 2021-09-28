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

package org.apache.spark.util.collection.unsafe.sort;

import java.util.Comparator;
import java.util.LinkedList;

import org.apache.avro.reflect.Nullable;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 *
 *
 * 使用 AlphaSort 样式的键前缀排序对记录进行排序。
 * 这种排序将指向记录的指针与记录的排序键的前缀一起存储。
 * 底层排序算法比较记录时，会先比较存储的键前缀； 如果前缀不相等，那么我们不需要遍历记录指针来比较实际记录。
 * 避免这些随机内存访问可以提高缓存命中率。
 */
public final class UnsafeInMemorySorter {

  // 排序比较器
  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    // 值比较器，用于在前缀比较失效时使用
    private final RecordComparator recordComparator;
    // 值前缀比较器
    private final PrefixComparator prefixComparator;

    private final TaskMemoryManager memoryManager;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    /**
     * 该比较方法首先比较两条记录的前缀，如果前缀相同，才读取两条记录的行数据进行比较
     */
    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      // 先使用前缀比较器比较亮条记录
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();

      if (prefixComparisonResult == 0) {
        // 前缀比较相同，获取两条记录的真实数据
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
        // skip length
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + uaoSize;
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer);
        // skip length
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + uaoSize;

        // 使用行记录比较器比较整条记录的数据
        return recordComparator.compare(baseObject1, baseOffset1, baseObject2, baseOffset2);
      } else {
        return prefixComparisonResult;
      }
    }
  }

  // 内存消费者，用于申请内存
  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;

  // 排序比较器
  @Nullable
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * If non-null, specifies the radix sort parameters and that radix sort will be used.
   *
   * 如果非空，指定了基数排序的参数，则会使用基数排序。
   */
  @Nullable
  private final PrefixComparators.RadixSortSupport radixSortSupport;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   *
   * Only part of the array will be used to store the pointers, the rest part is preserved as
   * temporary buffer for sorting.
   *
   * 这个array和BytesToBytesMap中的longArray结构一样。用于存放键值对的LongArray。
   * 在数组中：
   * - 2 * i的位置用于跟踪指向i位置key的指针。
   * - 2 * i + 1的位置用于存放key的32比特位的哈希码。
   * 每个为都是一个Long型整数。
   *
   * 该数组只有一部分用于存放指针，剩余的部分主要保留用于排序时的临时缓冲区。
   */
  private LongArray array;

  /**
   * The position in the sort buffer where new records can be inserted.
   *
   * 在排序缓冲区中可以插入新记录的位置。
   */
  private int pos = 0;

  /**
   * If sorting with radix sort, specifies the starting position in the sort buffer where records
   * with non-null prefixes are kept. Positions [0..nullBoundaryPos) will contain null-prefixed
   * records, and positions [nullBoundaryPos..pos) non-null prefixed records. This lets us avoid
   * radix sorting over null values.
   *
   * 如果使用基数排序，需要指定在排序缓冲区中哪些区域是前缀非空的记录使用的。
   * [0..nullBoundaryPos]这段区域用于容纳前缀为空的记录。
   * [nullBoundaryPos...pos]这段区域用于容纳前缀非空的记录。
   *
   * 这个值用于避免在基数排序时对空值进行处理。
   */
  private int nullBoundaryPos = 0;

  /*
   * How many records could be inserted, because part of the array should be left for sorting.
   *
   * 由于array数组的一部分需要预留给排序操作，这个属性用于记录还有可以插入多少记录。
   */
  private int usableCapacity = 0;

  // 初始大小
  private long initialSize;

  // 总共的排序时间，纳秒
  private long totalSortTimeNanos = 0L;

  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
    int initialSize,
    boolean canUseRadixSort) {

    // 这里申请了初始容量的两倍内存大小
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2), canUseRadixSort);
  }

  public UnsafeInMemorySorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;

    // 初始容量和传入的初始大小并不一样，是传入初始大小参数的两倍
    this.initialSize = array.size();

    // 构造排序比较器
    if (recordComparator != null) {
      // 值比较器为空时，创建排序比较器
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);

      // 判断是否可以使用基数排序
      if (canUseRadixSort && prefixComparator instanceof PrefixComparators.RadixSortSupport) {
        this.radixSortSupport = (PrefixComparators.RadixSortSupport)prefixComparator;
      } else {
        this.radixSortSupport = null;
      }
    } else {
      this.sortComparator = null;
      this.radixSortSupport = null;
    }
    this.array = array;

    // 获取初始的可用容量
    this.usableCapacity = getUsableCapacity();
  }

  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    // 基数排序，可以使用一半，Tim Sort排序可以使用三分之二
    return (int) (array.size() / (radixSortSupport != null ? 2 : 1.5));
  }

  /**
   * Free the memory used by pointer array.
   *
   * 释放由指针数组使用的内存。
   */
  public void free() {
    if (consumer != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  /**
   * 重置操作，会释放指针数组使用的内存，并重新申请初始容量大小的内存，
   * 将相关属性置为初始值。
   */
  public void reset() {
    if (consumer != null) {
      consumer.freeArray(array);
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
    }
    pos = 0;
    nullBoundaryPos = 0;
  }

  /**
   * 已经被插入到当前排序器的记录数量。
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

  /**
   * 内存排序使用的总时间
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  // 使用的总内存大小，字节单位。
  public long getMemoryUsage() {
    return array.size() * 8;
  }

  // 判断存放地址指针的数组是否还有剩余空间
  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < usableCapacity;
  }

  // 扩展存放指针的数组，传入的是新数组
  public void expandPointerArray(LongArray newArray) {
    // 保证新数组大小不能小于当前数组大小
    if (newArray.size() < array.size()) {
      throw new OutOfMemoryError("Not enough memory to grow pointer array");
    }
    // 将当前数组中的数据拷贝到新数组中
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      pos * 8L);
    // 释放当前数组使用的内存
    consumer.freeArray(array);
    // 将array指向新数组，更新可用容量
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * 插入一条记录，用于排序，假设记录的指针指向存储为 4 字节整数的记录长度，后跟记录的数据的字节。
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   *                      指向存放在内存页中记录的指针，即PackedRecordPointer，由TaskMemoryManager编码。
   * @param keyPrefix a user-defined key prefix
   *                  用户自定义的键前缀。
   */
  public void insertRecord(long recordPointer, long keyPrefix, boolean prefixIsNull) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    if (prefixIsNull && radixSortSupport != null) {
      /**
       * 键为空，且使用基数排序，在这种情况下，需要将null记录存放在array数组最开头的位置。
       * nullBoundaryPos游标之前的数据都是null记录，从nullBoundaryPos开始都是非null记录。
       * 在存放时，使用“向后交换”的方式空出存放null记录的空间，
       * 即当存放新的null记录时，将nullBoundaryPos位置处存放的一条旧的非null记录往后面空闲的空间移动，即往pos位置移动，
       * 此时nullBoundaryPos位置就空出来了，可以将新的null记录存放在放在这个位置上，然后nullBoundaryPos自增维护正确的位置。
       */
      // Swap forward a non-null record to make room for this one at the beginning of the array.
      // 向前交换一个非空记录，以便在数组的开头为这个记录腾出空间。
      array.set(pos, array.get(nullBoundaryPos));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 1));
      pos++;
      // Place this record in the vacated position.
      // 将此记录放在空出的位置。
      array.set(nullBoundaryPos, recordPointer);
      nullBoundaryPos++;
      array.set(nullBoundaryPos, keyPrefix);
      nullBoundaryPos++;
    } else {
      // 填充非空记录的指针和前缀
      array.set(pos, recordPointer);
      pos++;
      array.set(pos, keyPrefix);
      pos++;
    }
  }

  /**
   * 排序迭代器，UnsafeInMemorySorter通过getSortedIterator()方法获取该迭代器
   */
  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;
    private long currentPageNumber;

    /**
     * @param numRecords 记录总数
     * @param offset 记录存放的开始偏移量
     */
    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      iter.currentPageNumber = currentPageNumber;
      return iter;
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    /**
     * 判断是否还要下一条记录
     */
    @Override
    public boolean hasNext() {
      // 每条记录因为占用了两个位置（地址指针 和 键前缀 各占一个位置），因此需要除以2
      return position / 2 < numRecords;
    }

    /**
     * 加载下一条记录
     */
    @Override
    public void loadNext() {
      // This pointer points to a 4-byte record length, followed by the record's bytes
      // 获取下一条记录的地址指针
      final long recordPointer = array.get(offset + position);
      // 解码得到内存页号
      currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);

      // UAO Size
      int uaoSize = UnsafeAlignedOffset.getUaoSize();

      // 从MemoryManager中获取内存页
      baseObject = memoryManager.getPage(recordPointer);

      // Skip over record length
      // 跳过UAO Size，这部分区域是用于存储的记录的总长度
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;

      // 读取记录长度
      recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);

      // 读取键前缀
      keyPrefix = array.get(offset + position + 1);

      // position自增2，即跳过当前记录的地址指针和键前缀区域，指向下一条记录地址指针位置。
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    public long getCurrentPageNumber() {
      return currentPageNumber;
    }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   *
   * 获取排好序的记录指针迭代器。
   * 为了性能效率，所有对next()方法的调用会返回同一个对象。
   */
  public UnsafeSorterIterator getSortedIterator() {
    int offset = 0;
    // 开始时间
    long start = System.nanoTime();

    // 排序比较器不为空，即进行排序，注意此处仅仅是对记录的指针进行排序移动，不会移动记录的数据。
    if (sortComparator != null) {
      if (this.radixSortSupport != null) {
        // 使用基数排序
        offset = RadixSort.sortKeyPrefixArray(
          array, nullBoundaryPos, (pos - nullBoundaryPos) / 2L, 0, 7,
          radixSortSupport.sortDescending(), radixSortSupport.sortSigned());
      } else {
        // 使用Tim Sort排序
        // 得到指针数据的内存页，创建为新的LongArray
        MemoryBlock unused = new MemoryBlock(
          array.getBaseObject(),
          array.getBaseOffset() + pos * 8L,
          (array.size() - pos) * 8L);
        LongArray buffer = new LongArray(unused);

        // 创建排序器进行排序，会先比较前缀，前缀相同再比较整条记录的值
        Sorter<RecordPointerAndKeyPrefix, LongArray> sorter =
          new Sorter<>(new UnsafeSortDataFormat(buffer));
        sorter.sort(array, 0, pos / 2, sortComparator);
      }
    }

    // 整体排序时间
    totalSortTimeNanos += System.nanoTime() - start;
    if (nullBoundaryPos > 0) { // 存在null记录
      assert radixSortSupport != null : "Nulls are only stored separately with radix sort";
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();

      // The null order is either LAST or FIRST, regardless of sorting direction (ASC|DESC)
      /**
       * 具体要看null是放在最前面还是最后面，会将null记录组成一个单独的迭代器，
       * 然后和正常值的迭代器组合为一个迭代器链。
       */
      if (radixSortSupport.nullsFirst()) {
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
      } else {
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
      }
      return new UnsafeExternalSorter.ChainedIterator(queue);
    } else {
      // 不用处理null记录，直接返回。
      return new SortedIterator(pos / 2, offset);
    }
  }
}
