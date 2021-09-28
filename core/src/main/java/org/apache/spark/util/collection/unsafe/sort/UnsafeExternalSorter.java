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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.Utils;

/**
 * External sorter based on {@link UnsafeInMemorySorter}.
 *
 * 基于UnsafeInMemorySorter的外部排序器。
 */
public final class UnsafeExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  // 前缀和记录比较器
  @Nullable
  private final PrefixComparator prefixComparator;
  @Nullable
  private final RecordComparator recordComparator;

  // MemoryManager及BlockManager
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private final TaskContext taskContext;
  private ShuffleWriteMetrics writeMetrics;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter
   * 当需要使用DiskBlockObjectWriter进行溢写的缓冲区大小
   **/
  private final int fileBufferSizeBytes;

  /**
   * Force this sorter to spill when there are this many elements in memory. The default value is
   * 1024 * 1024 * 1024 / 2 which allows the maximum size of the pointer array to be 8G.
   *
   * 当内存中的元素个数超过该属性值时强制溢写。
   * 默认值是 1024 * 1024 * 1024 / 2，存放地址指针的数组最大允许大小为8G
   */
  public static final long DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD = 1024 * 1024 * 1024 / 2;

  // 当存储的元素超过该阈值是需要溢写。
  private final long numElementsForSpillThreshold;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   *
   * 用于存放记录的内存页。
   * 当溢写时这个列表汇总的内存页会被释放，虽然原则上我们可以在溢出时回收重利用这些页面
   * （如果我们可以在 TaskMemoryManager 本身中维护一个可重用页面池，那么在UnsafeExternalSorter中重利用回收页面是没有必要的）。
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  // 溢写后的文件写出器
  private final LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  // These variables are reset after spilling:
  // 这些变量在溢写后会被重置。
  @Nullable private volatile UnsafeInMemorySorter inMemSorter;

  private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long peakMemoryUsedBytes = 0;
  private long totalSpillBytes = 0L;
  private long totalSortTimeNanos = 0L;
  private volatile SpillableIterator readingIterator = null;

  // 静态方法，用于根据已有的UnsafeInMemorySorter创建UnsafeExternalSorter
  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      long numElementsForSpillThreshold,
      // 这里传入了UnsafeInMemorySorter
      UnsafeInMemorySorter inMemorySorter) throws IOException {
    UnsafeExternalSorter sorter = new UnsafeExternalSorter(taskMemoryManager, blockManager,
      serializerManager, taskContext, recordComparator, prefixComparator, initialSize,
        numElementsForSpillThreshold, pageSizeBytes, inMemorySorter, false /* ignored */);
    // 首先触发一次溢写
    sorter.spill(Long.MAX_VALUE, sorter);
    // The external sorter will be used to insert records, in-memory sorter is not needed.
    // 该External Sorter用于插入记录，因此InMemory Sorter是不需要的
    sorter.inMemSorter = null;
    return sorter;
  }

  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      long numElementsForSpillThreshold,
      boolean canUseRadixSort) {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager, serializerManager,
      taskContext, recordComparator, prefixComparator, initialSize, pageSizeBytes,
      numElementsForSpillThreshold, null, canUseRadixSort);
  }

  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      long numElementsForSpillThreshold,
      @Nullable UnsafeInMemorySorter existingInMemorySorter,
      boolean canUseRadixSort) {
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.taskContext = taskContext;
    this.recordComparator = recordComparator;
    this.prefixComparator = prefixComparator;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    // this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024
    // 溢写缓冲区大小，32k
    this.fileBufferSizeBytes = 32 * 1024;
    // The spill metrics are stored in a new ShuffleWriteMetrics,
    // and then discarded (this fixes SPARK-16827).
    // TODO: Instead, separate spill metrics should be stored and reported (tracked in SPARK-3577).
    this.writeMetrics = new ShuffleWriteMetrics();

    // 处理inMemorySorter，外界没有传入就自行创建。
    if (existingInMemorySorter == null) {
      this.inMemSorter = new UnsafeInMemorySorter(
        this, taskMemoryManager, recordComparator, prefixComparator, initialSize, canUseRadixSort);
    } else {
      this.inMemSorter = existingInMemorySorter;
    }

    // 初始化内存使用峰值
    this.peakMemoryUsedBytes = getMemoryUsage();

    // 溢写阈值
    this.numElementsForSpillThreshold = numElementsForSpillThreshold;

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the sorter's output (e.g. sort followed by limit).
    // 添加Task完成监听器，Task完成后，会清理资源
    taskContext.addTaskCompletionListener(
      new TaskCompletionListener() {
        @Override
        public void onTaskCompletion(TaskContext context) {
          cleanupResources();
        }
      }
    );
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   *
   * 标记当前内存页已经没有剩余空间可用，
   * 可以申请一个新的内存页，或进行溢写以解决该问题。
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    if (currentPage != null) {
      // 直接将游标置为当前页的最末尾处
      pageCursor = currentPage.getBaseOffset() + currentPage.size();
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   *
   * 排序并溢写当前已由的记录，以应付内存不足。
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this) {
      // 如果是自己触发的溢写，使用readingIterator进行溢写
      if (readingIterator != null) {
        return readingIterator.spill();
      }
      return 0L; // this should throw exception
    }

    // UnsafeInMemorySorter为空，或者其保存的记录数为0，则不用溢写直接返回。
    if (inMemSorter == null || inMemSorter.numRecords() <= 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillWriters.size(),
      spillWriters.size() > 1 ? " times" : " time");

    // We only write out contents of the inMemSorter if it is not empty.
    // 只会在UnsafeInMemorySorter内部存储的记录数不为空时才溢写内容到磁盘
    if (inMemSorter.numRecords() > 0) {
      // 创建溢写写出器
      final UnsafeSorterSpillWriter spillWriter =
        new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
          inMemSorter.numRecords());
      // 将溢写写出器存放到spillWriters列表
      spillWriters.add(spillWriter);

      // 通过UnsafeInMemorySorter取得已经排好序的迭代器
      final UnsafeSorterIterator sortedRecords = inMemSorter.getSortedIterator();

      // 不断迭代排好序的迭代器，将UnsafeInMemorySorter中所有的记录通过UnsafeSorterSpillWriter溢写到磁盘
      while (sortedRecords.hasNext()) {
        sortedRecords.loadNext();
        final Object baseObject = sortedRecords.getBaseObject();
        final long baseOffset = sortedRecords.getBaseOffset();
        final int recordLength = sortedRecords.getRecordLength();
        spillWriter.write(baseObject, baseOffset, recordLength, sortedRecords.getKeyPrefix());
      }

      // 关闭写出器
      spillWriter.close();
    }

    // 释放内存
    final long spillSize = freeMemory();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    /**
     * 请注意，这或多或少是页面大小的倍数，因此页面中浪费的空间当前将被视为内存溢出，
     * 即使该空间实际上并未写入磁盘。 这也计算了存储排序器指针数组所需的空间。
     */

    // 因为UnsafeInMemorySorter中所有的记录都溢写到磁盘了，重置它
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    /**
     * 只有在释放保存记录的内存页后，才重置内存中排序器的指针数组。
     * 否则，如果任务过度分配内存，那么如果不释放内存页，我们可能无法为指针数组获取内存。
     */

    // 更新度量值，记录溢写数据大小并返回
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    totalSpillBytes += spillSize;
    return spillSize;
  }

  /**
   * Return the total memory usage of this sorter, including the data pages and the sorter's pointer
   * array.
   *
   * 返回当前Sorter使用的总内存，包括数据页内存和地址指针数组的内存。
   */
  private long getMemoryUsage() {
    long totalPageSize = 0;

    // 存放数据逇内存页大小
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }

    // 地址指针数组的内存页大小
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  // 更新使用到的内存峰值
  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   *
   * 获取峰值内存大小
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * 返回内存内排序所花费的时间。
   *
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    UnsafeInMemorySorter sorter = inMemSorter;
    if (sorter != null) {
      return sorter.getSortTimeNanos();
    }
    return totalSortTimeNanos;
  }

  /**
   * Return the total number of bytes that has been spilled into disk so far.
   *
   * 返回迄今为止溢写到磁盘的数据的大小
   */
  public long getSpillSize() {
    return totalSpillBytes;
  }

  // 返回已经申请的内存页数量
  @VisibleForTesting
  public int getNumberOfAllocatedPages() {
    return allocatedPages.size();
  }

  /**
   * Free this sorter's data pages.
   *
   * 释放数据内存页。
   *
   * @return the number of bytes freed.
   */
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;

    // 遍历所有数据内存页进行释放
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Deletes any spill files created by this sorter.
   *
   * 删除所有溢写的文件
   */
  private void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   * 清除当前Sorter占用的所有内存页及溢写文件。
   */
  public void cleanupResources() {
    synchronized (this) {
      deleteSpillFiles();
      freeMemory();
      if (inMemSorter != null) {
        inMemSorter.free();
        inMemSorter = null;
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   *
   * 检查是否有足够的空间在地址指针数组中插入额外的记录，如果需要额外的空间，则增加数组。
   * 如果无法获得所需的空间，则内存中的数据将溢出到磁盘。
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      // UnsafeInMemorySorter已经没有剩余空间存放地址指针了

      // 获取UnsafeInMemorySorter已经使用的内存大小
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        // 申请两倍大小的新空间，该操作可能会导致溢写
        array = allocateArray(used / 8 * 2);
      } catch (OutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      // 再次检查UnsafeInMemorySorter是否有剩余空间
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        // 如果有就释放掉刚刚申请的空间
        freeArray(array);
      } else {
        // 否则给UnsafeInMemorySorter的地址指针数组扩容
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * 申请更多的内存以用于插入新的记录。
   * 会从MemoryManager中申请额外的内存，如果申请不到则会触发溢写。
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
      // TODO: try to find space on previous pages
      // 申请新的内存页
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the sorter.
   *
   * 写入一条记录到Sorter中
   */
  public void insertRecord(
      Object recordBase, long recordOffset, int length, long prefix, boolean prefixIsNull)
    throws IOException {

    assert(inMemSorter != null);

    // 如果UnsafeInMemorySorter中存的的记录已经超过溢写阈值就触发溢写
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      spill();
    }

    // 检查是否需要对UnsafeInMemorySorter中存放地址指针的数组进行扩容
    growPointerArrayIfNecessary();

    int uaoSize = UnsafeAlignedOffset.getUaoSize();

    // Need 4 bytes to store the record length.
    // 需要UAO Size大小的空间存放记录的总长度。
    final int required = length + uaoSize;

    // 检查是否需要申请新的内存页。
    acquireNewPageIfNecessary(required);

    // 获取当前内存页用于存放数据的对象
    final Object base = currentPage.getBaseObject();

    // 编码PackedRecordPointer地址指针。
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);

    // 写入记录总长度
    UnsafeAlignedOffset.putSize(base, pageCursor, length);

    // 后移游标
    pageCursor += uaoSize;

    // 写入记录的数据
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);

    // 后移游标
    pageCursor += length;

    // 将值前缀和记录的指针地址写入到UnsafeInMemorySorter
    inMemSorter.insertRecord(recordAddress, prefix, prefixIsNull);
  }

  /**
   * Write a key-value record to the sorter. The key and value will be put together in-memory,
   * using the following format:
   *
   * record length (4 bytes), key length (4 bytes), key data, value data
   *
   * record length = key length + value length + 4
   *
   * 插入键值对记录到Sorter中。
   * 键和值会被一起存放到Sorter中，使用下面的格式：
   *
   * 1. [记录总长度 (UAO Size)][键长度 (UAO Size)][键数据][值数据]
   * 2. 记录总长度 = 键长度 + 值长度 + UAO Size
   */
  public void insertKVRecord(Object keyBase, long keyOffset, int keyLen,
      Object valueBase, long valueOffset, int valueLen, long prefix, boolean prefixIsNull)
    throws IOException {

    // 检查是否要对地址指针数组扩容
    growPointerArrayIfNecessary();

    int uaoSize = UnsafeAlignedOffset.getUaoSize();

    // 写入记录需要的总字节数
    final int required = keyLen + valueLen + (2 * uaoSize);

    // 检查是否需要申请新的内存页
    acquireNewPageIfNecessary(required);

    // 获取当前内存页用于存放数据的对象
    final Object base = currentPage.getBaseObject();

    // 编码PackedRecordPointer地址指针
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);

    // 写入记录总长度
    UnsafeAlignedOffset.putSize(base, pageCursor, keyLen + valueLen + uaoSize);
    pageCursor += uaoSize;

    // 写入键长度
    UnsafeAlignedOffset.putSize(base, pageCursor, keyLen);
    pageCursor += uaoSize;

    // 写入键数据
    Platform.copyMemory(keyBase, keyOffset, base, pageCursor, keyLen);
    pageCursor += keyLen;

    // 写入值数据
    Platform.copyMemory(valueBase, valueOffset, base, pageCursor, valueLen);
    pageCursor += valueLen;

    // 将地址指针和键前缀插入到UnsafeInMemorySorter中
    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix, prefixIsNull);
  }

  /**
   * Merges another UnsafeExternalSorters into this one, the other one will be emptied.
   *
   * 合并另一个UnsafeExternalSorter到本Sorter中，被合并的UnsafeExternalSorter会被清空
   *
   * @throws IOException
   */
  public void merge(UnsafeExternalSorter other) throws IOException {
    // 另一个Sorter进行完全溢写
    other.spill();

    // 将另一个Sorter溢写后的Writer交给当前Sorter管理
    spillWriters.addAll(other.spillWriters);
    // remove them from `spillWriters`, or the files will be deleted in `cleanupResources`.

    // 清理另一个Sorter的Writer列表和资源都清理
    other.spillWriters.clear();
    other.cleanupResources();
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   *
   * 返回一个排序的迭代器。 调用者有责任在使用此迭代器后调用 `cleanupResources()`。
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    assert(recordComparator != null);
    if (spillWriters.isEmpty()) {
      assert(inMemSorter != null);
      readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
      return readingIterator;
    } else {
      // 依次添加溢写文件的迭代器和UnsafeInMemorySorter返回的有序迭代器，合并为UnsafeSorterSpillMerger
      final UnsafeSorterSpillMerger spillMerger =
        new UnsafeSorterSpillMerger(recordComparator, prefixComparator, spillWriters.size());
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        spillMerger.addSpillIfNotEmpty(spillWriter.getReader(serializerManager));
      }
      if (inMemSorter != null) {
        readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
        spillMerger.addSpillIfNotEmpty(readingIterator);
      }

      // 使用合并得到的UnsafeSorterSpillMerger返回有序迭代器。
      return spillMerger.getSortedIterator();
    }
  }

  /**
   * An UnsafeSorterIterator that support spilling.
   *
   * 支持溢写的迭代器
   */
  class SpillableIterator extends UnsafeSorterIterator {
    private UnsafeSorterIterator upstream;
    private UnsafeSorterIterator nextUpstream = null;
    private MemoryBlock lastPage = null;

    // 标识内存页面是否被加载过了
    private boolean loaded = false;
    private int numRecords = 0;

    SpillableIterator(UnsafeSorterIterator inMemIterator) {
      this.upstream = inMemIterator;
      this.numRecords = inMemIterator.getNumRecords();
    }

    // 获取记录总条数
    public int getNumRecords() {
      return numRecords;
    }

    // 溢写操作，该溢写操作只会发生一次
    public long spill() throws IOException {
      synchronized (this) {
        /**
         * 满足下面三个条件才可以溢写：
         * 1. upstream需要是UnsafeInMemorySorter产生的SortedIterator迭代器。
         * 2. nextUpstream需要为空。
         * 3. 总记录数大于0。
         */
        if (!(upstream instanceof UnsafeInMemorySorter.SortedIterator && nextUpstream == null
          && numRecords > 0)) {
          return 0L;
        }

        // 将upstream进行转换
        UnsafeInMemorySorter.SortedIterator inMemIterator =
          ((UnsafeInMemorySorter.SortedIterator) upstream).clone();

        // Iterate over the records that have not been returned and spill them.
        // 创建溢写写出器
        final UnsafeSorterSpillWriter spillWriter =
          new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics, numRecords);

        // 不断迭代SortedIterator中的记录
        while (inMemIterator.hasNext()) {
          inMemIterator.loadNext();
          final Object baseObject = inMemIterator.getBaseObject();
          final long baseOffset = inMemIterator.getBaseOffset();
          final int recordLength = inMemIterator.getRecordLength();

          // 使用写出器溢写
          spillWriter.write(baseObject, baseOffset, recordLength, inMemIterator.getKeyPrefix());
        }
        spillWriter.close();

        // 将完成溢写的溢写写出器存放到spillWriters中
        spillWriters.add(spillWriter);

        // 使用nextUpstream记录刚刚溢写产生的写出器
        nextUpstream = spillWriter.getReader(serializerManager);

        long released = 0L;
        synchronized (UnsafeExternalSorter.this) {
          // release the pages except the one that is used. There can still be a caller that
          // is accessing the current record. We free this page in that caller's next loadNext()
          // call.
          /**
           * 释放除使用的页面之外的页面。 仍然可能有呼叫者正在访问当前记录。
           * 我们在调用者的下一个 loadNext() 调用中释放这个页面。
           */
          for (MemoryBlock page : allocatedPages) {
            if (!loaded || page.pageNumber !=
                    ((UnsafeInMemorySorter.SortedIterator)upstream).getCurrentPageNumber()) {
              released += page.size();
              freePage(page);
            } else {
              // 记录溢写后最后一个未释放的页面
              lastPage = page;
            }
          }
          allocatedPages.clear();
        }

        // in-memory sorter will not be used after spilling
        // UnsafeInMemorySorter在溢写后就不会被使用了
        assert(inMemSorter != null);
        // 记录相关的指标值
        released += inMemSorter.getMemoryUsage();
        totalSortTimeNanos += inMemSorter.getSortTimeNanos();

        // 释放UnsafeInMemorySorter使用的内存
        inMemSorter.free();
        inMemSorter = null;
        taskContext.taskMetrics().incMemoryBytesSpilled(released);
        totalSpillBytes += released;
        return released;
      }
    }

    @Override
    public boolean hasNext() {
      return numRecords > 0;
    }

    @Override
    public void loadNext() throws IOException {
      synchronized (this) {
        loaded = true;

        // 如果nextUpstream不为空说明发生过溢写
        if (nextUpstream != null) {
          // Just consumed the last record from in memory iterator
          // 释放溢写后最后一个未释放的页面
          if (lastPage != null) {
            freePage(lastPage);
            lastPage = null;
          }
          // 接下来的迭代会写出溢写文件上的记录
          upstream = nextUpstream;
          nextUpstream = null;
        }
        numRecords--;
        upstream.loadNext();
      }
    }

    @Override
    public Object getBaseObject() {
      return upstream.getBaseObject();
    }

    @Override
    public long getBaseOffset() {
      return upstream.getBaseOffset();
    }

    @Override
    public int getRecordLength() {
      return upstream.getRecordLength();
    }

    @Override
    public long getKeyPrefix() {
      return upstream.getKeyPrefix();
    }
  }

  /**
   * Returns a iterator, which will return the rows in the order as inserted.
   *
   * It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   *
   * 返回一个迭代器，它将按照插入的顺序返回行。
   * 调用者有责任在使用此迭代器后调用 `cleanupResources()`。
   *
   * 该方法但返回的迭代器如果么有发生溢写，其实迭代的数据依旧是排好序的。
   * 如果发生溢写，溢写的数据会先被迭代，然后迭代UnsafeInMemorySorter中的有序数据。
   *
   * TODO: support forced spilling
   */
  public UnsafeSorterIterator getIterator() throws IOException {
    if (spillWriters.isEmpty()) {
      assert(inMemSorter != null);
      // 返回UnsafeInMemorySorter提供的有序迭代器
      return inMemSorter.getSortedIterator();
    } else {
      // 存在溢写文件，将溢写文件和UnsafeInMemorySorter提供的有序迭代器合并为ChainedIterator迭代器
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        queue.add(spillWriter.getReader(serializerManager));
      }
      if (inMemSorter != null) {
        queue.add(inMemSorter.getSortedIterator());
      }
      return new ChainedIterator(queue);
    }
  }

  /**
   * Chain multiple UnsafeSorterIterator together as single one.
   *
   * 用于整合多个迭代器，内部用队列管理多个迭代器。
   * 在使用该迭代器时，内部会从队列中依次取出迭代器迭代内部的数据，直到队列为空。
   */
  static class ChainedIterator extends UnsafeSorterIterator {

    private final Queue<UnsafeSorterIterator> iterators;
    private UnsafeSorterIterator current;
    private int numRecords;

    ChainedIterator(Queue<UnsafeSorterIterator> iterators) {
      assert iterators.size() > 0;
      this.numRecords = 0;
      for (UnsafeSorterIterator iter: iterators) {
        this.numRecords += iter.getNumRecords();
      }
      this.iterators = iterators;
      this.current = iterators.remove();
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      while (!current.hasNext() && !iterators.isEmpty()) {
        current = iterators.remove();
      }
      return current.hasNext();
    }

    @Override
    public void loadNext() throws IOException {
      while (!current.hasNext() && !iterators.isEmpty()) {
        current = iterators.remove();
      }
      current.loadNext();
    }

    @Override
    public Object getBaseObject() { return current.getBaseObject(); }

    @Override
    public long getBaseOffset() { return current.getBaseOffset(); }

    @Override
    public int getRecordLength() { return current.getRecordLength(); }

    @Override
    public long getKeyPrefix() { return current.getKeyPrefix(); }
  }
}
