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

package org.apache.spark.unsafe.map;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillWriter;

/**
 * An append-only hash map where keys and values are contiguous regions of bytes.
 *
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 *
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 *
 * The key and values under the hood are stored together, in the following format:
 *   Bytes 0 to 4: len(k) (key length in bytes) + len(v) (value length in bytes) + 4
 *   Bytes 4 to 8: len(k)
 *   Bytes 8 to 8 + len(k): key data
 *   Bytes 8 + len(k) to 8 + len(k) + len(v): value data
 *   Bytes 8 + len(k) + len(v) to 8 + len(k) + len(v) + 8: pointer to next pair
 *
 * This means that the first four bytes store the entire record (key + value) length. This format
 * is compatible with {@link org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter},
 * so we can pass records from this map directly into the sorter to sort records in place.
 *
 * 只可进行追加操作的HashMap，key和value都是连续的字节区域。
 *
 * 由大小为2的次方的哈希表实现，使用三角值二次探测解决哈希冲突，能够保证耗尽空间。
 *
 * 这个Map可以支持2 ^ 29个键。如果键的数量大于该值，你应该考虑使用Sorting代替Hashing，以便取得更好的缓存本地性。
 *
 * Map中的键和值是存放在一起的，使用下面的格式：
 * - 0 ~ 4 Bytes：键的字节长度 + 值的字节长度 + 4，注意，这里4指的即是0 ~ 4这4个字节。
 * - 4 ~ 8 Bytes：键的长度。
 * - 8 ~ 8 + len(k)：键的数据。
 * - 8 + len(k) ~ 8 + len(k) + len(v)：值的数据。
 * - 8 + len(k) + len(v) ~ 8 + len(k) + len(v) + 8：指向下一个键值对的指针。
 *
 * 这意味着前面的4个字节用于存放整个记录（键 + 值）的长度。这种格式是对UnsafeExternalSorter兼容的，
 * 因此我们可以直接将当前Map中的记录传递给UnsafeExternalSorter用于对记录进行原地排序。
 *
 * BytesToBytesMap本质是一个MemoryConsumer
 */
public final class BytesToBytesMap extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(BytesToBytesMap.class);

  // HashMap扩容策略，默认是两倍扩容
  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  private final TaskMemoryManager taskMemoryManager;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   *
   * 用于跟踪所有已经分配的内存页，以便我们可以释放所有内存。
   */
  private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();

  /**
   * The data page that will be used to store keys and values for new hashtable entries. When this
   * page becomes full, a new page will be allocated and this pointer will change to point to that
   * new page.
   *
   * 当前用于存放键值对的内存页。
   * 一旦当前内存页用完，会申请一个新的内存页，并将该变量指向新申请的内存页。
   */
  private MemoryBlock currentPage = null;

  /**
   * Offset into `currentPage` that points to the location where new data can be inserted into
   * the page. This does not incorporate the page's base offset.
   *
   * 指向当前内存页的偏移量指针，该偏移量的位置可用于插入新的数据。
   * 需要注意的是，该偏移量不包括baseOffset。
   */
  private long pageCursor = 0;

  /**
   * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
   * power-of-2-sized and its backing Java array can contain at most (1 &lt;&lt; 30) elements,
   * since that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
   * entries per key, giving us a maximum capacity of (1 &lt;&lt; 29).
   *
   * BytesToBytesMap支持存放的键的最大数量。
   * 哈希表是2的次方大小，并且用于支撑它的Java数组可以容纳最多1 << 30个元组，这是小于Integer.MAX_VALUE的最大2的次方值。
   * 我们需要两个Long数组存放每个Key，因此最大容量是1 << 29。
   */
  @VisibleForTesting
  static final int MAX_CAPACITY = (1 << 29);

  // This choice of page table size and page size means that we can address up to 500 gigabytes
  // of memory.
  // 页表大小和页大小的这种选择意味着我们可以寻址多达 500 GB 的内存。

  /**
   * A single array to store the key and value.
   *
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
   *
   * 用于存放键值对的LongArray。
   * 在数组中：
   * - 2 * i的位置用于跟踪指向i位置key的指针。
   * - 2 * i + 1的位置用于存放key的32比特位的哈希码。
   * 每个为都是一个Long型整数。
   */
  @Nullable private LongArray longArray;
  // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
  // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
  // only one long per map entry, increasing the chance that this array will fit in cache at the
  // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
  // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
  // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
  // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
  // full base addresses in the page table for off-heap mode so that we can reconstruct the full
  // absolute memory addresses.
  /**
   * TODO：我们在这里浪费了 32 位空间； 我们可能可以存储更少的哈希码位并利用字对齐来使用更少的位来保存地址。
   * 这可能让我们在每个键值对条目中只存储一个 long，增加了这个数组匹配缓存的机会，代价是如果我们有哈希冲突，可能会执行更多的查找。
   * 假设我们只存储了 27 位哈希码和 37 位地址。 给定字对齐，37 位足以寻址 1 TB 的 RAM。
   * 如果我们将 13 位用于我们的页表，那么我们的最大页面大小为 2^24 * 8 = 每页大约 134 兆字节。
   * 此更改将要求我们将完整的基地址存储在堆外模式的页表中，以便我们可以重建完整的绝对内存地址。
   */

  /**
   * Whether or not the longArray can grow. We will not insert more elements if it's false.
   * 是否可以对LongArray进行扩容，如果为false，在LongArray满了之后就不再插入元素
   */
  private boolean canGrowArray = true;

  private final double loadFactor;

  /**
   * The size of the data pages that hold key and value data. Map entries cannot span multiple
   * pages, so this limits the maximum entry size.
   *
   * 用于存放键值对数据的内存页大小。
   * Map键值对不能跨越多个内存页，所以这个值限制了最大的键值对大小。
   */
  private final long pageSizeBytes;

  /**
   * Number of keys defined in the map.
   *
   * Map中键的数量。
   */
  private int numKeys;

  /**
   * Number of values defined in the map. A key could have multiple values.
   *
   * Map中值的数量。一个键可以有多个值。
   */
  private int numValues;

  /**
   * The map will be expanded once the number of keys exceeds this threshold.
   *
   * 当键的数量超过该阈值，Map会被扩容。
   */
  private int growthThreshold;

  /**
   * Mask for truncating hashcodes so that they do not exceed the long array's size.
   * This is a strength reduction optimization; we're essentially performing a modulus operation,
   * but doing so with a bitmask because this is a power-of-2-sized hash map.
   *
   * 用于截断哈希码的掩码，以保证它们不会超过LongArray的大小。
   * 这是一种长度削减的优化；我们本质上是在执行模数运算，但是使用位掩码这样做，因为这是一个 2 次幂大小的哈希映射。
   */
  private int mask;

  /**
   * Return value of {@link BytesToBytesMap#lookup(Object, long, int)}.
   *
   * 返回lookup方法获得的值
   */
  private final Location loc;

  private final boolean enablePerfMetrics;

  private long timeSpentResizingNs = 0;

  private long numProbes = 0;

  private long numKeyLookups = 0;

  private long numHashCollisions = 0;

  private long peakMemoryUsedBytes = 0L;

  private final int initialCapacity;

  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private volatile MapIterator destructiveIterator = null;
  private LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      int initialCapacity, // 初始化大小
      double loadFactor, // 负载因子
      long pageSizeBytes, // 页大小
      boolean enablePerfMetrics) { // 是否打开性能度量
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    this.pageSizeBytes = pageSizeBytes;
    this.enablePerfMetrics = enablePerfMetrics;
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be greater than 0");
    }
    if (initialCapacity > MAX_CAPACITY) { // 1 << 29
      throw new IllegalArgumentException(
        "Initial capacity " + initialCapacity + " exceeds maximum capacity of " + MAX_CAPACITY);
    }
    if (pageSizeBytes > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) { // （2^32-1）× 8
      throw new IllegalArgumentException("Page size " + pageSizeBytes + " cannot exceed " +
        TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES);
    }
    this.initialCapacity = initialCapacity;

    // 初始化时就申请了初始容量大小的内存
    allocate(initialCapacity);
  }

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      int initialCapacity,
      long pageSizeBytes) {
    this(taskMemoryManager, initialCapacity, pageSizeBytes, false);
  }

  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this(
      taskMemoryManager,
      SparkEnv.get() != null ? SparkEnv.get().blockManager() :  null,
      SparkEnv.get() != null ? SparkEnv.get().serializerManager() :  null,
      initialCapacity,
      // In order to re-use the longArray for sorting, the load factor cannot be larger than 0.5.
      // 用了使用LongArray进行排序，负载因子不能大于0.5；Radix Sorting需要一半的空间。
      0.5,
      pageSizeBytes,
      enablePerfMetrics);
  }

  /**
   * Returns the number of keys defined in the map.
   */
  public int numKeys() { return numKeys; }

  /**
   * Returns the number of values defined in the map. A key could have multiple values.
   */
  public int numValues() { return numValues; }

  // Map迭代器，迭代得到的是Location对象。
  public final class MapIterator implements Iterator<Location> {

    private int numRecords;
    private final Location loc;

    private MemoryBlock currentPage = null;
    private int recordsInPage = 0;
    private Object pageBaseObject;
    private long offsetInPage;

    // If this iterator destructive or not. When it is true, it frees each page as it moves onto
    // next one.
    private boolean destructive = false;
    private UnsafeSorterSpillReader reader = null;

    /**
     * 构造Map迭代器
     * @param numRecords 记录数量
     * @param loc 用于记录值地址的Location
     * @param destructive 是否可破坏底层数据，该值为true时，每遍历完一个内存页就将该内存页释放掉。
     */
    private MapIterator(int numRecords, Location loc, boolean destructive) {
      this.numRecords = numRecords;
      this.loc = loc;
      this.destructive = destructive;
      if (destructive) {
        destructiveIterator = this;
      }
    }

    // 推进到下一个内存页
    private void advanceToNextPage() {
      synchronized (this) {
        // 下一页的ID索引
        int nextIdx = dataPages.indexOf(currentPage) + 1;
        if (destructive && currentPage != null) {
          // 如果设置destructive，因为当前页遍历完了，就将当前页释放掉，并从dataPages链表移除
          dataPages.remove(currentPage);
          freePage(currentPage);
          // 注意，计算的下一页的索引需要减1
          nextIdx --;
        }
        if (dataPages.size() > nextIdx) {
          // 还存在剩余的页，根据前面保存的下一页的ID获取页，并赋值给当前页currentPage属性记录。
          currentPage = dataPages.get(nextIdx);
          pageBaseObject = currentPage.getBaseObject();
          offsetInPage = currentPage.getBaseOffset();
          // 获取记录数量
          recordsInPage = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
          // 获取记录真正开始的偏移量
          offsetInPage += UnsafeAlignedOffset.getUaoSize();
        } else {
          // 内存中没有更多的页了，将currentPage置为null
          currentPage = null;
          // 判断是否存在溢写到磁盘的文件，如果存在就删除
          if (reader != null) {
            // remove the spill file from disk
            File file = spillWriters.removeFirst().getFile();
            if (file != null && file.exists()) {
              if (!file.delete()) {
                logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
              }
            }
          }
          try {
            Closeables.close(reader, /* swallowIOException = */ false);
            reader = spillWriters.getFirst().getReader(serializerManager);
            recordsInPage = -1;
          } catch (IOException e) {
            // Scala iterator does not handle exception
            Platform.throwException(e);
          }
        }
      }
    }

    // 判断是否还有值
    @Override
    public boolean hasNext() {
      if (numRecords == 0) {
        // 如果记录数为0，移除溢写到磁盘的文件
        if (reader != null) {
          // remove the spill file from disk
          File file = spillWriters.removeFirst().getFile();
          if (file != null && file.exists()) {
            if (!file.delete()) {
              logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
            }
          }
        }
      }
      return numRecords > 0;
    }

    // 获取下一个值
    @Override
    public Location next() {
      if (recordsInPage == 0) { // 当前页的剩余记录数为0，推进到下一页
        advanceToNextPage();
      }
      numRecords--;
      if (currentPage != null) { // 还有剩余的页且该也中有数据
        int totalLength = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
        loc.with(currentPage, offsetInPage);
        // [total size] [key size] [key] [value] [pointer to next]
        /**
         * 移动偏移量到下一条记录的起始位置，
         * [total size]指的是[key size][key][value] 这三个部分的长度，
         * 因此下面的加法中，UnsafeAlignedOffset.getUaoSize()表示[total size]的长度，
         * 8表示最后[pointer to next]的长度。
         */
        offsetInPage += UnsafeAlignedOffset.getUaoSize() + totalLength + 8;
        // 当前页剩余记录数 -1
        recordsInPage --;
        return loc;
      } else { // 内存里没有剩余的页了，迭代溢出到磁盘的数据
        assert(reader != null);
        if (!reader.hasNext()) {
          advanceToNextPage();
        }
        try {
          // 使用磁盘Reader载入数据
          reader.loadNext();
        } catch (IOException e) {
          try {
            reader.close();
          } catch(IOException e2) {
            logger.error("Error while closing spill reader", e2);
          }
          // Scala iterator does not handle exception
          Platform.throwException(e);
        }
        // 锚定数据到Reader下一条记录的位置上。
        loc.with(reader.getBaseObject(), reader.getBaseOffset(), reader.getRecordLength());
        return loc;
      }
    }

    public long spill(long numBytes) throws IOException {
      synchronized (this) {
        // 如果迭代过程中不可破坏数据，或者申请的内存只有一页，就不进行溢写。
        if (!destructive || dataPages.size() == 1) {
          return 0L;
        }

        // TODO: use existing ShuffleWriteMetrics
        ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();

        long released = 0L;
        while (dataPages.size() > 0) {
          // 从后往前进行溢写
          MemoryBlock block = dataPages.getLast();
          // The currentPage is used, cannot be released
          if (block == currentPage) {
            break;
          }

          // 写出一个块到磁盘
          Object base = block.getBaseObject();
          long offset = block.getBaseOffset();
          int numRecords = UnsafeAlignedOffset.getSize(base, offset);
          int uaoSize = UnsafeAlignedOffset.getUaoSize();
          offset += uaoSize;

          // 构造溢写的文件写出器
          final UnsafeSorterSpillWriter writer =
            new UnsafeSorterSpillWriter(blockManager, 32 * 1024, writeMetrics, numRecords);

          // 不断迭代块中的数据，写出到磁盘
          while (numRecords > 0) {
            int length = UnsafeAlignedOffset.getSize(base, offset);
            writer.write(base, offset + uaoSize, length, 0);
            offset += uaoSize + length + 8;
            numRecords--;
          }

          // 关闭写出器
          writer.close();
          // 将写出器记录到spillWriters中
          spillWriters.add(writer);

          // 移除最后一个Block
          dataPages.removeLast();

          // 累计释放的内存大小并进行释放
          released += block.size();
          freePage(block);

          // 如果溢写的数据大小满足了要求释放的大小，就跳出循环
          if (released >= numBytes) {
            break;
          }
        }

        return released;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns an iterator for iterating over the entries of this map.
   *
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public MapIterator iterator() {
    return new MapIterator(numValues, loc, false);
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map. It frees each page
   * as it moves onto next one. Notice: it is illegal to call any method on the map after
   * `destructiveIterator()` has been called.
   *
   * For efficiency, all calls to `next()` will return the same {@link Location} object.
   *
   * If any other lookups or operations are performed on this map while iterating over it, including
   * `lookup()`, the behavior of the returned iterator is undefined.
   */
  public MapIterator destructiveIterator() {
    return new MapIterator(numValues, loc, true);
  }

  /**
   * Looks up a key, and return a {@link Location} handle that can be used to test existence
   * and read/write values.
   *
   * This function always return the same {@link Location} instance to avoid object allocation.
   *
   * 用于查找某个键，返回Location对象，可以用于测试值是否存在或者读写值。
   * 这个方法总是返回相同的Location对象，避免对象分配。
   */
  public Location lookup(Object keyBase, long keyOffset, int keyLength) {
    safeLookup(keyBase, keyOffset, keyLength, loc,
      Murmur3_x86_32.hashUnsafeWords(keyBase, keyOffset, keyLength, 42));
    return loc;
  }

  /**
   * Looks up a key, and return a {@link Location} handle that can be used to test existence
   * and read/write values.
   *
   * This function always return the same {@link Location} instance to avoid object allocation.
   *
   * 用于查找某个键，返回Location对象，可以用于测试值是否存在或者读写值。
   * 这个方法总是返回相同的Location对象，避免对象分配。
   */
  public Location lookup(Object keyBase, long keyOffset, int keyLength, int hash) {
    safeLookup(keyBase, keyOffset, keyLength, loc, hash);
    return loc;
  }

  /**
   * Looks up a key, and saves the result in provided `loc`.
   *
   * This is a thread-safe version of `lookup`, could be used by multiple threads.
   *
   * 查找键，将结果保存到Location中。
   * 这是一个线程安全的查找方法，可以在多线程环境使用。
   */
  public void safeLookup(Object keyBase, long keyOffset, int keyLength, Location loc, int hash) {
    assert(longArray != null);

    if (enablePerfMetrics) {
      numKeyLookups++; // 记录查找键的操作次数
    }
    // 哈希值限定范围
    int pos = hash & mask;
    int step = 1;

    // 不断循环
    while (true) {
      // 如果开启了性能度量，自增探测计数
      if (enablePerfMetrics) {
        numProbes++;
      }

      // 根据pos获取键的对应键值对数据的内存地址
      if (longArray.get(pos * 2) == 0) {
        // This is a new key.
        // 获取的是0，表示这是一个新的键，因此直接将该键锚定到loc上返回
        loc.with(pos, hash, false);
        return;
      } else {
        // 获取的不为0，说明该对应位置上已经有键存在于Map中，那就获取已存储键的哈希码，根据哈希码判断两个键是否相同
        long stored = longArray.get(pos * 2 + 1);
        if ((int) (stored) == hash) { // 哈希码相同，比较键是否相同
          // Full hash code matches.  Let's compare the keys for equality.
          // 锚定loc到pos位置，以获取具体的键值数据
          loc.with(pos, hash, true);
          if (loc.getKeyLength() == keyLength) { // 已存储的键数据长度跟传入的键数据长度一致
            // 逐个比较两个键的字节数据
            final boolean areEqual = ByteArrayMethods.arrayEquals(
              keyBase,
              keyOffset,
              loc.getKeyBase(),
              loc.getKeyOffset(),
              keyLength
            );

            if (areEqual) {
              // 两个键相同，说明找到了，此时loc值已经锚定到了正确的数据位置，直接返回
              return;
            } else {
              // 没有找到，需要在下一次循环重试，如果开启了性能度量，就将哈希冲突的计数自增
              if (enablePerfMetrics) {
                numHashCollisions++;
              }
            }
          }
        }
      }
      // 走到这里，说明发送了哈希冲突，需要自增pos，进行线性探测。
      pos = (pos + step) & mask;
      step++;
    }
  }

  /**
   * Handle returned by {@link BytesToBytesMap#lookup(Object, long, int)} function.
   *
   * 由BytesToBytesMap的lookup方法返回，用于操作底层内存的Location Handle
   */
  public final class Location {
    /** An index into the hash map's Long array
     * 指向BytesToBytesMap的LongArray中的索引
     **/
    private int pos;
    /** True if this location points to a position where a key is defined, false otherwise
     * 如果当前Location指向的位置上键已经定义了，就返回true，否则返回false
     **/
    private boolean isDefined;
    /**
     * The hashcode of the most recent key passed to
     * {@link BytesToBytesMap#lookup(Object, long, int, int)}. Caching this hashcode here allows us
     * to avoid re-hashing the key when storing a value for that key.
     *
     * 最近穿给BytesToBytesMap的lookup方法的键的哈希码。
     * 缓存该哈希码以避免在正在存储该键对应的值时对该键进行Rehash。
     */
    private int keyHashcode;

    // 存放键值的baseObject
    private Object baseObject;  // the base object for key and value
    private long keyOffset;
    private int keyLength;
    private long valueOffset;
    private int valueLength;

    /**
     * Memory page containing the record. Only set if created by {@link BytesToBytesMap#iterator()}.
     *
     * 存放记录的内存页，仅在BytesToBytesMap的iterator()中创建Location时被设置。
     */
    @Nullable private MemoryBlock memoryPage;

    // 更新Address和Size
    private void updateAddressesAndSizes(long fullKeyAddress) {
      updateAddressesAndSizes(
          // 这里其实使用了TaskMemoryManager的PackedRecordPointer复合指针进行定位的
        taskMemoryManager.getPage(fullKeyAddress), // 13 bit的Page Number
        taskMemoryManager.getOffsetInPage(fullKeyAddress)); // 51 bit的偏移量
    }

    /**
     * 该方法根据baseObject和offset来更新地址及偏移量，
     * 定位键和值的偏移量及长度。
     */
    private void updateAddressesAndSizes(final Object base, long offset) {
      // 更新baseObject
      baseObject = base;

      // 计算总字节大小
      final int totalLength = UnsafeAlignedOffset.getSize(base, offset);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();

      // offset需要向前偏移UAO Size
      offset += uaoSize;

      // 获取键的大小
      keyLength = UnsafeAlignedOffset.getSize(base, offset);

      // offset需要再向前偏移UAO Size
      offset += uaoSize;

      // 键偏移量
      keyOffset = offset;

      // 值偏移量（值就在键数据的后面）
      valueOffset = offset + keyLength;
      // 值的大小 = 总大小 - 键大小 - UAO Size
      valueLength = totalLength - keyLength - uaoSize;
    }

    /**
     *
     * @param pos
     * @param keyHashcode
     * @param isDefined
     * @return
     */
    private Location with(int pos, int keyHashcode, boolean isDefined) {
      assert(longArray != null); // 要求LongArray不为空
      this.pos = pos;
      this.isDefined = isDefined; // 用于标记记录是否存在
      this.keyHashcode = keyHashcode;
      if (isDefined) { // 如果记录已经存在，需要将相关的变量更新为对应记录相关的值
        // 获取键的指针，并定位该指针的键值数据
        final long fullKeyAddress = longArray.get(pos * 2);
        updateAddressesAndSizes(fullKeyAddress);
      }
      return this;
    }

    // 定位具体的内存页中的键值对数据
    private Location with(MemoryBlock page, long offsetInPage) {
      this.isDefined = true;
      this.memoryPage = page;
      updateAddressesAndSizes(page.getBaseObject(), offsetInPage);
      return this;
    }

    /**
     * This is only used for spilling
     * 仅用于溢写，该方法第三个参数给定了键值对数据的长度。
     */
    private Location with(Object base, long offset, int length) {
      this.isDefined = true;
      this.memoryPage = null;
      baseObject = base;
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      keyOffset = offset + uaoSize;
      keyLength = UnsafeAlignedOffset.getSize(base, offset);
      valueOffset = offset + uaoSize + keyLength;
      valueLength = length - uaoSize - keyLength;
      return this;
    }

    /**
     * Find the next pair that has the same key as current one.
     * 找到下一个与当前键相同的键值对。
     * 在内存页中，相同的键的键值对数据是相邻存放的。
     */
    public boolean nextValue() {
      assert isDefined;
      long nextAddr = Platform.getLong(baseObject, valueOffset + valueLength);
      if (nextAddr == 0) {
        return false;
      } else {
        updateAddressesAndSizes(nextAddr);
        return true;
      }
    }

    /**
     * Returns the memory page that contains the current record.
     * This is only valid if this is returned by {@link BytesToBytesMap#iterator()}.
     *
     * 返回当前Location记录使用的MemoryBlock内存页。
     * 只有在BytesToBytesMap的iterator()方法返回时才有效。
     */
    public MemoryBlock getMemoryPage() {
      return this.memoryPage;
    }

    /**
     * Returns true if the key is defined at this position, and false otherwise.
     */
    public boolean isDefined() {
      return isDefined;
    }

    /**
     * Returns the base object for key.
     */
    public Object getKeyBase() {
      assert (isDefined);
      return baseObject;
    }

    /**
     * Returns the offset for key.
     */
    public long getKeyOffset() {
      assert (isDefined);
      return keyOffset;
    }

    /**
     * Returns the base object for value.
     */
    public Object getValueBase() {
      assert (isDefined);
      return baseObject;
    }

    /**
     * Returns the offset for value.
     */
    public long getValueOffset() {
      assert (isDefined);
      return valueOffset;
    }

    /**
     * Returns the length of the key defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getKeyLength() {
      assert (isDefined);
      return keyLength;
    }

    /**
     * Returns the length of the value defined at this position.
     * Unspecified behavior if the key is not defined.
     */
    public int getValueLength() {
      assert (isDefined);
      return valueLength;
    }

    /**
     * Append a new value for the key. This method could be called multiple times for a given key.
     * The return value indicates whether the put succeeded or whether it failed because additional
     * memory could not be acquired.
     * <p>
     * It is only valid to call this method immediately after calling `lookup()` using the same key.
     * </p>
     * <p>
     * The key and value must be word-aligned (that is, their sizes must multiples of 8).
     * </p>
     * <p>
     * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
     * will return information on the data stored by this `append` call.
     * </p>
     * <p>
     * As an example usage, here's the proper way to store a new key:
     * </p>
     * <pre>
     *   Location loc = map.lookup(keyBase, keyOffset, keyLength);
     *   if (!loc.isDefined()) {
     *     if (!loc.append(keyBase, keyOffset, keyLength, ...)) {
     *       // handle failure to grow map (by spilling, for example)
     *     }
     *   }
     * </pre>
     * <p>
     * Unspecified behavior if the key is not defined.
     * </p>
     *
     * 为键创建新的值。对于给定的键，这个方法会被调用多次。
     * 返回值表示是否添加成功，可能会由于无法获取到内存而失败。
     *
     * 只有在调用lookup方法后使用相同的键调用本方法才是有效的。
     * 键和值必须的字对齐的（即，它们的宽度必须是8的倍数）。
     *
     * 在调用本方法后，调用get[Key|Value]Address()和get[Key|Value]Length会返回本方法存储的数据信息。
     *
     * 例如，下面是存入新建的方法：
     * <pre>
     *   Location loc = map.lookup(keyBase, keyOffset, keyLength);
     *   if (!loc.isDefined()) {
     *     if (!loc.append(keyBase, keyOffset, keyLength, ...)) {
     *       // handle failure to grow map (by spilling, for example)
     *     }
     *   }
     * </pre>
     *
     * 如果键未被定义，将不会有任何行为。
     *
     * @return true if the put() was successful and false if the put() failed because memory could
     *         not be acquired.
     */
    public boolean append(Object kbase, long koff, int klen, Object vbase, long voff, int vlen) {
      // 检查键和值的字节长度是否是8的倍数数
      assert (klen % 8 == 0);
      assert (vlen % 8 == 0);

      // 检查LongArray不能为空
      assert (longArray != null);

      if (numKeys == MAX_CAPACITY
        // The map could be reused from last spill (because of no enough memory to grow),
        // then we don't try to grow again if hit the `growthThreshold`.
          /**
           * 这个Map在上一次溢写后无法被重用（由于没有足够的内存以扩容），
           * 因此就算达到了扩容阈值，这里也不会尝试再次扩容。
           */
        || !canGrowArray && numKeys > growthThreshold) {
        return false;
      }

      // Here, we'll copy the data into our data pages. Because we only store a relative offset from
      // the key address instead of storing the absolute address of the value, the key and value
      // must be stored in the same memory page.
      // (8 byte key length) (key) (value) (8 byte pointer to next value)
      int uaoSize = UnsafeAlignedOffset.getUaoSize(); // 获取UAO Size
      // 记录的总长为：2 * UAO Size + Key length + Value length + 8
      final long recordLength = (2 * uaoSize) + klen + vlen + 8;

      // 检查当前页的大小是否足够存储recordLength字节数据，如果不够就进行内存申请。
      if (currentPage == null || currentPage.size() - pageCursor < recordLength) {
        if (!acquireNewPage(recordLength + uaoSize)) {
          return false;
        }
      }

      // --- Append the key and value data to the current data page --------------------------------
      // 将键值对数据添加到当前内存页中
      final Object base = currentPage.getBaseObject();

      // 获取可写的起始偏移量
      long offset = currentPage.getBaseOffset() + pageCursor;
      final long recordOffset = offset;

      // 写入Key Length + Value length + UAO Size
      UnsafeAlignedOffset.putSize(base, offset, klen + vlen + uaoSize);
      // 写入Key Length
      UnsafeAlignedOffset.putSize(base, offset + uaoSize, klen);
      // 写入kbase中键数据到base中
      offset += (2 * uaoSize);
      Platform.copyMemory(kbase, koff, base, offset, klen);
      offset += klen;
      // 写入kbase中值数据到base中
      Platform.copyMemory(vbase, voff, base, offset, vlen);
      offset += vlen;
      // put this value at the beginning of the list
      // 如果该值之前已经存在，那么会在后面加上之前那个值的地址。
      Platform.putLong(base, offset, isDefined ? longArray.get(pos * 2) : 0);

      // --- Update bookkeeping data structures ----------------------------------------------------
      // 重置为baseOffset
      offset = currentPage.getBaseOffset();
      // 将头部存放页中记录总数的值 + 1
      UnsafeAlignedOffset.putSize(base, offset, UnsafeAlignedOffset.getSize(base, offset) + 1);

      // 更新页游标，下次从该游标处开始写新的键值对
      pageCursor += recordLength;

      // 根据页号、偏移量组合键的复合地址
      final long storedKeyAddress = taskMemoryManager.encodePageNumberAndOffset(
        currentPage, recordOffset);

      // 将键的复合地址存放到LongArray中
      longArray.set(pos * 2, storedKeyAddress);
      updateAddressesAndSizes(storedKeyAddress);
      numValues++; // 值数量 ++
      if (!isDefined) {
        numKeys++; // 如果是新的键，键数量++
        // 存放键的哈希码
        longArray.set(pos * 2 + 1, keyHashcode);
        isDefined = true;

        // 检查是否需要ReHash
        if (numKeys > growthThreshold && longArray.size() < MAX_CAPACITY) {
          try {
            growAndRehash();
          } catch (OutOfMemoryError oom) {
            canGrowArray = false;
          }
        }
      }
      return true;
    }
  }

  /**
   * Acquire a new page from the memory manager.
   * 申请新的内存页。
   *
   * @return whether there is enough space to allocate the new page.
   */
  private boolean acquireNewPage(long required) {
    try {
      currentPage = allocatePage(required);
    } catch (OutOfMemoryError e) {
      return false;
    }

    // 申请的内存页是放入到dataPages队尾的。
    dataPages.add(currentPage);

    // 先将页中的记录数记为0
    UnsafeAlignedOffset.putSize(currentPage.getBaseObject(), currentPage.getBaseOffset(), 0);

    // 将起始游标指向可以记录数据的起始偏移量位置，前面的UAO Size个字节用于存放内存页中的记录总数
    pageCursor = UnsafeAlignedOffset.getUaoSize();
    return true;
  }

  // 其他内存消费者在申请内存时发现内存不足，可能会调用该方法要求溢写数据到文件。
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this && destructiveIterator != null) {
      // 获取迭代器，将指定大小的内存数据溢写到磁盘
      return destructiveIterator.spill(size);
    }
    return 0L;
  }

  /**
   * Allocate new data structures for this map. When calling this outside of the constructor,
   * make sure to keep references to the old data structures so that you can free them.
   *
   * 为该Map申请新的数据结构。
   * 当在构造器之外调用该方法，你需要保持旧的数据结构的引用，以便后面需要对它们进行释放。
   *
   * @param capacity the new map capacity
   */
  private void allocate(int capacity) {
    assert (capacity >= 0);
    // 申请的内存在64 Bytes ~ 1 << 29 Bytes之间
    capacity = Math.max((int) Math.min(MAX_CAPACITY, ByteArrayMethods.nextPowerOf2(capacity)), 64);
    assert (capacity <= MAX_CAPACITY);
    // 申请时会申请两倍
    longArray = allocateArray(capacity * 2);
    longArray.zeroOut();

    // 更新扩容阈值和哈希掩码
    this.growthThreshold = (int) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   *
   * This method is idempotent and can be called multiple times.
   *
   * 释放与此Map关联的所有已分配内存，包括键和值的存储以及哈希映射数组本身。
   * 这个方法是幂等的，可以被调用多次。
   */
  public void free() {
    // 更新使用内存的峰值度量
    updatePeakMemoryUsed();

    // 释放LongArray
    if (longArray != null) {
      freeArray(longArray);
      longArray = null;
    }

    // 释放dataPages中的页
    Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
    while (dataPagesIterator.hasNext()) {
      MemoryBlock dataPage = dataPagesIterator.next();
      dataPagesIterator.remove();
      freePage(dataPage);
    }
    assert(dataPages.isEmpty());

    while (!spillWriters.isEmpty()) {
      File file = spillWriters.removeFirst().getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        }
      }
    }
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  public long getPageSizeBytes() {
    return pageSizeBytes;
  }

  /**
   * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
   * 返回使用的所有内存。
   */
  public long getTotalMemoryConsumption() {
    long totalDataPagesSize = 0L;
    for (MemoryBlock dataPage : dataPages) {
      totalDataPagesSize += dataPage.size();
    }
    return totalDataPagesSize + ((longArray != null) ? longArray.memoryBlock().size() : 0L);
  }

  // 更新峰值内存
  private void updatePeakMemoryUsed() {
    long mem = getTotalMemoryConsumption();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   * 返回迄今为止使用的峰值内存
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * Returns the total amount of time spent resizing this map (in nanoseconds).
   *
   * 返回用于对Map进行Resize花费的时间
   */
  public long getTimeSpentResizingNs() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return timeSpentResizingNs;
  }

  /**
   * Returns the average number of probes per key lookup.
   * 返回在查找键时的平均探测时间。
   */
  public double getAverageProbesPerLookup() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return (1.0 * numProbes) / numKeyLookups;
  }

  public long getNumHashCollisions() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException();
    }
    return numHashCollisions;
  }

  @VisibleForTesting
  public int getNumDataPages() {
    return dataPages.size();
  }

  /**
   * Returns the underline long[] of longArray.
   *
   * 返回底层long[]数组，以LongArray类型。
   */
  public LongArray getArray() {
    assert(longArray != null);
    return longArray;
  }

  /**
   * Reset this map to initialized state.
   * 将当前Map重置为初始状态。
   */
  public void reset() {
    numKeys = 0;
    numValues = 0;

    // 释放所有申请的内存
    freeArray(longArray);
    while (dataPages.size() > 0) {
      MemoryBlock dataPage = dataPages.removeLast();
      freePage(dataPage);
    }

    // 申请初始容量
    allocate(initialCapacity);
    currentPage = null;
    pageCursor = 0;
  }

  /**
   * Grows the size of the hash table and re-hash everything.
   *
   * 对Map进行扩容，对所有键值对进行Rehash。
   */
  @VisibleForTesting
  void growAndRehash() {
    assert(longArray != null);

    long resizeStartTime = -1;
    if (enablePerfMetrics) {
      resizeStartTime = System.nanoTime();
    }
    // Store references to the old data structures to be used when we re-hash
    // 记录旧的数据结构
    final LongArray oldLongArray = longArray;

    // 记录旧的容量
    final int oldCapacity = (int) oldLongArray.size() / 2;

    // Allocate the new data structures
    // 申请新的数据结构，两倍扩容
    allocate(Math.min(growthStrategy.nextCapacity(oldCapacity), MAX_CAPACITY));

    // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
    for (int i = 0; i < oldLongArray.size(); i += 2) {
      // 遍历每一个键的指针
      final long keyPointer = oldLongArray.get(i);
      if (keyPointer == 0) {
        continue;
      }
      // 遍历每一个键的哈希码
      final int hashcode = (int) oldLongArray.get(i + 1);

      // 计算新的位置
      int newPos = hashcode & mask;

      // 线性探测，直到探测到空闲位置
      int step = 1;
      while (longArray.get(newPos * 2) != 0) {
        newPos = (newPos + step) & mask;
        step++;
      }

      // 将键的指针和哈希码设置到探测到的位置里。
      longArray.set(newPos * 2, keyPointer);
      longArray.set(newPos * 2 + 1, hashcode);
    }

    // 释放旧的LongArray
    freeArray(oldLongArray);

    if (enablePerfMetrics) {
      timeSpentResizingNs += System.nanoTime() - resizeStartTime;
    }
  }
}
