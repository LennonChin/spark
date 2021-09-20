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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
 *
 * @param context     TaskContextImpl实现类
 * @param aggregator  optional Aggregator with combine functions to use for merging data
 *                    对map任务的输出数据进行聚合的聚合器
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 *                    对map任务的输出数据按照key计算分区的分区计算器Partitioner
 * @param ordering    optional Ordering to sort keys within each partition; should be a total ordering
 *                    对map任务的输出数据按照key进行排序的scala.math.Ordering的实现类
 * @param serializer  serializer to use when spilling to disk
 *                    即SparkEnv的子组件serializer
 * @tparam K 键类型
 * @tparam V 值类型
 * @tparam C 聚合后的值类型
 */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
  with Logging {

  private val conf = SparkEnv.get.conf

  // 分区数量。默认为1。
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  // 是否有分区。当numPartitions大于1时为true。
  private val shouldPartition = numPartitions > 1
  // 使用分区器获取键的分区
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  // SparkEnv的子组件BlockManager
  private val blockManager = SparkEnv.get.blockManager
  // BlockManager的子组件DiskBlockManager
  private val diskBlockManager = blockManager.diskBlockManager
  // SparkEnv的子组件SerializerManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  // 用于设置DiskBlockObjectWriter内部的文件缓冲大小。可通过spark.shuffle.file.buffer属性进行配置，默认是32KB。
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  /**
    * 用于将DiskBlockObjectWriter内部的文件缓冲写到磁盘的大小。
    * 可通过spark.shuffle.spill.batchSize属性进行配置，默认是10000。
    */
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  /**
   * 当设置了聚合器（Aggregator）时，Map端将中间结果溢出到磁盘前，
   * 先利用此数据结构在内存中对中间结果进行聚合处理。
   */
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  /**
   * 当没有设置聚合器（Aggregator）时，Map端将中间结果溢出到磁盘前，
   * 先利用此数据结构将中间结果存储在内存中。
   */
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  // 用于对溢出到磁盘的字节数进行统计（单位为字节）。
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  // 内存中数据结构大小的峰值（单位为字节）。
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // 是否对Shuffle数据进行排序
  @volatile private var isShuffleSort: Boolean = true
  /**
    * 缓存强制溢出的文件数组。
    * SpilledFile保存了溢出文件的信息，包括：
    * - file（文件）
    * - blockId（BlockId）
    * - serializerBatchSizes
    * - elementsPerPartition（每个分区的元素数量）。
    */
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  // 用于包装内存中数据的迭代器和溢出文件，并表现为一个新的迭代器。
  @volatile private var readingIterator: SpillableIterator = null

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  // 中间输出的key的比较器。用于在分区内对中间结果按照key进行排序，以便于聚合。
  private val keyComparator: Comparator[K] = ordering.getOrElse(
    // 当没有指定比较器时，会使用默认的按照key的哈希值进行比较的比较器
    new Comparator[K] {
      override def compare(a: K, b: K): Int = {
        val h1 = if (a == null) 0 else a.hashCode()
        val h2 = if (b == null) 0 else b.hashCode()
        if (h1 < h2) -1 else if (h1 == h2) 0 else 1
      }
    }
  )

  // 获取比较器
  private def comparator: Option[Comparator[K]] = {
    // 只有在ordering被定义，或开启了Map端聚合时才需要比较器
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
    file: File, // 溢写的文件
    blockId: BlockId, // 对应的数据块的BlockId
    serializerBatchSizes: Array[Long], // 每个批次的数据大小，默认是10000条数据一个批次
    elementsPerPartition: Array[Long]) // 每个分区的元素数量

  // 缓存溢出的文件数组。
  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   *
   * numSpills方法用于返回spills的大小，即溢出的文件数量。
   */
  private[spark] def numSpills: Int = spills.size

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    /**
     * Aggregator的参数：
     * - createCombiner: V => C，创建聚合的初始值。
     * - mergeValue: (C, V) => C，用于将新的值合并到已有的聚合结果中。
     * - mergeCombiners: (C, C) => C，合并多个聚合结果。
     */
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) { // 如果用户指定了聚合器，那么对数据进行聚合
      // Combine values in-memory first using our AppendOnlyMap
      // 获取聚合器的mergeValue函数，此函数用于将新的Value合并到聚合的结果中
      val mergeValue = aggregator.get.mergeValue
      // 获取聚合器的createCombiner函数，此函数用于创建聚合的初始值。
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      /**
       * 定义偏函数，当有新的Value时，调用mergeValue函数将新的Value合并到之前聚合的结果中，
       * 否则说明刚刚开始聚合，此时调用createCombiner函数以Value作为聚合的初始值
       */
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }

      // 迭代输入的记录
      while (records.hasNext) {
        // 增加已经读取的元素数
        addElementsRead()
        kv = records.next()
        // 计算分区索引（ID），将分区索引与key、update偏函数作为参数对由分区索引与key组成的对偶进行聚合
        map.changeValue((getPartition(kv._1), kv._1), update)
        // 进行可能的磁盘溢出
        maybeSpillCollection(usingMap = true)
      }
    } else { // 如果用户没有指定聚合器，只对数据进行缓冲
      // Stick values into our buffer
      while (records.hasNext) {
        // 增加已经读取的元素计数
        addElementsRead()
        val kv = records.next()
        // 计算分区索引ID，调用PartitionedPairBuffer的insert()方法
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        // 进行可能的磁盘溢出
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * 用于判断何时需要将内存中的数据写入磁盘
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) { // 如果使用了PartitionedAppendOnlyMap
      // 对PartitionedAppendOnlyMap的大小进行估算
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) { // 将PartitionedAppendOnlyMap中的数据溢出到磁盘
        // 重新创建PartitionedAppendOnlyMap
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else { // 如果使用了PartitionedPairBuffer
      // 对PartitionedPairBuffer的大小进行估算
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) { // 将PartitionedPairBuffer中的数据溢出到磁盘
        // 重新创建PartitionedPairBuffer
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) { // 更新ExternalSorter已经使用的内存大小的峰值
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    /**
     * 获取WritablePartitionedIterator，传入的是比较器由comparator方法提供。
     * 如果定义了ordering或aggregator，那么比较器就是keyComparator，否则没有比较器。
     * 这里会影响后面的排序过程：
     * - 如果指定了ordering或aggregator，此处传入keyComparator，后面的[[PartitionedAppendOnlyMap]]不仅会根据分区ID进行排序，还会根据keyComparator对键进行排序；
     * - 如果没有指定ordering或aggregator，此处传入None，后面的[[PartitionedPairBuffer]]则只会根据分区ID进行排序。
     */
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    // 将集合中的数据溢出到磁盘
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    // 将溢出生成的文件添加到spills中
    spills += spillFile
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   * 强制溢写当前内存中的集合数据到磁盘，以释放内存。
   * 该方法会在Task内存不足时由TaskMemoryManager调用。
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) { // 需要对Shuffle数据进行排序
      // 因此无法溢写
      false
    } else {
      assert(readingIterator != null)
      // 进行溢写操作
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * Spill contents of in-memory iterator to a temporary file on disk.
   *
   * 将WritablePartitionedIterator迭代器中的数据溢写到磁盘
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    // 创建唯一的BlockId和文件
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    // objectsWritten用于统计已经写入磁盘的键值对数量
    var objectsWritten: Long = 0
    // 用于对Shuffle中间结果写入到磁盘的度量与统计。
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // 获取DiskBlockObjectWriter
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    // 创建存储批次大小的数组缓冲batchSizes
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    // 创建存储每个分区有多少个元素的数组缓冲
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      // 将DiskBlockObjectWriter的输出流中的数据真正写入到磁盘
      val segment = writer.commitAndGet()
      // 将本批次写入的文件长度添加到batchSizes和_diskBytesSpilled中
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      // 将objectsWritten清零，以便下一批次的操作
      objectsWritten = 0
    }

    var success = false
    try {
      // 对WritablePartitionedIterator进行迭代
      while (inMemoryIterator.hasNext) {
        // 获取数据的分区ID
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        // 将键值对写入磁盘
        inMemoryIterator.writeNext(writer)
        // 将elementsPerPartition中统计的分区对应的元素数量加1
        elementsPerPartition(partitionId) += 1
        // 将objectsWritten加一
        objectsWritten += 1

        // 每写10000条记录进行一次刷盘
        if (objectsWritten == serializerBatchSize) {
          // 将DiskBlockObjectWriter的输出流中的数据真正写入到磁盘
          flush()
        }
      }
      // 遍历记录完毕，如果还有剩余未刷盘的数据，则进行刷盘
      if (objectsWritten > 0) {
        // 将DiskBlockObjectWriter的输出流中的数据真正写入到磁盘
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      // 标记写出成功
      success = true
    } finally {
      if (success) {
        // 关闭写出器
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    /**
     * 创建并返回SpilledFile
     * batchSizes.toArray：记录了每个批次的数据大小
     * elementsPerPartition：记录了每个分区有多少条键值对记录
     */
    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   *
   * 用于将destructiveIterator()方法返回的迭代器中数据（存储在内存中）
   * 与已经溢出到磁盘的文件进行合并
   *
   * @param spills 溢写的文件
   * @param inMemory destructiveIterator方法迭代器，其中的记录按照分区ID和键进行了排序
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    // 为每个SpilledFile创建SpillReader读取器
    val readers = spills.map(new SpillReader(_))
    // 创建缓冲迭代器，该迭代器扩展了一个功能方法head()，即可以查看迭代器中的下一个元素，但不会将它移出
    val inMemBuffered = inMemory.buffered
    // 遍历所有分区的ID，顺序遍历
    (0 until numPartitions).iterator.map { p =>
      // 为当前分区创建一个迭代器
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      // 使用SpillReader顺序读取包含了每个分区的数据的迭代器，并与inMemIterator迭代器合并
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      // 判断是否需要聚合
      if (aggregator.isDefined) { // 需要聚合
        // Perform partial aggregation across partitions
        /**
         * 使用mergeWithAggregation()方法进行聚合，返回元素类型为 (分区ID, 对应的聚合数据的迭代器) 的迭代器
         * 虽然传入了ordering参数，但是目前Spark在Map端不存在需要排序的算子；
         * 典型的在Map端聚合的算子：reduceByKey、aggregateByKey。
         */
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) { // 需要排序
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        /**
         * 使用mergeSort()方法进行归并排序，返回元素类型为 (分区ID, 对应的有序数据的迭代器) 的迭代器
         * 不过目前Spark在Map端不存在需要排序的算子；
         * sortByKey、repartitionAndSortWithinPartitions()两个算子都是在Shuffle Read阶段排序的。
         */
        (p, mergeSort(iterators, ordering.get))
      } else {
        // 不需要聚合，也不需要排序，直接合并溢写文件中对应分区的数据
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   * 使用比较器对迭代器内的键值对数据进行归并排序
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    val bufferedIters = iterators
      .filter(_.hasNext) // 先过滤还有元素的迭代器
      .map(_.buffered) // 再将每个迭代器转换为缓冲迭代器
    type Iter = BufferedIterator[Product2[K, C]]
    /**
     * 构造一个优先队列，作为小顶堆结构
     * 注意，PriorityQueue队列在添加完元素后，并非元素就是有序的，
     * 只有在dequeue或者dequeueAll的时候才会使用比较方法比较并返回有序的元素。
     */
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      // 比较方法，使用比较器比较每个两个迭代器的下一个元素（即键值对）的键
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    // 将bufferedIters添加到heap队列，即可实现排序
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    // 返回一个新的迭代器，其中的元素都存放在heap优先队列中
    new Iterator[Product2[K, C]] {
      // 是否还有下一个元素
      override def hasNext: Boolean = !heap.isEmpty

      // 获取下一个元素
      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        // heap出队一个迭代器
        val firstBuf = heap.dequeue()
        // 使用该迭代器获取一个键值对
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) { // 如果该迭代器还有元素
          // 将该迭代器再次放入heap堆中，这样下次还可以从该迭代器获取键值对
          heap.enqueue(firstBuf)
        }
        // 返回迭代到的键值对
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   *
   * 对键值对以key进行聚合，
   *
   * @param iterators 需要聚合的键值对迭代器
   * @param mergeCombiners 聚合函数
   * @param comparator 比较器
   * @param totalOrder 是否是全排序
   * @return
   */
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] =
  {
    if (!totalOrder) { // 不是全排序
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      // 创建新的迭代器
      new Iterator[Iterator[Product2[K, C]]] {
        // 先使用mergeSort()方法进行合并排序，该方法迭代的键值对会按照键进行排序
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        // 装载键的数组
        val keys = new ArrayBuffer[K]
        // 聚合的值的数组
        val combiners = new ArrayBuffer[C]

        // 需要看sorted中是否还有下一个
        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          // 清空键数组和聚合值数组
          keys.clear()
          combiners.clear()
          // 获得下一个键值对
          val firstPair = sorted.next()
          // 将键存入keys
          keys += firstPair._1
          // 将值存入combiners
          combiners += firstPair._2
          // 记录当前迭代到的键
          val key = firstPair._1

          /**
           * 满足下面两个条件，就循环：
           * 1. sorted还有下一个键值对；
           * 2. 比较下一个键值对的键与当前key记录的键是否相等。
           *
           * 如果1和2都满足，说明下一个键值对的键与当前键值对的键是一样的
           */
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            // 获取下一个键值对pair
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            // 从keys数组中找到对应的键
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) { // 找到相同的键了
                /**
                 * 取出聚合数组中对应的旧的聚合值，
                 * 将旧的聚合值与当前pair的值使用mergeCombiners函数进行聚合，
                 * 将新值存入combiners
                 */
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                // 找到对应的键，说明该键之前已经迭代到了
                foundKey = true
              }
              i += 1
            }
            // 没有找到对应的键，则将键和值分别存入keys和combiners
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          // 将键和聚合的值进行zip，获取新的键值对
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else { // 全排序
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        // 先使用mergeSort()方法进行合并排序，该方法迭代的键值对会按照键进行排序
        val sorted = mergeSort(iterators, comparator).buffered

        // 需要看sorted中是否还有下一个
        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          // 获取sorted中的下一个键值对
          val elem = sorted.next()
          // 取出键
          val k = elem._1
          // 取出值
          var c = elem._2
          /**
           * 满足下面两个条件，就循环：
           * 1. sorted还有下一个键值对；
           * 2. 比较下一个键值对的键与当前k记录的键是否相等。
           *
           * 如果1和2都满足，说明下一个键值对的键与当前键值对的键是一样的
           */
          while (sorted.hasNext && sorted.head._1 == k) {
            // 获取下一个键值对
            val pair = sorted.next()
            // 将下一个键值对的值和当前值使用mergeCombiners()方法进行聚合，聚合的值赋值给c
            c = mergeCombiners(c, pair._2)
          }
          // 返回键和聚合后的值
          (k, c)
        }
      }
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   *
   * 溢写文件读取器，用于读取溢写的文件。
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    /**
     * scanLeft函数会将当前索引位置之前的元素进行算子计算，如：
     * val s = 1 to 10
     *    => Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
     * val ss = s.scanLeft(0)(_ + _)
     *    => Vector(0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55)
     *
     * 通过这种方式，可以得到每个批次的数据的偏移量
     */
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    var partitionId = 0 // 分区ID
    var indexInPartition = 0L // 当前分区读取的键值对条数
    var batchId = 0 // 批次ID
    var indexInBatch = 0 // 当前批次已读取的键值对条数
    var lastPartitionId = 0 // 记录最后读取的分区的ID

    // 跳到下一个分区开头
    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    // 输入流
    var fileStream: FileInputStream = null
    // 反序列化流
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    // 下一个键值对
    var nextItem: (K, C) = null
    // 是否结束读取
    var finished = false

    /** Construct a stream that only reads from the next batch
     * 为下一个批次的数据构建反序列化流
     */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      /**
       * batchOffsets数组的长度为批次数量 + 1，
       * 可以使用batchOffsets数组的长度判断批次ID是否在范围内。
       */
      if (batchId < batchOffsets.length - 1) {
        /**
         * 如果反序列化流不为null，说明之前读取了一个批次的数据。
         * 为了让两个批次的数据互不干扰，先关闭该反序列化流和文件输入流
         */
        if (deserializeStream != null) {
          // 先关闭反序列化流和文件输入流
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        // 获取起始偏移量
        val start = batchOffsets(batchId)
        // 以溢写文件构建输入流
        fileStream = new FileInputStream(spill.file)
        // 使用FileChannel定位到起始偏移量
        fileStream.getChannel.position(start)
        // 将batchId自增1
        batchId += 1

        // 获取终止偏移量
        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        // 将文件输入流包装为缓冲输入流
        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        // 将缓冲输入流进行压缩和加密
        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        // 将经过压缩和加密的流封装为反序列化流
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        // 没有更多的批次需要处理，执行清理操作
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     * 跳到下一个分区开头
     */
    private def skipToNextPartition() {
      // 分区号小于最大分区数，且分区内索引达到了该分区内最大元素的数量
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        // 分区号自增1
        partitionId += 1
        // 重置分区内索引记录为0
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     *
     * 获取下一个键值对
     */
    private def readNextItem(): (K, C) = {
      // 检查是否已经完成读取，或者反序列化流是否为null
      if (finished || deserializeStream == null) {
        return null
      }
      // 读取键和值
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      // 记录当前读取的分区ID
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      // 批次索引自增
      indexInBatch += 1

      /**
       * serializerBatchSize是每个批次刷盘的阈值，默认为10000，
       * 即溢写键值对时，每10000条键值对就进行一次刷盘，
       * 因此如果当前批次读取的条数达到了10000阈值，则需要读取下一个批次了。
       */
      if (indexInBatch == serializerBatchSize) {
        // 重置当前批次读取的条数记录为0
        indexInBatch = 0
        // 创建下一个批次的数据的反序列化流
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      // 当前分区读取的键值对条数自增
      indexInPartition += 1
      // 判断是否要跳向下一个分区
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      /**
       * 如果读取的分区是最后一个分区，
       * 注意，partitionId是在skipToNextPartition()中自增的，
       * 当partitionId与numPartitions相等，说明所有分区都读完了，
       * numPartitions - 1是最后一个分区的ID
       */
      if (partitionId == numPartitions) {
        // 标记为true
        finished = true
        // 关闭反序列化流
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      // 返回本次读取的键值对
      (k, c)
    }

    // 下一个要读取的分区的ID，默认从0开始
    var nextPartitionToRead = 0

    // 读取下一个分区的数据，返回的是所有该分区的键值对的迭代器
    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      // 当前读取的分区的ID
      val myPartition = nextPartitionToRead
      // 更新nextPartitionToRead，自增1
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          // 读取当前读取的分区的下一条记录
          nextItem = readNextItem()
          if (nextItem == null) { // 读取为空，说明当前分区没有下一条记录了
            // 返回false
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        // 由于readNextItem()读取时可能会跳到下一个分区，如果没有跳到下一个分区，说明当前分区还有数据
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        // 将nextItem返回即可
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      // 重置batchId
      batchId = batchOffsets.length  // Prevent reading any other batch
      // 释放反序列化流，协助GC
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      if (ds != null) {
        // 关闭反序列化流
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    if (isShuffleSort) { // 需要对Shuffle数据进行排序
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    // 获取当前使用的数据结构
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    // 判断spills是否为空，如果为空表示还没有溢写到磁盘文件
    if (spills.isEmpty) { // 没有溢出到磁盘的文件，即所有的数据依然都在内存中，直接返回内存数据的迭代器即可
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) { // 对底层data数组中的数据只按照分区ID排序
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        // 对分区进行分组
        groupByPartition(
          // 返回迭代器
          destructiveIterator(
            // 对底层data数组中的数据按照分区ID排序
            collection.partitionedDestructiveSortedIterator(None)
          )
        )
      } else { // 对底层data数组中的数据按照分区ID和key排序
        // We do need to sort by both partition ID and key
        // 对分区进行分组
        groupByPartition(
          // 返回迭代器
          destructiveIterator(
            // 对底层data数组中的额数据按照分区ID和key排序
            collection.partitionedDestructiveSortedIterator(Some(keyComparator))
          )
        )
      }
    } else { // 如果spills中缓存了溢出到磁盘的文件，即有些数据在内存中，有些数据已经溢出到了磁盘上
      // Merge spilled and in-memory data
      // 将溢出的磁盘文件和data数组中的数据合并
      merge(spills,
        // 返回迭代器
        destructiveIterator(
          // 按照分区ID和key排序
          collection.partitionedDestructiveSortedIterator(comparator)
        )
      )
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   * 返回已被聚合器进行聚合的、包含所有键值对数据的迭代器
   */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    // 先获取每个分区数据的迭代器，再进行flatMap
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * 将当前ExternalSorter中的添加数据全部写出到磁盘的文件，这个方法主要被SortShuffleWriter调用。
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   *                写出到存储体系中的BlockId
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   *         返回每个分区数据的长度所组成的数组，该返回值主要交给MapOutputTracker使用
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    // 创建对每个分区的长度进行跟踪的数组lengths
    val lengths = new Array[Long](numPartitions)
    // 获取DiskBlockObjectWriter
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) { // 没有缓存溢出到磁盘的文件，即所有的数据依然都在内存中
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) { // 将底层data数组中的数据按照分区ID分别写入到磁盘中
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        val segment = writer.commitAndGet()
        // 将分区的数据长度更新到lengths数组中
        lengths(partitionId) = segment.length
      }
    } else { // 如果spills中缓存了溢出到磁盘的文件，即有些数据在内存中，有些数据已经溢出到了磁盘上
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      // 使用partitionedIterator()方法对磁盘和内存中的数据进行合并后，将各个元素写到磁盘
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          val segment = writer.commitAndGet()
          // 将各个分区的数据长度更新到lengths数组中
          lengths(id) = segment.length
        }
      }
    }

    // 关闭DiskBlockObjectWriter
    writer.close()
    // 更新度量信息
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    // 返回lengths数组
    lengths
  }

  def stop(): Unit = {
    // 删除所有的溢写文件
    spills.foreach(s => s.file.delete())
    spills.clear()
    // 删除所有的强制溢写文件
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    // 清理集合，释放内存
    if (map != null || buffer != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * 用于对destructiveIterator方法返回的迭代器按照分区ID进行分区
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    // 给每个分区生成了一个IteratorForPartition
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   *
   * 分区数据迭代器
   *
   * @param partitionId 指定的迭代分区
   * @param data 该分区的缓冲迭代器
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    /**
      * 用于判断对于指定分区ID是否有下一个元素：
      * 1. data本身需要有下一个元素。
      * 2. data的下一个元素对应的分区ID要与指定的分区ID一样。
      */
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    // 获取下一个元素
    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      // 直接从data中获取
      val elem = data.next()
      // 返回键值对
      (elem._1._2, elem._2)
    }
  }

  /**
   * 可溢写的迭代器，它会将内存的数据包装为迭代器，在TaskMemoryManager内存不足要求强制溢写时，
   * 会调用该迭代器的spill()方法进行溢写
   *
   * @param upstream 内存数据迭代器
   */
  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {

    // 溢写锁
    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[((Int, K), C)] = null

    // 在初始化时直接读取下一个元素
    private var cur: ((Int, K), C) = readNext()

    // 标记是否溢出了
    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) { // 如果已经进行过溢写
        // 直接返回false
        false
      } else {
        // 创建基于WritablePartitionedIterator的迭代器
        val inMemoryIterator = new WritablePartitionedIterator {
          private[this] var cur = if (upstream.hasNext) upstream.next() else null

          // 使用DiskBlockObjectWriter迭代写出
          def writeNext(writer: DiskBlockObjectWriter): Unit = {
            // 写出键值对
            writer.write(cur._1._2, cur._2)
            // 更新cur
            cur = if (upstream.hasNext) upstream.next() else null
          }

          def hasNext(): Boolean = cur != null

          // 下一个分区
          def nextPartition(): Int = cur._1._1
        }
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s" it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")

        // 将WritablePartitionedIterator迭代器中的数据溢写到磁盘
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)

        // 将强制溢写的文件放入到forceSpillFiles进行记录
        forceSpillFiles += spillFile

        // 创建溢写文件读取器
        val spillReader = new SpillReader(spillFile)

        // 以每个分区的数据创建新的迭代器
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          // 获取下一个分区数据的迭代器
          val iterator = spillReader.readNextPartition()
          // 转换为((Int, K), V)类型的键值对
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        // 标记hasSpilled为true，表示已经执行过溢写操作
        hasSpilled = true
        true
      }
    }

    // 读取下一个元素
    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      /**
       * 如果nextUpstream不为null，说明出现过溢写操作，
       * 此时内存中的数据（即upstream迭代器中的数据）都溢写到磁盘了，
       * nextUpstream是根据磁盘文件中的数据构建的新的迭代器，
       * 因此将其赋值给upstream，作为新的迭代器。
       */
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }

      // 如果upstream迭代器有下一个元素，就返回它的下一个元素
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    // 是否有下一个元素，以cur是否为null来判断
    override def hasNext(): Boolean = cur != null

    // 获取下一个元素
    override def next(): ((Int, K), C) = {
      val r = cur
      // 使用readNext()方法来获取
      cur = readNext()
      r
    }
  }
}
