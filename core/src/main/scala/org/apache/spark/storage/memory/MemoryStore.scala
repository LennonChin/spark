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

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockInfoManager, StorageLevel, StreamBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

// 内存中的Block抽象为特质MemoryEntry
private sealed trait MemoryEntry[T] {
  def size: Long // 当前Block的大小
  def memoryMode: MemoryMode // 当前Block的存储的内存类型
  def classTag: ClassTag[T] // 当前Block的类型标记
}

// 表示反序列化后的MemoryEntry
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  val memoryMode: MemoryMode = MemoryMode.ON_HEAP
}

// 表示序列化后的MemoryEntry
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

private[storage] trait BlockEvictionHandler {
  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return
    *
    * @param blockId 被驱逐的数据块的BlockId
    * @param data 被驱逐的数据块的数据
    * @tparam T 被驱逐数据块的数据类型，从前面写入数据的流程可知，
    *           写入的数据可能是ChunkedByteBuffer缓冲区，也有可能是数组类型的集合
    * @return the block's new effective StorageLevel.
    *         数据块被驱逐之后新的持久化级别
    */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
  *
  * 内存存储。依赖于MemoryManager，负责对Block的内存存储。
  *
  * @param blockInfoManager Block信息管理器BlockInfoManager
  * @param serializerManager 序列化管理器SerializerManager
  * @param memoryManager 内存管理器MemoryManager
  * @param blockEvictionHandler Block驱逐处理器。用于将Block从内存中驱逐出去。
  */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  // 内存中的BlockId与MemoryEntry（Block的内存形式）之间映射关系的缓存。
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  // TaskAttempt线程的标识TaskAttemptId与该TaskAttempt线程在堆内存展开的所有Block占用的内存大小之和之间的映射关系。
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  // TaskAttempt线程的标识TaskAttemptId与该TaskAttempt线程在堆外内存展开的所有Block占用的内存大小之和之间的映射关系。
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  // 用来展开任何Block之前，初始请求的内存大小，可以修改属性spark.storage.unrollMemoryThreshold（默认为1MB）改变大小。
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes.
    * MemoryStore用于存储Block的最大内存，其实质为MemoryManager的maxOnHeapStorageMemory和maxOffHeapStorageMemory之和。
    * - 如果Memory Manager为StaticMemoryManager，那么maxMemory的大小是固定的。
    * - 如果Memory Manager为UnifiedMemoryManager，那么maxMemory的大小是动态变化的。
    **/
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes.
    * MemoryStore中已经使用的内存大小。
    * 其实质为MemoryManager中onHeapStorageMemoryPool已经使用的大小和offHeapStorageMemoryPool已经使用的大小之和。
    **/
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
    *
    * MemoryStore用于存储Block（即MemoryEntry）使用的内存大小，即memoryUsed与currentUnrollMemory的差值。
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  // 获取BlockId对应MemoryEntry（即Block的内存形式）所占用的大小。
  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
    *
    * 将BlockId对应的Block（已经封装为ChunkedByteBuffer）写入内存。
   *
   * @return true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    // 申请内存
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) { // 申请成功
      // We acquired enough memory for the block, so go ahead and put it
      // 获取Block的数据
      val bytes = _bytes()
      assert(bytes.size == size)
      // 将数据包装为SerializedMemoryEntry对象
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        // 将Block数据写入entries，即写入内存
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
   * Attempt to put the given block in memory store as values.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * 尝试将给定的Block写入内存，Block是迭代器类型的，里面包含多个对象。
   *
   * 在某些情况下，给定的Block迭代器可能因为包含的对象太大，难以物化存储到内存中。
   * 为了避免OOM异常，这个方法会优雅地展开迭代器，并且周期性地检查是否有足够的内存用于存储。
   * 如果整个迭代器中的Block都可以成功物化，在物化过程中临时使用的o展开内存会转换为存储内存，
   * 这种方式可以避免申请超过实际用于存储Block的额外内存。
   *
   *
   * @return in case of success, the estimated size of the stored data. In case of failure, return
   *         an iterator containing the values of the block. The returned iterator will be backed
   *         by the combination of the partially-unrolled block and the remaining elements of the
   *         original input iterator. The caller must either fully consume this iterator or call
   *         `close()` on it in order to free the storage memory consumed by the partially-unrolled
   *         block.
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far
    // 已经展开的元素数量。
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    // MemoryStore是否仍然有足够的内存，以便于继续展开Block。
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    /**
      * 即unrollMemoryThreshold。
      * 用来展开任何Block之前，初始请求的内存大小，
      * 可以修改属性spark.storage.unrollMemoryThreshold（默认为1MB）改变大小。
      */
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    // 检查内存是否足够的阀值，此值固定为16。即每展开16个元素就检查一次。
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    // 当前任务用于展开Block所保留的内存。
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    // 展开内存不充足时，请求增长的因子。此值固定为1.5。
    val memoryGrowthFactor = 1.5
    // Keep track of unroll memory used by this particular block / putIterator() operation
    // Block已经使用的展开内存大小计数器
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying vector for unrolling the block
    // 用于追踪Block每次迭代的数据。
    var vector = new SizeTrackingVector[T]()(classTag)

    // Request enough memory to begin unrolling
    // 请求足够的内存开始展开操作，默认为unrollMemoryThreshold，即1M；默认On-heap内存
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, MemoryMode.ON_HEAP)

    if (!keepUnrolling) { // 无法请求到足够的初始内存，记录日志
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      // 将申请到的内存添加到已使用的展开内存计数器中
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    // 如果还有元素，且申请到了足够的初始内存
    while (values.hasNext && keepUnrolling) {
      // 将下一个元素添加到vector进行记录
      vector += values.next()
      if (elementsUnrolled % memoryCheckPeriod == 0) { // 判断是否需要检查内存是否足够
        // If our vector's size has exceeded the threshold, request more memory
        val currentSize = vector.estimateSize() // 所有已经分配的内存
        if (currentSize >= memoryThreshold) { // 所有已经分配的内存大于为当前展开保留的内存
          // 计算还需要请求的内存大小
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          // 尝试申请更多内存
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
          if (keepUnrolling) { // 申请成功
            // 将申请到的内存添加到已使用的展开内存计数器中
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          // 更新为当前展开保留的内存大小
          memoryThreshold += amountToRequest
        }
      }
      // 完成了一次元素展开，展开个数加1
      elementsUnrolled += 1
    }

    if (keepUnrolling) { // 走到这里，说明计算的申请内存是足够的
      // We successfully unrolled the entirety of this block
      val arrayValues = vector.toArray
      vector = null
      // 将所有Block数据构造为DeserializedMemoryEntry对象
      val entry =
        new DeserializedMemoryEntry[T](arrayValues, SizeEstimator.estimate(arrayValues), classTag)
      // 最终的数据大小
      val size = entry.size

      // 定义将展开Block的内存转换为存储Block的内存的方法
      def transferUnrollToStorage(amount: Long): Unit = {
        // Synchronize so that transfer is atomic
        memoryManager.synchronized { // 这里是需要加锁的，保证释放后能立即获取
          // 先尝试释放一些展开内存
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, amount)
          // 申请存储内存
          val success = memoryManager.acquireStorageMemory(blockId, amount, MemoryMode.ON_HEAP)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }


      // Acquire storage memory if necessary to store this block in memory.
      val enoughStorageMemory = {
        // 可能需要申请额外的内存
        if (unrollMemoryUsedByThisBlock <= size) { // 如果计算的展开使用内存小于等于实际使用内存，即使用SizeEstimator评估时没有评估到位，还缺一部分内存
          // 需要申请额外的内存
          val acquiredExtra =
            memoryManager.acquireStorageMemory(
              blockId, size - unrollMemoryUsedByThisBlock, MemoryMode.ON_HEAP)
          if (acquiredExtra) { // 申请成功
            // 根据实际使用内存大小将展开Block的内存转换为存储Block的内存
            transferUnrollToStorage(unrollMemoryUsedByThisBlock)
          }
          acquiredExtra
        } else { // unrollMemoryUsedByThisBlock > size，评估申请的内存大于最终评估的大小，需要把多评估的内存释放
          // If this task attempt already owns more unroll memory than is necessary to store the
          // block, then release the extra memory that will not be used.
          // 如果计算的展开使用内存大于实际使用内存大小，则将过剩的内存释放掉
          val excessUnrollMemory = unrollMemoryUsedByThisBlock - size
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, excessUnrollMemory)
          // 根据实际使用内存大小将展开Block的内存转换为存储Block的内存
          transferUnrollToStorage(size)
          true
        }
      }
      if (enoughStorageMemory) { // 如果内存分配足够
        entries.synchronized {
          // 将对应的映射关系添加到entries字典
          entries.put(blockId, entry)
        }
        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(
          blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(size)
      } else { // 内存分配不够
        assert(currentUnrollMemoryForThisTask >= unrollMemoryUsedByThisBlock,
          "released too much unroll memory")
        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = arrayValues.toIterator,
          rest = Iterator.empty))
      }
    } else { // 计算展开使用内存时就无法满足
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(new PartiallyUnrolledIterator(
        this,
        MemoryMode.ON_HEAP,
        unrollMemoryUsedByThisBlock,
        unrolled = vector.iterator,
        rest = values))
    }
  }

  /**
   * Attempt to put the given block in memory store as bytes.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
    *
    * 以序列化后的字节数组方式，将BlockId对应的Block（已经转换为Iterator）写入内存。
   *
   * @return in case of success, the estimated size of the stored data. In case of failure,
   *         return a handle which allows the caller to either finish the serialization by
   *         spilling to disk or to deserialize the partially-serialized block and reconstruct
   *         the original input iterator. The caller must either fully consume this result
   *         iterator or call `discard()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // 根据内存模式获取内存分配器
    val allocator = memoryMode match {
      case MemoryMode.ON_HEAP => ByteBuffer.allocate _ // 堆内存ByteBuffer
      case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _ // 堆外内存DirectBuffer
    }

    // Whether there is still enough memory for us to continue unrolling this block
    // MemoryStore是否仍然有足够的内存，以便于继续展开Block。
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    /**
      * 即unrollMemoryThreshold。
      * 用来展开任何Block之前，初始请求的内存大小
      * 可以修改属性spark.storage.unrollMemoryThreshold（默认为1MB）改变大小。
      */
    val initialMemoryThreshold = unrollMemoryThreshold
    // Keep track of unroll memory used by this particular block / putIterator() operation
    // Block已经使用的展开内存量计数器
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying buffer for unrolling the block
    // 包装为重定向输出流
    val redirectableStream = new RedirectableOutputStream
    val bbos = new ChunkedByteBufferOutputStream(initialMemoryThreshold.toInt, allocator)
    redirectableStream.setOutputStream(bbos)

    // 包装为序列化流
    val serializationStream: SerializationStream = {
      val autoPick = !blockId.isInstanceOf[StreamBlockId]
      val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
      ser.serializeStream(serializerManager.wrapStream(blockId, redirectableStream))
    }

    // Request enough memory to begin unrolling
    // 请求足够的内存开始展开操作，默认为initialMemoryThreshold，即1M
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) { // 无法请求到足够的初始内存，记录日志
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      // 将申请到的内存添加到已使用的展开内存量计数器中
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // 在需要时申请保留额外的展开内存
    def reserveAdditionalMemoryIfNecessary(): Unit = {
      // 构建的ChunkedByteBufferOutputStream大小大于已经保留到的展开内存量
      if (bbos.size > unrollMemoryUsedByThisBlock) {
        // 需要申请保留额外内存
        val amountToRequest = bbos.size - unrollMemoryUsedByThisBlock
        // 尝试申请保留
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          // 申请保留成功，将申请量累加到unrollMemoryUsedByThisBlock中
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }
    }

    // Unroll this block safely, checking whether we have exceeded our threshold
    // 如果还有元素，且申请到了足够的初始内存
    while (values.hasNext && keepUnrolling) {
      // 将数据元素进行序列化，并写入到流中
      serializationStream.writeObject(values.next())(classTag)
      // 在需要时申请保留额外的展开内存
      reserveAdditionalMemoryIfNecessary()
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.
    if (keepUnrolling) { // 申请到了足够的初始内存
      // 将序列化流关闭
      serializationStream.close()
      // 在需要时申请保留额外的展开内存
      reserveAdditionalMemoryIfNecessary()
    }

    if (keepUnrolling) { // 申请到了足够的初始内存
      // 将序列化后的数据赋值给SerializedMemoryEntry对象的buffer字段持有
      val entry = SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
      // Synchronize so that transfer is atomic
      memoryManager.synchronized { // 加锁同步，保证释放和申请是同步的
        // 释放之前保留的内存
        releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
        // 申请对应的内存存储空间，用于正式存储
        val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
        assert(success, "transferring unroll memory to storage memory failed")
      }
      entries.synchronized {
        // 将指定的BlockId与上面创建的SerializedMemoryEntry对象存入entries字典进行记录
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(entry.size),
        Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      // 保存成功，返回
      Right(entry.size)
    } else { // 没有申请到足够的初始内存
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, bbos.size)
      // 返回保留了数据迭代器的PartiallySerializedBlock对象
      Left(
        new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          serializationStream,
          redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          bbos,
          values,
          classTag))
    }
  }

  // 从内存中读取BlockId对应的Block（已经封装为ChunkedByteBuffer）。
  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    // 加锁获取
    val entry = entries.synchronized { entries.get(blockId) }
    // 判断是否获取到
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  // 用于从内存中读取BlockId对应的Block（已经封装为Iterator）。
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    // 加锁获取
    val entry = entries.synchronized { entries.get(blockId) }
    // 判断是否获取到
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  // 从内存中移除BlockId对应的Block
  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    // 加锁移除
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    // 判断移除结果
    if (entry != null) {
      entry match {
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      // 移除后需要从存储内存中释放
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  // 清空MemoryStore
  def clear(): Unit = memoryManager.synchronized {
    // 清空entr字典
    entries.synchronized {
      entries.clear()
    }
    // 清空堆内存中展开内存的映射关系
    onHeapUnrollMemoryMap.clear()
    // 清空堆外内存中展开内存的映射关系
    offHeapUnrollMemoryMap.clear()
    // 从MemoryManager中清空占用的存储内存
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    // 将BlockId转换为RDDBlockId，然后获取RDDBlockId的rddId属性
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to evict blocks to free up a given amount of space to store a particular block.
   * Can fail if either the block is bigger than our memory or it would require replacing
   * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
   * RDDs that don't fit into memory that we want to avoid).
    *
    * 驱逐Block，尝试腾出指定大小的内存空间，以便存储新的Block
   *
   * @param blockId the ID of the block we are freeing space for, if any
    *                要存储的Block的BlockId。
   * @param space the size of this block
    *              需要驱逐Block所腾出的内存大小。
   * @param memoryMode the type of memory to free (on- or off-heap)
    *                   存储Block所需的内存模式。
   * @return the amount of memory (in bytes) freed by eviction
   */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode): Long = {
    // 检查参数
    assert(space > 0)
    // 使用MemoryManager进行操作
    memoryManager.synchronized {
      // 已经释放的内存大小。
      var freedMemory = 0L
      // 将要添加的RDD的RDDBlockId标记。
      val rddToAdd = blockId.flatMap(getRddId)
      // 已经选择的用于驱逐的Block的BlockId的数组。
      val selectedBlocks = new ArrayBuffer[BlockId]

      /**
       * 判断BlockId指定的Block是否可以驱逐，需要满足两个条件：
       * 1. 该Block使用的内存模式与申请的相同，即On-heap内存只可被驱逐返还给On-heap内存池，Off-heap内存只可驱逐返还给Off-heap内存池。
       * 2. BlockId对应的Block不是RDD，或者存入的BlockId与驱逐blockId指向的不是同一个RDD。
       *
       * @param blockId 判断的BlockId
       * @param entry BlockId对应的MemoryEntry
       * @return BlockId指向的Block是否可被驱逐
       */
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }

      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        // 遍历entries字典
        val iterator = entries.entrySet().iterator()
        // 只要空闲空间小于需要的空间，且还有可遍历的Block，就继续遍历
        while (freedMemory < space && iterator.hasNext) {
          // 获取BlockId和对应的MemoryEntry
          val pair = iterator.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          // 判断该Block是否可以驱逐
          if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            // 可以驱逐，先尝试获取写锁
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              // 获取写锁成功，驱逐并释放该Block
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }

      // 丢弃Block
      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        // 获取对应的数据
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }

        // 从内存中驱逐
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        // 检查迁移后的存储系统是否有效
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          // 释放当前TaskAttempt线程获取的被迁移Block的写锁
          blockInfoManager.unlock(blockId)
        } else { // 迁移后的存储系统无效，说明Block从存储体系中彻底移除了
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          // 删除被迁移Block的信息
          blockInfoManager.removeBlock(blockId)
        }
      }

      if (freedMemory >= space) { // 空闲内存大于等于需要的内存
        logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
          s"(${Utils.bytesToString(freedMemory)} bytes)")
        // 遍历所有取值的BlockId
        for (blockId <- selectedBlocks) {
          // 获取对应的MemoryEntry
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            // 驱逐对应的Block
            dropBlock(blockId, entry)
          }
        }
        logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
          s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
        freedMemory
      } else { // 空闲内存小于需要的内存
        // 即便驱逐内存中所有符合条件的Block，腾出的空间也不足以存储blockId对应的Block
        // 记录日志
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        // 由当前TaskAttempt线程释放selectedBlocks中每个BlockId对应的Block的写锁
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
    *
    * 用于为展开TaskAttempt任务给定的Block，保留指定内存模式上指定大小的内存。
   *
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized { // 加锁
      // 尝试申请展开内存
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) { // 申请成功
        // 获取当前上下文中的TaskAttemptId
        val taskAttemptId = currentTaskAttemptId()
        // 区分内存模式
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        // 更新TaskAttemptId与展开内存大小之间的映射关系
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      // 返回操作状态
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
    *
    * 用于释放TaskAttempt任务占用的内存
   */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    // 获取TaskAttemptId
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized { // 加锁
      // 区分内存模式
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) { // 检查是否包含taskAttemptId对应的占用内存大小
        // 计算要释放的内存
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) { // 释放展开内存
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        // 如果TaskAttemptId所对应的TaskAttempt任务所占用的unroll内存为0，则将其从记录中移除
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
    *
    * 用于展开数据块所使用的内存大小，
    * 是onHeapUnrollMemoryMap中的所有用于展开Block所占用的内存大小与
    * offHeap-UnrollMemoryMap中的所有用于展开Block所占用的内存大小之和。
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
    * 当前的TaskAttempt线程用于展开Block所占用的内存。
    * 即onHeapUnrollMemoryMap中缓存的当前TaskAttempt线程对应的占用大小与
    * offHeapUnrollMemoryMap中缓存的当前的TaskAttempt线程对应的占用大小之和。
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    // onHeapUnrollMemoryMap中缓存的当前TaskAttempt线程对应的占用大小
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      // offHeapUnrollMemoryMap中缓存的当前的TaskAttempt线程对应的占用大小
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
    * 当前使用MemoryStore展开Block的任务的数量。
    * 其实质为onHeapUnrollMemoryMap的键集合与offHeapUnrollMemoryMap的键集合的并集。
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore  the memoryStore, used for freeing memory.
 * @param memoryMode   the memory mode (on- or off-heap).
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled     an iterator for the partially-unrolled values.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore the MemoryStore, used for freeing memory.
 * @param serializerManager the SerializerManager, used for deserializing values.
 * @param blockId the block id.
 * @param serializationStream a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream an OutputStream which can be redirected to a different sink.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode whether the unroll memory is on- or off-heap
 * @param bbos byte buffer output stream containing the partially-serialized values.
 *                     [[redirectableOutputStream]] initially points to this output stream.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 * @param classTag the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values
   * and then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized
   * values and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
  }
}
