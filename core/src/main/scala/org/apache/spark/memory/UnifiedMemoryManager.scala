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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
  *
  * UnifiedMemoryManager在MemoryManager的内存模型之上，
  * 将执行内存和存储内存之间的边界修改为“软”边界，即任何一方可以向另一方借用空闲的内存。
  *
  * @param maxHeapMemory 最大堆内存。大小为系统可用内存与spark.memory.fraction属性值（默认为0.6）的乘积。
  * @param onHeapStorageRegionSize Size of the storage region, in bytes.
  *                          This region is not statically reserved; execution can borrow from
  *                          it if necessary. Cached blocks can be evicted only if actual
  *                          storage memory usage exceeds this region.
  *                          用于存储的堆内存大小。
  * @param numCores CPU内核数，该值会影响计算的内存页大小
  */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  // 检查不可变性
  private def assertInvariants(): Unit = {
    // 堆内存用于执行和存储的总内存大小不能发生改变
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    // 堆外内存用于执行和存储的总内存大小不能发生改变
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  // 返回用于存储的最大堆内存。
  override def maxOnHeapStorageMemory: Long = synchronized {
    // 总堆内存 - 用于计算操作的堆内存
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  // 返回用于存储的最大堆外内存。
  override def maxOffHeapStorageMemory: Long = synchronized {
    // 总对外内存 - 用于计算操作的堆外内存
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
    *
    * 获取执行内存
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    // 检查不可变性
    assertInvariants()
    // 检查申请的内存大小
    assert(numBytes >= 0)
    /**
      * 根据内存模式获取UnifiedMemoryManager中管理的堆上或堆外的
      * 执行内存池（executionPool）、存储内存池（storagePool）、
      * 存储区域大小（storageRegionSize）、内存最大值（maxMemory）。
      */
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
        // 堆内存
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
        // 堆外内存
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
      *
      * 此函数用于借用或收回存储内存。
      *
      * 如果存储内存池的空闲空间大于存储内存池从执行内存池借用的空间大小，
      * 那么除了回收被借用的空间外，还会向存储池再借用一些空间；
      * 如果存储池的空闲空间小于等于存储池从执行池借用的空间大小，那么只需要回收被借用的空间。
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        // 可从存储内存池借用的内存大小
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        // 大于0，说明可以从存储区域借用内存
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          // 仅借用必要的满足需求的内存大小
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          // 将借用的内存大小从存储内存池缩减
          storagePool.decrementPoolSize(spaceToReclaim)
          // 将借用的内存大小添加到执行内存池
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
      *
      * 计算最大的执行内存池时，如果存储区域的边界大小大于已经被存储使用的内存，
      * 那么执行内存的最大空间可以跨越存储内存与执行内存之间的“软”边界；
      * 如果存储区域的边界大小小于等于已经被存储使用的内存，
      * 这说明存储内存已经跨越了存储内存与执行内存之间的“软”边界，
      * 执行内存可以收回被存储内存借用的空间。
     */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    // 调用ExecutionMemoryPool的acquireMemory()方法，给taskAttemptId对应的TaskAttempt获取指定大小的内存。
    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)
  }

  // 为存储BlockId对应的Block，从堆内存或堆外内存获取所需大小的内存。
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized { // 加锁
    // 检查不可变性
    assertInvariants()
    // 检查申请的内存大小
    assert(numBytes >= 0)
    // 根据内存模式获取执行内存池、存储内存池和可以用于存储的最大空间
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => ( // 堆内存
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory) // 注意，此处最大可用内存刨去了执行内存，
                                // 也即是说，在处理存储内存申请时，直接先将执行内存的部分排除在外
      case MemoryMode.OFF_HEAP => ( // 堆外内存
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapMemory)
    }

    // 如果申请的用于存储的内存大于可用于存储的（经过协调之后的）最大空间，则返回false，表示获取失败
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }

    // 如果申请的用于存储的内存大于存储内存池的可用空间，则需要去执行内存池中收回之前借出的空间
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // 计算从执行内存池中借来的空间
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes)
      // 从执行内存池中减去借走的空间
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      // 将借来的空间加到存储内存池中
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    // 使用存储内存池分配空间
    storagePool.acquireMemory(blockId, numBytes)
  }

  // 为展开BlockId对应的Block，从堆内存或堆外内存获取所需大小的内存。
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    // 获取可用的最大堆内存
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      // 用于存储的堆内存大小，默认为 可用的最大堆内存 * 0.5
      onHeapStorageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
    * 获取统一内存管理器可用的最大内存
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    /**
     * 系统可用的最大内存，通过spark.testing.memory配置，未配置的话则取运行时环境的最大内存（即Executor内存），
     * spark.testing.memory是测试环境参数，生产环境会忽略该参数，因此会取spark.executor.memory参数指定的Executor内存。
     */
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    /**
      * 获取系统保留内存大小，通过spark.testing.reservedMemory配置获取，
      * 如果没有指定，判断是否配置了spark.testing，如果配置了则为0，否则默认为300 * 1024 * 1024，即300MB；
      * spark.testing.reservedMemory和spark.testing都是测试环境参数，生产环境会忽略该参数，因此会取默认的300MB。
      */
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)

    // 最小的系统内存阈值，默认为450MB
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong

    // 检查系统可用的最大内存是否小于最小系统内存阈值，如果小于则抛出异常
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    // 判断是否有spark.executor.memory配置
    if (conf.contains("spark.executor.memory")) {
      // 获取spark.executor.memory配置，即Execution使用的内存大小
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      // 判断Execution使用的内存大小是否小于最小系统内存阈值，如果小于则抛出异常
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }

    // 可用内存 = 系统可用的最大内存 - 系统保留内存
    val usableMemory = systemMemory - reservedMemory

    // 可用内存占比
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)

    // 最终统一内存管理器的可用内存大小 = 可用内存 * 可用内存占比
    (usableMemory * memoryFraction).toLong
  }
}
