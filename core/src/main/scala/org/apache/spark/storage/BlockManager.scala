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

package org.apache.spark.storage

import java.io._
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer


/** Class for returning a fetched block and associated metrics.
  * BlockResult用于封装从本地的BlockManager中获取的Block数据及与Block相关联的度量数据。
  *
  * @param data Block及与Block相关联的度量数据。
  * @param readMethod 读取Block的方法。readMethod采用枚举类型DataReadMethod提供的Memory、Disk、Hadoop、Network四个枚举值。
  * @param bytes 读取的Block的字节长度。
  */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
  *
  * BlockManager运行在每个节点上（包括Driver和Executor），提供对本地或远端节点上的内存、磁盘及堆外内存中Block的管理。
  * 存储体系从狭义上来说指的就是BlockManager，从广义上来说，包括整个Spark集群中的：
  * 各个BlockManager、BlockInfoManager、DiskBlockManager、DiskStore、MemoryManager、MemoryStore、
  * 对集群中的所有BlockManager进行管理的BlockManagerMaster及各个节点上对外提供Block上传与下载服务的Block TransferService。
 */
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    val serializerManager: SerializerManager,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with BlockEvictionHandler with Logging {

  // 是否开启外部Shuffle服务，默认不开启
  private[spark] val externalShuffleServiceEnabled =
    conf.getBoolean("spark.shuffle.service.enabled", false)

  // 创建DiskBlockManager
  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    // 在没有开启外部Shuffle服务，且是Driver的情况下，需要在停止时清理文件
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    // 构造DiskBlockManager实例
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing
  // 创建BlockInfoManager
  private[storage] val blockInfoManager = new BlockInfoManager

  // 创建用于执行Future的线程池，线程前缀为block-manager-future，线程池大小默认为128
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  // 创建MemoryStore
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)

  // 创建DiskStore
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager)

  // 将MemoryStore设置给MemoryManager
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  // 获取内存最大存储大小，堆内存 + 堆外内存
  private val maxMemory =
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  // 获取外部Shuffle服务的端口
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  // 当前BlockManager的BlockManagerId
  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  // 用于标识提供Shuffle服务时的Shuffle服务ID
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  /**
    * 创建ShuffleClient客户端.
    * 如果部署了外部的Shuffle服务，则需要配置spark.shuffle.service.enabled属性为true（默认是false），
    * 此时将创建ExternalShuffleClient。
    * 默认情况下，NettyBlockTransferService会作为Shuffle的客户端。
    */
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
      securityManager.isSaslEncryptionEnabled())
  } else {
    blockTransferService
  }

  // Max number of failures before this block manager refreshes the block locations from the driver
  // 向Driver进行刷新数据块位置操作的最大错误次数
  private val maxFailuresBeforeLocationRefresh =
    conf.getInt("spark.block.failures.beforeLocationRefresh", 5)

  // 此BlockManager的BlockManagerSlaveEndpoint
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
    *
    * 初始化方法。只有在该方法被调用后BlockManager才能发挥作用。
   */
  def initialize(appId: String): Unit = {
    // 初始化BlockTransferService
    blockTransferService.init(this)
    // 初始化Shuffle客户端
    shuffleClient.init(appId)

    // 设置数据块的副本复制策略
    blockReplicationPolicy = {
      // 默认为RandomBlockReplicationPolicy
      val priorityClass = conf.get(
        "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    /**
      * 生成当前BlockManager的BlockManagerId。
      * 此处创建的BlockManagerId实际只是在向BlockManagerMaster注册BlockManager时，给BlockManagerMaster提供参考，
      * BlockManagerMaster将会创建一个包含了拓扑信息的新BlockManagerId作为正式分配给BlockManager的身份标识。
      */
    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

    /**
      * 向BlockManagerMaster注册当前BlockManager，传递的参数有：
      * 1. BlockManagerId；
      * 2. 当前BlockManager管理的最大内存
      * 3. 当前BlockManager的BlockManagerSlaveEndpoint端点
      */
    val idFromMaster = master.registerBlockManager(
      id,
      maxMemory,
      slaveEndpoint)

    // 根据注册返回的ID重置blockManagerId
    blockManagerId = if (idFromMaster != null) idFromMaster else id

    /**
      * 生成shuffleServerId。
      * 当启用了外部Shuffle服务时将新建一个BlockManagerId作为shuffleServerId，
      * 由spark.shuffle.service.enabled参数配置，默认为false；
      * 否则是BlockManager自身的BlockManagerId。
      */
    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    // Register Executors' configuration with the local shuffle service, if one should exist.
    // 当启用了外部Shuffle服务，并且当前BlockManager所在节点不是Driver时，需要注册外部的Shuffle服务。
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    logInfo(s"Initialized BlockManager: $blockManagerId")
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = 3
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    // 遍历BlockInfoManager管理的所有BlockId与BlockInfo的映射
    for ((blockId, info) <- blockInfoManager.entries) {
      // 获取Block的状态信息
      val status = getCurrentBlockStatus(blockId, info)
      // 如果需要将Block的BlockStatus汇报给BlockManagerMaster，则调用tryToReportBlockStatus()方法汇报
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
    *
    * 向BlockManagerMaster重新注册BlockManager，并向BlockManagerMaster报告所有的Block信息。
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    // 向BlockManagerMaster重新注册BlockManager
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)
    // 报告所有的Block信息
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized { // 加锁
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          // 异步注册BlockManager
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        Await.ready(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
    *
    * 获取本地Block的数据
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) { // 当前Block是ShuffleBlock
      // 使用ShuffleManager的ShuffleBlockResolver组件的getBlockData()方法获取Block数据
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else { // 当前Block不是ShuffleBlock
      // 调用getLocalBytes()获取Block数据
      getLocalBytes(blockId) match {
        // 能够获取到，封装为BlockManagerManagedBuffer
        case Some(buffer) => new BlockManagerManagedBuffer(blockInfoManager, blockId, buffer)
        case None => // 无法获取
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          // 调用reportBlockStatus方法告诉BlockManagerMaster此Block不存在
          reportBlockStatus(blockId, BlockStatus.empty)
          // 抛出BlockNotFoundException异常
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
    *
    * 将Block数据写入本地
   */
  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean = {
    // 调用putBytes()方法实现
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing.
    *
    * 获取Block的状态
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    // 使用BlockManager进行获取
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
    *
    * 获取匹配过滤器条件的BlockId的序列
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    /**
      * 从BlockInfoManager的entries缓存中获取BlockId外，还需要从DiskBlockManager中获取。
      * 这是因为DiskBlockManager中可能存在BlockInfoManager不知道的Block。
      */
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    // 向BlockManagerMaster汇报BlockStatus
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) { // 需要重新向BlockManagerMaster注册当前BlockManager
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      // 向BlockManagerMaster异步注册BlockManager
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    // 更新BlockInfo
    master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      // 匹配存储级别
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          // 检查内存存储
          val inMem = level.useMemory && memoryStore.contains(blockId)
          // 检查磁盘存储
          val onDisk = level.useDisk && diskStore.contains(blockId)
          // 检查序列化
          val deserialized = if (inMem) level.deserialized else false
          // 检查副本数
          val replication = if (inMem  || onDisk) level.replication else 1
          // 构造存储级别对象
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          // 获取占用的内存大小
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          // 获取占用的磁盘大小
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          // 构造BlockStatus对象
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Cleanup code run in response to a failed local read.
   * Must be called while holding a read lock on the block.
   */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
   * Get block from local block manager as an iterator of Java objects.
    *
    * 从本地的BlockManager中获取Block数据
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    // 获取BlockId所对应的读锁
    blockInfoManager.lockForReading(blockId) match {
      case None => // 无法获取
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) => // 返回的是BlockInfo对象
        // 获取存储级别
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        if (level.useMemory && memoryStore.contains(blockId)) { // 优先考虑内存存储
          val iter: Iterator[Any] = if (level.deserialized) { // 不需要反序列化
            memoryStore.getValues(blockId).get // 从MemoryStore中获取
          } else { // 需要反序列化
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iter, releaseLock(blockId))
          // 返回BlockResult结果
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) { // 从磁盘读取
          val iterToReturn: Iterator[Any] = {
            // 从磁盘获取数据
            val diskBytes = diskStore.getBytes(blockId)
            if (level.deserialized) { // 不需要反序列化
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskBytes.toInputStream(dispose = true))(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else { // 需要反序列化
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskBytes)
                .map {_.toInputStream(dispose = false)}
                .getOrElse { diskBytes.toInputStream(dispose = true) }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, releaseLock(blockId))
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
    * 从存储体系获取BlockId所对应Block的数据，并封装为ChunkedByteBuffer后返回
   */
  def getLocalBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) { // 是ShuffleBlock
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      Option(
        // 封装为ChunkedByteBuffer对象
        new ChunkedByteBuffer(
          // 使用ShuffleManager的ShuffleBlockResolver组件获取Block数据
          shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer()))
    } else { // 不是ShuffleBlock
      // 获取Block的读锁，然后使用BlockInfo的doGetLocalBytes()方法获取数据
      blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): ChunkedByteBuffer = {
    // 获取存储级别
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) { // Block没有被序列化，按照DiskStore、MemoryStore的顺序获取Block数据
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) { // DiskStore
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId) // 获取数据
      } else if (level.useMemory && memoryStore.contains(blockId)) { // MemoryStore
        // The block was not found on disk, so serialize an in-memory copy:
        serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag) // 获取数据
      } else { // 没有找到对应的存储数据，释放锁，并移除对应的Block
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized Block被序列化了，按照MemoryStore、DiskStore的顺序获取Block数据
      if (level.useMemory && memoryStore.contains(blockId)) { // MemoryStore
        memoryStore.getBytes(blockId).get // 获取数据
      } else if (level.useDisk && diskStore.contains(blockId)) { // DiskStore
        val diskBytes = diskStore.getBytes(blockId) // 获取数据
        maybeCacheDiskBytesInMemory(info, blockId, level, diskBytes).getOrElse(diskBytes)
      } else { // 没有找到对应的存储数据，释放锁，并移除对应的Block
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   */
  private def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBytes(blockId).map { data =>
      val values =
        serializerManager.dataDeserializeStream(blockId, data.toInputStream(dispose = true))(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    }
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host.
   */
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    // 使用BlockManagerMaster的getLocations()进行获取并对位置进行"洗牌"避免热点
    val locs = Random.shuffle(master.getLocations(blockId))
    // 将位置分为"优先位置"和"其他位置"，"优先位置"表示对应的块在BlockManager节点上
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    // 返回位置集合，"优先位置"在前面
    preferredLocs ++ otherLocs
  }

  /**
   * Get block from remote block managers as serialized bytes.
    * 从远程的BlockManager中获取数据块
   */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    var runningFailureCount = 0
    var totalFailureCount = 0
    // 根据blockId获取远程位置集合，即BlockManagerId集合
    val locations = getLocations(blockId)
    // 最大可重试次数
    val maxFetchFailures = locations.size
    // 遍历Block块的位置集合
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        // 尝试获取
        blockTransferService.fetchBlockSync(
          loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()
      } catch {
        case NonFatal(e) =>
          // 检查重试次数是否过多
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          /**
            * 如果重试失败次数大于spark.block.failures.beforeLocationRefresh参数指定的次数，默认5次
            * 说明可能对应的数据位置发生了改变，需要重新刷新一下数据位置
            */
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = getLocations(blockId).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          null
      }

      if (data != null) { // 获取成功，将数据包装为ChunkedByteBuffer对象后返回
        return Some(new ChunkedByteBuffer(data))
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
    *
    * 优先从本地获取Block数据，当本地获取不到所需的Block数据，再从远端获取Block数据
   */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    // 先从本地读取
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    // 再从远端读取
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
    *
    * 将当前线程持有的Block的写锁降级为读锁
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block.
    *
    * 释放当前线程对持有的Block的锁
   */
  def releaseLock(blockId: BlockId): Unit = {
    blockInfoManager.unlock(blockId)
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
    *
    * 将TaskAttempt线程注册到BlockInfoManager
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
    *
    * 对指定TaskAttempt线程持有的所有Block的锁进行释放
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
    *
    * 获取Block。如果Block存在，则获取此Block并返回BlockResult，
    * 否则调用makeIterator()方法计算Block，并持久化后返回BlockResult或Iterator。
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    // 从本地或远端的BlockManager获取Block
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None => // Block已经成功存储到内存
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) => // Block存储到内存时发生了错误
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
       Right(iter)
    }
  }

  /**
    * 将Block数据写入存储体系
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    // 调用doPutIterator()方法实现
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
    *
    * 用于创建并获取DiskBlockObjectWriter，
    * 通过DiskBlockObjectWriter可以跳过对DiskStore的使用，直接将数据写入磁盘
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    // 属性spark.shuffle.sync决定DiskBlockObjectWriter把数据写入磁盘时是采用同步方式还是异步方式，默认是异步方式。
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    // 调用doPutBytes()方法写入Block数据
    doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster)
  }

  /**
   * Put the given bytes according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return true if the block was already present or if the put succeeded, false otherwise.
   */
  private def doPutBytes[T](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Boolean = {
    // 调用doPut()方法写入
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      // 开始时间
      val startTimeMs = System.currentTimeMillis
      // Since we're storing bytes, initiate the replication before storing them locally.
      // This is faster as data is already serialized and ready to send.
      val replicationFuture = if (level.replication > 1) { // 副本数大于1，说明需要创建副本
        // 创建异步线程通过调用replicate()方法复制Block数据到其他节点的存储体系中
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          // 使用replicate()方法进行副本创建
          replicate(blockId, bytes, level, classTag)
        }(futureExecutionContext)
      } else {
        null
      }

      val size = bytes.size

      if (level.useMemory) { // 优先写入内存
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        val putSucceeded = if (level.deserialized) { // 没有指定序列化
          // 使用SerializerManager的dataDeserializeStream()方法将数据反序列化为迭代器
          val values =
            serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
          // 使用MemoryStore的putIteratorAsValues()方法进行写入
          memoryStore.putIteratorAsValues(blockId, values, classTag) match {
            case Right(_) => true
            case Left(iter) =>
              // If putting deserialized values in memory failed, we will put the bytes directly to
              // disk, so we don't need this iterator and can close it to free resources earlier.
              iter.close()
              false
          }
        } else { // 指定了序列化
          // 使用MemoryStore的putBytes()方法写入
          memoryStore.putBytes(blockId, size, level.memoryMode, () => bytes)
        }
        if (!putSucceeded && level.useDisk) { // 内存写入失败，如果开启了磁盘持久化，则尝试写入磁盘
          logWarning(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
      } else if (level.useDisk) { // 不能使用内存时，写入磁盘
        diskStore.putBytes(blockId, bytes)
      }

      // 写入完成后，获取该Block块的状态
      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      // 检查是否写入成功，通过BlockInfo中的存储级别的isValid()方法判断
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) { // 写入成功
        // Now that the block is in either the memory or disk store,
        // tell the master about it.
        // 更新BlockInfo中记录的size
        info.size = size
        // 必要时向BlockManagerMaster汇报BlockStatus
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        // 维护度量信息
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
      }
      logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
      if (level.replication > 1) { // 副本数大于1
        // Wait for asynchronous replication to finish
        try {
          // 等待副本写入完成，无限期等待
          Await.ready(replicationFuture, Duration.Inf)
        } catch {
          case NonFatal(t) =>
            throw new Exception("Error occurred while waiting for replication to finish", t)
        }
      }
      // 返回结果
      if (blockWasSuccessfullyStored) {
        None
      } else {
        Some(bytes)
      }
    }.isEmpty
  }

  /**
   * Helper method used to abstract common code from [[doPutBytes()]] and [[doPutIterator()]].
    *
    * 用于数据块数据的写入
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    // 检查参数
    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")


    val putBlockInfo = {
      // 构造BlockInfo对象
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) { // 尝试获取写锁
        // 走到这里说明获取写锁成功，直接返回添加的BlockInfo
        newInfo
      } else {
        // 走到这里说明获取写锁失败，可能是有其它的线程先创建了，此时lockNewBlockForWriting()会获取读锁
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) { // 如果不保留读锁，需要将其释放
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        // 直接返回
        return None
      }
    }

    // 记录开始时间
    val startTimeMs = System.currentTimeMillis
    // 记录是否抛出了异常
    var exceptionWasThrown: Boolean = true

    // 走到此处说明创建新的BlockInfo成功，且获取到了写锁，将尝试执行数据块写入
    val result: Option[T] = try {
      // 执行Block写入，返回值如果是None，说明写入成功，否则会携带其它信息
      val res = putBody(putBlockInfo)  // putBody是传入的柯里化回调函数
      exceptionWasThrown = false
      if (res.isEmpty) {
        // Block成功存储，执行锁降级或释放锁
        // the block was successfully stored
        if (keepReadLock) { // 需要保留读锁
          // 锁降级
          blockInfoManager.downgradeLock(blockId)
        } else { // 不需要保留读锁，直接释放锁
          blockInfoManager.unlock(blockId)
        }
      } else { // 数据块存储失败，移除此数据块
        // 移除数据块，但不告知BlockManagerMasterEndpoint
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } finally {
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        logWarning(s"Putting block $blockId failed due to an exception")
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        // 如果抛出了异常，需要移除此数据块，根据tellMaster决定是否告知BlockManagerMasterEndpoint
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        // 更新度量信息
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    // 记录持久化副本数的相关日志
    if (level.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
    *         当Block成功存储后会返回None，否则返回存储失败的迭代器
   */
  private def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {

    // 执行Block的写入
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) { // 使用内存
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        if (level.deserialized) { // 不需要序列化
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) => // 写入成功
              size = s
            case Left(iter) => // 没有足够的内存写入
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) { // 可以使用硬盘
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { fileOutputStream =>
                  serializerManager.dataSerializeStream(blockId, fileOutputStream, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else { // 不能使用硬盘，记录写入出错的结果
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized 需要序列化
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) => // 写入成功
              size = s
            case Left(partiallySerializedValues) => // 写入失败
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) { //  可以使用硬盘
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { fileOutputStream =>
                  partiallySerializedValues.finishWritingToStream(fileOutputStream)
                }
                size = diskStore.getSize(blockId)
              } else { // 不能使用硬盘，记录写入出错的结果
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) { // 使用硬盘
        diskStore.put(blockId) { fileOutputStream =>
          serializerManager.dataSerializeStream(blockId, fileOutputStream, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      // 获取BlockId对应的BlockStatus对象
      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      // 判断是否写入成功
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) { // 写入成功
        // Now that the block is in either the memory or disk store, tell the master about it.
        info.size = size
        // 如果有需要就通知给BlockManagerMaster
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        // 更新度量信息
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
        if (level.replication > 1) { // 副本数要求大于1
          val remoteStartTime = System.currentTimeMillis
          // 获取本地数据用于创建副本
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            // 创建副本
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
   * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
   *         If this returns bytes from the memory store then the original disk store bytes will
   *         automatically be disposed and the caller should not continue to use them. Otherwise,
   *         if this returns None then the original disk store bytes will be unaffected.
   */
  private def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskBytes: ChunkedByteBuffer): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskBytes.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskBytes.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskBytes.copy(allocator)
          })
          if (putSucceeded) {
            diskBytes.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
   * subsequent reads. This method requires the caller to hold a read lock on the block.
   *
   * @return a copy of the iterator. The original iterator passed this method should no longer
   *         be used after this method returns.
   */
  private def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
   * Get peer block managers in the system.
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Replicate block to another node. Note that this is a blocking call that returns after
   * the block has been replicated.
    *
    * 进行持久化副本的复制
   */
  private def replicate(
      blockId: BlockId,
      data: ChunkedByteBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {

    // 最大容忍的失败副本数量，默认为1
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)

    // 构造存储级别，这里的存储级别中的副本数都是1
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)

    // 需要复制的份数，原来已经存了一份了，因此-1
    val numPeersToReplicateTo = level.replication - 1

    // 开始时间
    val startTime = System.nanoTime

    // 副本所在的BlockManager的标识
    var peersReplicatedTo = mutable.HashSet.empty[BlockManagerId]
    // 失败副本所在的BlockManager的标识
    var peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]

    // 失败次数
    var numFailures = 0

    /**
      * 使用副本复制策略器获取存放副本的BlockManager的BlockManagerId标识集合，
      * 该策略器通过spark.storage.replication.policy配置，
      * 默认是RandomBlockReplicationPolicy，
      * 该策略尽可能的保证选择的是不同的BlockManager
      */
    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      getPeers(false), // 从哪些BlockManager中选择
      mutable.HashSet.empty,
      blockId,
      numPeersToReplicateTo) // 需要选择的BlockManager的个数

    /**
      * 三个条件，就进行循环：
      * 1. 失败次数小于最大可容许的失败次数；
      * 2. 还有待进行副本复制的BlockManager；
      * 3. 已经复制的副本数量与要求的不一致。
      */
    while(numFailures <= maxReplicationFailures &&
        !peersForReplication.isEmpty &&
        peersReplicatedTo.size != numPeersToReplicateTo) {
      // 得到peersForReplication中的头一个BlockManagerId
      val peer = peersForReplication.head
      try {
        // 记录开始时间
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        // 使用BlockTransferService同步上传数据到该BlockManagerId对应的BlockManager上
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          new NettyManagedBuffer(data.toNetty),
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        // 上传成功，移除已经上传过的头一个BlockManagerId
        peersForReplication = peersForReplication.tail
        // 将已经上传过的BlockManagerId添加到peersReplicatedTo集合
        peersReplicatedTo += peer
      } catch { // 出现异常
        case NonFatal(e) => // 非致命异常
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          // 记录上传失败的BlockManager的BlockManagerId
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          /**
            * 重新获取除当前BlockManager以外的所有BlockManager的BlockManagerId集合
            * 并将刚刚上传失败的BlockManager的BlockManagerId过滤掉
            */
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          // 失败次数自增
          numFailures += 1

          /**
            * 重新使用副本复制策略器获取存放副本的BlockManager的BlockManagerId标识集合，
            * 注意，这一次选择的范围会过滤掉已经失败的BlockManager，
            * 同时需要的BlockManager的数量会减去已经成功的副本数量
            */
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers, // 此次选择的范围会过滤掉已经失败的BlockManager
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size) // 需要的BlockManager的数量会减去已经成功的副本数量
      }
    }

    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
  }

  /**
   * Read a block consisting of a single object.
    *
    * 获取由单个对象组成的Block
   */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
   * Write a block consisting of a single object.
    *
    * 将由单个对象组成的Block写入存储体系
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * 从内存中删除Block，当Block的存储级别允许写入磁盘，Block将被写入磁盘。
   * 此方法主要在内存不足，需要从内存腾出空闲空间时使用。
   *
   * @param blockId 被驱逐的数据块的BlockId
   * @param data 被驱逐的数据块的数据
   * @tparam T 被驱逐数据块的数据类型，从前面写入数据的流程可知，
   *           写入的数据可能是ChunkedByteBuffer缓冲区，也有可能是数组类型的集合
   * @return the block's new effective StorageLevel.
   *         数据块被驱逐之后新的持久化级别
   */
  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    // 确认当前TaskAttempt线程是否已经持有BlockId对应的写锁
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // Drop to disk, if storage level requires
    // 如果定义了磁盘存储级别，则将Block写入磁盘
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { fileOutputStream =>
            serializerManager.dataSerializeStream(
              blockId,
              fileOutputStream,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // Actually drop from memory store
    // 将内存中的Block删除
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    // 获取Block状态
    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      // 向BlockManagerMaster报告Block状态
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      // 当Block写入了磁盘或Block从内存中删除，更新任务度量信息
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    // 返回Block的存储级别
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
    *
    * 移除属于指定RDD的所有Block
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    // 从BlockInfoManager的entries中找出所有的RDDBlockId，并过滤出其rddId属性等于指定rddId的所有RDDBlockId
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    // 调用removeBlock方法删除过滤出来的所有RDDBlockId
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    // 返回移除的Block数量
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
    *
    * 移除属于指定Broadcast的所有Block
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    /**
      * 从BlockInfoManager的entries中找出所有的BroadcastBlockId，
      * 并过滤出其broadcastId属性等于指定broadcastId的所有BroadcastBlockId
      */
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    // 调用removeBlock方法删除过滤出来的所有BroadcastBlockId
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    // 返回移除的Block数量
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
    *
    * 删除指定的Block
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    // 获取写锁
    blockInfoManager.lockForWriting(blockId) match {
      case None => // 未获取到写锁
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        // 从存储体系中删除Block
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
   * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
   * lock on the block.
   */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    // 从MemoryStore中移除Block
    val removedFromMemory = memoryStore.remove(blockId)
    // 从DiskStore中移除Block
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as it was not found on disk or in memory")
    }
    // 从BlockInfoManager中移除Block对应的BlockInfo
    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      // 如果需要向BlockManagerMaster汇报Block状态，则调用reportBlockStatus()方法汇报
      reportBlockStatus(blockId, BlockStatus.empty)
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    Option(TaskContext.get()).foreach { c =>
      c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
    }
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }
}
