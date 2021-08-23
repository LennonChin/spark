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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 *
 * ShuffleMapTask执行后返回的结果，包含该Task运行的Executor上的BlockManager的地址信息，以及针对每个Reducer任务的输出数据大小。
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run.
    * 用于返回ShuffleMapTask运行的位置，即所在节点的BlockManager的身份标识BlockManagerId
    **/
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
    *
    * 用于返回reduce任务需要拉取的Block的大小（单位为字节）。
   */
  def getSizeForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {

    /**
      * 根据uncompressedSizes的长度是否大于2000，分别创建HighlyCompressedMapStatus和CompressedMapStatus：
      * - 对于较大的数据量使用高度压缩的HighlyCompressedMapStatus。
      * - 一般的数据量则使用CompressedMapStatus。
      */
    if (uncompressedSizes.length > 2000) {
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      /**
       * LOG_BASE为1.1，compressedSize & 0xFF用于截取低8位
       * 1.1的compressedSize指数倍
       */
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * 经过数据块信息经过压缩的MapStatus实例
 *
 * @param loc location where the task is being executed.
 *            Task所执行的位置，BlockManagerId类型
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 *                        Reduce任务所需要拉取的数据块大小，它是一个数组
 *                        数组的索引标识Reduce任务的ID
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {

  // 构造方法
  protected def this() = this(null, null.asInstanceOf[Array[Byte]])  // For deserialization only

  // 构造方法
  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  // 获取ShuffleMapTask运行的位置，即所在节点的BlockManager的身份标识BlockManagerId
  override def location: BlockManagerId = loc

  // 获取reduce任务需要拉取的Block的大小（单位为字节）
  override def getSizeForBlock(reduceId: Int): Long = {
    // 需要从compressedSizes数组中按照reduceId获取，并进行解压
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  // 序列化写出操作
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  // 反序列化读入操作
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that only stores the average size of non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 *            Task所执行的位置，BlockManagerId类型
 * @param numNonEmptyBlocks the number of non-empty blocks
 *                          非空数据块的数量
 * @param emptyBlocks a bitmap tracking which blocks are empty
 *                    用于记录需要拉取数据为空的Reduce的reduceId
 * @param avgSize average size of the non-empty blocks
 *                非空数据块的平均大小
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  // 构造方法
  protected def this() = this(null, -1, null, -1)  // For deserialization only

  // 获取ShuffleMapTask运行的位置，即所在节点的BlockManager的身份标识BlockManagerId
  override def location: BlockManagerId = loc

  // 获取reduce任务需要拉取的Block的大小（单位为字节）
  override def getSizeForBlock(reduceId: Int): Long = {
    // 如果emptyBlocks中包含指定的reduceId，说明对应的数据块为空
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      // 否则返回非空数据块的平均大小
      avgSize
    }
  }

  // 序列化写方法
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)
  }

  // 反序列化读方法
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    // 用于非空数据块数量
    var numNonEmptyBlocks: Int = 0
    // 用于记录累计数据量
    var totalSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    // 创建Bitmap，用于记录需要拉取数据为空的Reduce的reduceId
    val emptyBlocks = new RoaringBitmap()
    // 未经压缩的数据块数量
    val totalNumBlocks = uncompressedSizes.length
    while (i < totalNumBlocks) {
      // 获取需要拉取的数据块的大小
      var size = uncompressedSizes(i)
      if (size > 0) { // 数据块大小大于0
        // 自增非空数据块数量
        numNonEmptyBlocks += 1
        // 累计数据量
        totalSize += size
      } else { // 数据块大小为0
        // 将i添加到Bitmap中
        emptyBlocks.add(i)
      }
      i += 1
    }

    // 计算平均大小
    val avgSize = if (numNonEmptyBlocks > 0) {
      totalSize / numNonEmptyBlocks
    } else {
      0
    }

    // 构造HighlyCompressedMapStatus实例
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize)
  }
}
