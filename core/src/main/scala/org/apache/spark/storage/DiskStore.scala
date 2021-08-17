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

import java.io.{FileOutputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import com.google.common.io.Closeables

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Stores BlockManager blocks on disk.
  *
  * 磁盘存储。依赖于DiskBlockManager，负责对Block的磁盘存储。
  *
  * @param conf SparkConf对象
  * @param diskManager 磁盘Block管理器DiskBlockManager对象
  */
private[spark] class DiskStore(conf: SparkConf, diskManager: DiskBlockManager) extends Logging {

  /**
    * 读取磁盘中的Block时，是直接读取还是使用FileChannel的内存镜像映射方法读取的阈值。
    * 当文件大小小于该阈值，就直接读取，否则使用FileChannel的方式读取。
    * 由spark.storage.memoryMapThreshold配置，默认为2M
    */
  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  // 用于获取给定BlockId所对应Block的大小
  def getSize(blockId: BlockId): Long = {
    // 使用DiskBlockManager的getFile()方法获取到对应的文件
    diskManager.getFile(blockId.name).length
  }

  /**
   * Invokes the provided callback function to write the specific block.
    *
    * 用于将BlockId所对应的Block写入磁盘
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: FileOutputStream => Unit): Unit = {
    // 判断是否包含了该BlockId对应的文件
    if (contains(blockId)) {
      // 如果包含就抛出异常
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    // 开始时间
    val startTime = System.currentTimeMillis
    // 获取文件File对象
    val file = diskManager.getFile(blockId)
    // 构建输出流
    val fileOutputStream = new FileOutputStream(file)
    var threwException: Boolean = true
    try {
      // 进行写出
      writeFunc(fileOutputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(fileOutputStream, threwException)
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    // 结束时间
    val finishTime = System.currentTimeMillis
    // 记录耗时
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  // 用于将BlockId所对应的Block写入磁盘
  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    // 调用put()方法，传入描述了写出操作的回调函数
    put(blockId) { fileOutputStream =>
      // 获取文件流的FileChannel
      val channel = fileOutputStream.getChannel
      Utils.tryWithSafeFinally {
        // 使用FileChannel写出到磁盘
        bytes.writeFully(channel)
      } {
        channel.close()
      }
    }
  }

  // 用于读取给定BlockId所对应的Block，并封装为ChunkedByteBuffer返回
  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    // 获取文件
    val file = diskManager.getFile(blockId.name)
    // 获取文件的FileChannel
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map
      // 小于阈值，写入堆缓冲
      if (file.length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(file.length.toInt)
        // 定位到文件开头
        channel.position(0)
        // 循环将读取数据写入到buf中
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=0\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        // 切换读模式
        buf.flip()
        // 返回包装了ByteBuffer的ChunkedByteBuffer对象
        new ChunkedByteBuffer(buf)
      } else {
        // 否则返回MappedByteBuffer内存映射的ChunkedByteBuffer包装对象
        new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length))
      }
    } {
      channel.close()
    }
  }

  // 用于删除给定BlockId所对应的Block文件
  def remove(blockId: BlockId): Boolean = {
    // 使用DiskBlockManager的getFile()获取对应的文件
    val file = diskManager.getFile(blockId.name)
    // 判断文件是否存在
    if (file.exists()) {
      // 存在，则删除文件
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      // 不存在，直接返回false
      false
    }
  }

  // 用于判断本地磁盘存储路径下是否包含给定BlockId所对应的Block文件
  def contains(blockId: BlockId): Boolean = {
    // 使用DiskBlockManager的getFile()获取对应的文件
    val file = diskManager.getFile(blockId.name)
    // 判断文件是否存在
    file.exists()
  }
}
