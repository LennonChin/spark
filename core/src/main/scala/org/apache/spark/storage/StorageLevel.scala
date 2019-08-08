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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.MemoryMode
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * or ExternalBlockStore, whether to drop the RDD to disk if it falls out of memory or
 * ExternalBlockStore, whether to keep the data in memory in a serialized format, and whether
 * to replicate the RDD partitions on multiple nodes.
 *
 * The [[org.apache.spark.storage.StorageLevel$]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 *
  * @param _useDisk 能否写入磁盘。当Block的StorageLevel中的_useDisk为true时，存储体系将允许Block写入磁盘。
  * @param _useMemory 能否写入堆内存。当Block的StorageLevel中的_useMemory为true时，存储体系将允许Block写入堆内存。
  * @param _useOffHeap 能否写入堆外内存。当Block的StorageLevel中的_useOffHeap为true时，存储体系将允许Block写入堆外内存。
  * @param _deserialized 表示数据是否已被反序列化。当Block本身经过了序列化后，Block的StorageLevel中的_deserialized将被设置为false，即表示在使用数据时需要对Block进行反序列化。
  * @param _replication Block的复制份数。Block的StorageLevel中的_replication默认等于1，可以在构造Block的StorageLevel时明确指定_replication的数量。
  *                     当_replication大于1时，Block除了在本地的存储体系中写入一份，还会复制到其他不同节点的存储体系中写入，达到复制备份的目的。
  */
@DeveloperApi
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1)
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  private def this(flags: Int, replication: Int) {
    this((flags & 8) != 0, // 是否使用磁盘
      (flags & 4) != 0, // 是否使用内存
      (flags & 2) != 0, // 是否使用堆外内存
      (flags & 1) != 0, // 是否已被反序列化
      replication) // 副本数量
  }

  // 创建用于反序列化的StorageLevel对象
  def this() = this(false, true, false, false)  // For deserialization

  // 能否写入磁盘
  def useDisk: Boolean = _useDisk
  // 能否写入堆内存
  def useMemory: Boolean = _useMemory
  // 能否写入堆外内存
  def useOffHeap: Boolean = _useOffHeap
  // 数据块是否已被反序列化
  def deserialized: Boolean = _deserialized
  // 复制份数
  def replication: Int = _replication

  // 副本数量必须小于40
  assert(replication < 40, "Replication restricted to be less than 40 for calculating hash codes")

  // 当使用堆外内存存储时，数据必须序列化存储
  if (useOffHeap) {
    require(!deserialized, "Off-heap storage level does not support deserialized storage")
  }

  // 内存模式
  private[spark] def memoryMode: MemoryMode = {
    // 如果useOffHeap为true，则返回MemoryMode.OFF_HEAP
    if (useOffHeap) MemoryMode.OFF_HEAP
    else MemoryMode.ON_HEAP // 否则返回MemoryMode.ON_HEAP
  }

  // 克隆操作，会返回一个新的StorageLevel对象
  override def clone(): StorageLevel = {
    new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
  }

  // 判断是否相等，需要所有属性都相同，才代表两个StorageLevel相等
  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ =>
      false
  }

  // 当前的StorageLevel是否有效：使用了内存或磁盘级别，且副本数量大于1
  def isValid: Boolean = (useMemory || useDisk) && (replication > 0)

  /**
    * 将当前StorageLevel转换为整型表示。
    * 四位二进制位，每一位都表示是否开启对应的级别。
    */
  def toInt: Int = {
    var ret = 0
    if (_useDisk) { // 1000
      ret |= 8
    }
    if (_useMemory) { // 0100
      ret |= 4
    }
    if (_useOffHeap) { // 0010
      ret |= 2
    }
    if (_deserialized) { // 0001
      ret |= 1
    }
    ret
  }

  /**
    * 将StorageLevel首先通过toInt方法将_useDisk、_useMemory、_useOffHeap、_deserialized四个属性
    * 设置到四位数的状态位，
    * 然后与_replication一起被序列化写入外部二进制流。
    */
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeByte(toInt)
    out.writeByte(_replication)
  }

  // 从外部二进制流中读取StorageLevel的各个属性。
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
  }

  // 缓存操作，会把当前StorageLevel缓存到伴生对象的storageLevelCache字典中
  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = {
    val disk = if (useDisk) "disk" else ""
    val memory = if (useMemory) "memory" else ""
    val heap = if (useOffHeap) "offheap" else ""
    val deserialize = if (deserialized) "deserialized" else ""

    val output =
      Seq(disk, memory, heap, deserialize, s"$replication replicas").filter(_.nonEmpty)
    s"StorageLevel(${output.mkString(", ")})"
  }

  override def hashCode(): Int = toInt * 41 + replication

  def description: String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    if (useMemory) {
      result += (if (useOffHeap) "Memory (off heap) " else "Memory ")
    }
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)

  /**
   * :: DeveloperApi ::
   * Return the StorageLevel object with the specified name.
    *
    * 从指定的字符串得到对应的存储级别
   */
  @DeveloperApi
  def fromString(s: String): StorageLevel = s match {
    case "NONE" => NONE
    case "DISK_ONLY" => DISK_ONLY
    case "DISK_ONLY_2" => DISK_ONLY_2
    case "MEMORY_ONLY" => MEMORY_ONLY
    case "MEMORY_ONLY_2" => MEMORY_ONLY_2
    case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
    case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
    case "MEMORY_AND_DISK" => MEMORY_AND_DISK
    case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
    case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
    case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
    case "OFF_HEAP" => OFF_HEAP
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      useOffHeap: Boolean,
      deserialized: Boolean,
      replication: Int): StorageLevel = {
    // 创建，并进行缓存
    getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object without setting useOffHeap.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      deserialized: Boolean,
      replication: Int = 1): StorageLevel = {
    // 创建，并进行缓存
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, false, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object from its integer representation.
   */
  @DeveloperApi
  def apply(flags: Int, replication: Int): StorageLevel = {
    // 创建，并进行缓存
    getCachedStorageLevel(new StorageLevel(flags, replication))
  }

  /**
   * :: DeveloperApi ::
   * Read StorageLevel object from ObjectInput stream.
   */
  @DeveloperApi
  def apply(in: ObjectInput): StorageLevel = {
    // 从流中反序列化，并进行缓存
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  // 用于缓存StorageLevel
  private[spark] val storageLevelCache = new ConcurrentHashMap[StorageLevel, StorageLevel]()

  // 先推入缓存，如果推入失败，说明已存在；再进行取
  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
