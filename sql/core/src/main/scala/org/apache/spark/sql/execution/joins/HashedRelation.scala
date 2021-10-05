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

package org.apache.spark.sql.execution.joins

import java.io._

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation extends KnownSizeEstimation {
  /**
   * Returns matched rows.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: InternalRow): Iterator[InternalRow]

  /**
   * Returns matched rows for a key that has only one column with LongType.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: Long): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the matched single row.
   */
  def getValue(key: InternalRow): InternalRow

  /**
   * Returns the matched single row with key that have only one column of LongType.
   */
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns true iff all the keys are unique.
   */
  def keyIsUnique: Boolean

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  def asReadOnlyCopy(): HashedRelation

  /**
   * Release any used resources.
   */
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   */
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int = 64,
      taskMemoryManager: TaskMemoryManager = null): HashedRelation = {

    // 获取或创建TaskMemoryManager
    val mm = Option(taskMemoryManager).getOrElse {
      new TaskMemoryManager(
        // 默认是StaticMemoryManager，
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"), // 关闭Off-heap内存
          Long.MaxValue, // 执行内存无限制
          Long.MaxValue, // 存储内存无限制
          1),
        0)
    }

    /**
     * 连接的键的只有1个，并且是Long类型，就使用LongHashedRelation，
     * 否则使用UnsafeHashedRelation
     */
    if (key.length == 1 && key.head.dataType == LongType) {
      LongHashedRelation(input, key, sizeEstimate, mm)
    } else {
      UnsafeHashedRelation(input, key, sizeEstimate, mm)
    }
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of value] [key bytes] [bytes for value]
 */
private[joins] class UnsafeHashedRelation(
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap)
  extends HashedRelation with Externalizable with KryoSerializable {

  private[joins] def this() = this(0, null)  // Needed for serialization

  override def keyIsUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def asReadOnlyCopy(): UnsafeHashedRelation = {
    new UnsafeHashedRelation(numFields, binaryMap)
  }

  override def estimatedSize: Long = binaryMap.getTotalMemoryConsumption

  // re-used in get()/getValue()
  var resultRow = new UnsafeRow(numFields)

  // 根据键获取多个值
  override def get(key: InternalRow): Iterator[InternalRow] = {
    // 得到连接键
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack

    // 使用BytesToBytesMap的safeLookup查找键对应的数据
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())

    // 检查是否能查找到
    if (loc.isDefined) {

      // 返回迭代器
      new Iterator[UnsafeRow] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext

        // 可以用于迭代获取下一行数据
        override def next(): UnsafeRow = {
          // 将resultRow锚定Location内的内存区域
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          // Location的nextValue()方法可以获取串联的下一个行的地址，并将该地址更新到Location内部的属性上，以便获取该行
          _hasNext = loc.nextValue()

          // 返回当前行数据
          resultRow
        }
      }
    } else {
      null
    }
  }

  // 根据键获取一个值
  def getValue(key: InternalRow): InternalRow = {
    // 得到连接键
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack


    // 使用BytesToBytesMap的safeLookup查找键对应的数据
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())

    // 检查是否能查找到
    if (loc.isDefined) {
      // 将resultRow锚定Location内的内存区域并返回
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  private def write(
      writeInt: (Int) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit) : Unit = {
    writeInt(numFields)
    // TODO: move these into BytesToBytesMap
    writeLong(binaryMap.numKeys())
    writeLong(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      writeBuffer(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      writeInt(loc.getKeyLength)
      writeInt(loc.getValueLength)
      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(in.readInt, in.readLong, in.readFully)
  }

  private def read(
      readInt: () => Int,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    numFields = readInt()
    resultRow = new UnsafeRow(numFields)
    val nKeys = readLong()
    val nValues = readLong()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // TODO(josh): We won't need this dummy memory manager after future refactorings; revisit
    // during code review

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      (nKeys * 1.5 + 1).toInt, // reduce hash collision
      pageSizeBytes)

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nValues) {
      val keySize = readInt()
      val valuesSize = readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      readBuffer(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      readBuffer(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSuceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        binaryMap.free()
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(in.readInt, in.readLong, in.readBytes)
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): HashedRelation = {

    // 获取Page大小，默认是16M
    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // 创建BytesToBytesMap
    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt, // 初始化大小，只有70%的空间可用
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    // 用于从数据行投影得到连接键的Projection
    val keyGenerator = UnsafeProjection.create(key)

    // 不断遍历input迭代器中的数据行
    var numFields = 0
    while (input.hasNext) {
      // 转换为UnsafeRow
      val row = input.next().asInstanceOf[UnsafeRow]

      // 获取字段数量
      numFields = row.numFields()

      // 从数据行中投影得到连接键
      val key = keyGenerator(row)

      if (!key.anyNull) { // 键不为空
        // 从BytesToBytesMap中查找对应的地址指针Location
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)

        // 使用Location将数据添加到BytesToBytesMap中
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)

        // 如果添加不成功，说明可能是内存不足，直接释放申请的内存，抛出异常
        if (!success) {
          binaryMap.free()
          throw new SparkException("There is no enough memory to build hash map")
        }
      }
    }

    // 使用BytesToBytesMap创建UnsafeHashedRelation，即构建表的Relation
    new UnsafeHashedRelation(numFields, binaryMap)
  }
}

/**
 * An append-only hash map mapping from key of Long to UnsafeRow.
 *
 * The underlying bytes of all values (UnsafeRows) are packed together as a single byte array
 * (`page`) in this format:
 *
 *  [bytes of row1][address1][bytes of row2][address1] ...
 *
 *  address1 (8 bytes) is the offset and size of next value for the same key as row1, any key
 *  could have multiple values. the address at the end of last value for every key is 0.
 *
 * The keys and addresses of their values could be stored in two modes:
 *
 * 1) sparse mode: the keys and addresses are stored in `array` as:
 *
 *  [key1][address1][key2][address2]...[]
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1. The position of key1
 *  is determined by `key1 % cap`. Quadratic probing with triangular numbers is used to address
 *  hash collision.
 *
 * 2) dense mode: all the addresses are packed into a single array of long, as:
 *
 *  [address1] [address2] ...
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1, the position is
 *  determined by `key1 - minKey`.
 *
 * The map is created as sparse mode, then key-value could be appended into it. Once finish
 * appending, caller could all optimize() to try to turn the map into dense mode, which is faster
 * to probe.
 *
 * see http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
 *
 * 只支持追加的HashMap，映射LOng型的键到UnsafeRow类型的值。
 *
 * 所有UnsafeRow类型的值的字节数据会被几种存储为单个Array字节数组（其实是内存Page）中，存储格式如下：
 *
 * [row1的字节数据][address1（8 bytes）][row2的字节数据][address1（8 bytes）]...
 *
 * address1是与row1对应的key相同的下一个行的地址，任何key可以有多个对应的值，最后一个值的address为0。
 *
 * key和对应值的地址可以有两种存储模式：
 *
 * 1. 稀疏存储：键和地址在array中存储为：
 *
 * [key1][address1][key2][address2]...[]
 *
 * address1（Long）是key1对应值的offset和size。key1的位置通过`key1 % cap`获取。
 * 使用三角形数的二次探测用于解决散列冲突。
 *
 * 2. 密集存储：所有地址都打包成一个Long型数组，如：
 *
 *  [address1] [address2] ...
 *
 *  address1（Long）是 key1 值的offset（在 `page` 中）和size，位置由 `key1 - minKey` 决定。
 *
 *  Map在创建时为稀疏模式，然后可以将键值附加到其中。
 *  一旦完成附加，调用者都可以使用 optimize() 尝试将Map转换为密集模式，这样可以更快地探测。
 *
 *  参考：http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
 */
private[execution] final class LongToUnsafeRowMap(val mm: TaskMemoryManager, capacity: Int)
  extends MemoryConsumer(mm) with Externalizable with KryoSerializable {

  // Whether the keys are stored in dense mode or not.
  // 是否是密集模式存储
  private var isDense = false

  // The minimum key
  // 最小的键，Long最小值
  private var minKey = Long.MaxValue

  // The maxinum key
  // 最大的键，Long最大值
  private var maxKey = Long.MinValue

  // The array to store the key and offset of UnsafeRow in the page.
  //
  // Sparse mode: [key1] [offset1 | size1] [key2] [offset | size2] ...
  // Dense mode: [offset1 | size1] [offset2 | size2]
  // 用于存储键和内存页中UnsafeRow的偏移量的数组，两种模式存储格式不同
  private var array: Array[Long] = null
  private var mask: Int = 0

  // The page to store all bytes of UnsafeRow and the pointer to next rows.
  // [row1][pointer1] [row2][pointer2]
  // 用于存储UnsafeRow所有字节数据及指向下一个key相同的UnsafeRow数据的地址指针
  private var page: Array[Long] = null

  // Current write cursor in the page.
  // 当前内存页中的游标
  private var cursor: Long = Platform.LONG_ARRAY_OFFSET

  // The number of bits for size in address
  // 地址的比特位长度
  private val SIZE_BITS = 28
  // 地址掩码，低28位
  private val SIZE_MASK = 0xfffffff

  // The total number of values of all keys.
  // 值的数量
  private var numValues = 0L

  // The number of unique keys.
  // 键的数量
  private var numKeys = 0L

  // needed by serializer
  def this() = {
    this(
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0),
      0)
  }

  // 保证内存充足，内部会申请内存
  private def ensureAcquireMemory(size: Long): Unit = {
    // do not support spilling
    val got = acquireMemory(size)
    if (got < size) {
      freeMemory(got)
      throw new SparkException(s"Can't acquire $size bytes memory to build hash relation, " +
        s"got $got bytes")
    }
  }

  // 初始化
  private def init(): Unit = {
    if (mm != null) { // MemoryManager不为空
      // 最多只支持5亿条数据
      require(capacity < 512000000, "Cannot broadcast more than 512 millions rows")
      var n = 1
      while (n < capacity) n *= 2
      ensureAcquireMemory(n * 2L * 8 + (1 << 20))
      array = new Array[Long](n * 2)
      mask = n * 2 - 2
      page = new Array[Long](1 << 17)  // 1M bytes
    }
  }

  // 创建时就会调用初始化方法
  init()

  // 不会响应溢写
  def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Returns whether all the keys are unique.
   *
   * 所有键是否都是唯一的，可通过键的数量和值的数量是否相等来判断
   */
  def keyIsUnique: Boolean = numKeys == numValues

  /**
   * Returns total memory consumption.
   *
   * 返回总的内存消耗
   */
  def getTotalMemoryConsumption: Long = array.length * 8L + page.length * 8L

  /**
   * Returns the first slot of array that store the keys (sparse mode).
   *
   * 返回稀疏模式下数组中用于存储指定key的slot
   */
  private def firstSlot(key: Long): Int = {
    val h = key * 0x9E3779B9L
    (h ^ (h >> 32)).toInt & mask
  }

  /**
   * Returns the next probe in the array.
   *
   * 返回下一个探测位置
   */
  private def nextSlot(pos: Int): Int = (pos + 2) & mask

  // offset和size转换为地址
  private[this] def toAddress(offset: Long, size: Int): Long = {
    // 高36位为offset，低位为size
    ((offset - Platform.LONG_ARRAY_OFFSET) << SIZE_BITS) | size
  }

  // address转换为offset
  private[this] def toOffset(address: Long): Long = {
    // 高36位 + Platform.LONG_ARRAY_OFFSET
    (address >>> SIZE_BITS) + Platform.LONG_ARRAY_OFFSET
  }

  // address转换为size
  private[this] def toSize(address: Long): Int = {
    // 低28位
    (address & SIZE_MASK).toInt
  }

  // 根据address获取行，会把出入的resultRow锚定到address内存区域
  private def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(page, toOffset(address), toSize(address))
    resultRow
  }

  /**
   * Returns the single UnsafeRow for given key, or null if not found.
   *
   * 返回给定key的单个行，如果没有找到就返回null
   */
  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    if (isDense) { // 密集模式
      if (key >= minKey && key <= maxKey) { // 确保范围
        // 直接获取值
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return getRow(value, resultRow)
        }
      }
    } else { // 稀疏模式
      // 获取slot
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) { // address不为0，表明对应的值存在
        if (array(pos) == key) { // 判断key是否相同
          // 可以取到值，返回即可
          return getRow(array(pos + 1), resultRow)
        }
        // 哈希冲突，找下一个位置
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Returns an iterator of UnsafeRow for multiple linked values.
   * 返回多个值的UnsafeRow迭代器
   */
  private def valueIter(address: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    new Iterator[UnsafeRow] {
      var addr = address

      // 当addr不为0时，说明还有行数据
      override def hasNext: Boolean = addr != 0

      // 获取下一条数据
      override def next(): UnsafeRow = {

        // 取偏移量和长度
        val offset = toOffset(addr)
        val size = toSize(addr)

        // 先锚定到内存区域
        resultRow.pointTo(page, offset, size)

        // 取下一个key相同的value的地址，使用addr记录
        addr = Platform.getLong(page, offset + size)

        // 返回当前行
        resultRow
      }
    }
  }

  /**
   * Returns an iterator for all the values for the given key, or null if no value found.
   * 返回多个值的UnsafeRow迭代器，如果没有找到就返回null
   */
  def get(key: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    if (isDense) { // 密集模式
      if (key >= minKey && key <= maxKey) { // 检查key范围
        // 直接获取值
        val value = array((key - minKey).toInt)
        if (value > 0) {
          // 返回迭代器
          return valueIter(value, resultRow)
        }
      }
    } else { // 稀疏模式
      // 获取slot
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) { // address不为0，表明对应的值存在
        if (array(pos) == key) { // 判断key是否相同
          // 可以取到值，返回迭代器
          return valueIter(array(pos + 1), resultRow)
        }
        // 哈希冲突，找下一个位置
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Appends the key and row into this map.
   * 添加key和对应的行到Map中
   */
  def append(key: Long, row: UnsafeRow): Unit = {
    // 行的大小
    val sizeInBytes = row.getSizeInBytes

    // 一行不可以超过256M
    if (sizeInBytes >= (1 << SIZE_BITS)) {
      sys.error("Does not support row that is larger than 256M")
    }

    // 检查Key是否超出minKey ~ maxKey范围，如果超过就更新二者
    if (key < minKey) {
      minKey = key
    }
    if (key > maxKey) {
      maxKey = key
    }

    // There is 8 bytes for the pointer to next value
    // 8字节用于存放下一个key相同的值的指针
    // 判断是否需要扩容
    if (cursor + 8 + row.getSizeInBytes > page.length * 8L + Platform.LONG_ARRAY_OFFSET) {
      val used = page.length

      // 大于8G不支持
      if (used >= (1 << 30)) {
        sys.error("Can not build a HashedRelation that is larger than 8G")
      }

      // 两倍扩容
      ensureAcquireMemory(used * 8L * 2)

      // 构建新的内存页
      val newPage = new Array[Long](used * 2)

      // 将旧的内存页中数据拷贝到新内存页中
      Platform.copyMemory(page, Platform.LONG_ARRAY_OFFSET, newPage, Platform.LONG_ARRAY_OFFSET,
        cursor - Platform.LONG_ARRAY_OFFSET)

      // page指向新内存页
      page = newPage

      // 释放旧的内存
      freeMemory(used * 8L)
    }

    // copy the bytes of UnsafeRow
    // 将UnsafeRow的数据拷贝到内存页中
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)

    // 更新游标
    cursor += row.getSizeInBytes

    // 下一个key相同的值的指针存为0
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1

    // 更新地址信息
    updateIndex(key, toAddress(offset, row.getSizeInBytes))
  }

  /**
   * Update the address in array for given key.
   * 更新给定key的地址信息
   */
  private def updateIndex(key: Long, address: Long): Unit = {
    // 找一个slot
    var pos = firstSlot(key)
    assert(numKeys < array.length / 2)

    // 发生哈希冲突，持续探测
    while (array(pos) != key && array(pos + 1) != 0) {
      pos = nextSlot(pos)
    }

    if (array(pos + 1) == 0) { // 地址为0，说明key在当前Map中不存在，这是第一次插入
      // this is the first value for this key, put the address in array.
      // 记录key和address
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 4 > array.length) {
        // reach half of the capacity
        if (array.length < (1 << 30)) {
          // Cannot allocate an array with 2G elements
          growArray()
        } else if (numKeys > array.length / 2 * 0.75) {
          // The fill ratio should be less than 0.75
          sys.error("Cannot build HashedRelation with more than 1/3 billions unique keys")
        }
      }
    } else {
      // there are some values for this key, put the address in the front of them.
      // Map中已经存在相同key
      val pointer = toOffset(address) + toSize(address)
      // 将之前相同key的地址存入到数据行的尾指针里
      Platform.putLong(page, pointer, array(pos + 1))
      // 更新地址为当前地址
      array(pos + 1) = address
    }
  }

  // 扩容array大小
  private def growArray(): Unit = {
    var old_array = array
    val n = array.length
    numKeys = 0

    // 扩容两倍
    ensureAcquireMemory(n * 2 * 8L)
    array = new Array[Long](n * 2)

    // mask更新
    mask = n * 2 - 2

    // 需要将旧array的数据转移到新array中
    var i = 0
    while (i < old_array.length) {
      if (old_array(i + 1) > 0) {
        updateIndex(old_array(i), old_array(i + 1))
      }
      i += 2
    }

    // 释放旧的array
    old_array = null  // release the reference to old array
    freeMemory(n * 8L)
  }

  /**
   * Try to turn the map into dense mode, which is faster to probe.
   * 尝试将Map转为密集模式，可以更快地探测
   */
  def optimize(): Unit = {
    val range = maxKey - minKey
    // Convert to dense mode if it does not require more memory or could fit within L1 cache
    // SPARK-16740: Make sure range doesn't overflow if minKey has a large negative value
    /**
     * 如果不需要更多内存或可以放入 L1 缓存，则转换为密集模式。
     * SPARK-16740: 如果 minKey 具有较大的负值，请确保范围不会溢出。
     *
     * - key所在的范围小于array的长度，说明key有重复。（注意：array的大小是两倍于存储的key的数量的）
     * - key值的范围差小于1024。
     */
    if (range >= 0 && (range < array.length || range < 1024)) {
      try {
        // 按照key的范围差申请内存
        ensureAcquireMemory((range + 1) * 8L)
      } catch {
        case e: SparkException =>
          // there is no enough memory to convert
          return
      }
      // 创建密集数组
      val denseArray = new Array[Long]((range + 1).toInt)

      // 遍历array里的key
      var i = 0
      while (i < array.length) {
        if (array(i + 1) > 0) { // 地址不为0
          // 根据当前key的值与minKey的值的差作为索引，存放到密集数组中
          val idx = (array(i) - minKey).toInt
          denseArray(idx) = array(i + 1)
        }
        // 计算下一个key
        i += 2
      }
      val old_length = array.length
      array = denseArray
      isDense = true
      freeMemory(old_length * 8L)
    }
  }

  /**
   * Free all the memory acquired by this map.
   * 释放Map内存，将array和page都进行释放
   */
  def free(): Unit = {
    if (page != null) {
      freeMemory(page.length * 8L)
      page = null
    }
    if (array != null) {
      freeMemory(array.length * 8L)
      array = null
    }
  }

  // 将arr中的数据通过writeBuffer写出
  private def writeLongArray(
      writeBuffer: (Array[Byte], Int, Int) => Unit,
      arr: Array[Long],
      len: Int): Unit = {
    val buffer = new Array[Byte](4 << 10) // 4096
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = len * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      Platform.copyMemory(arr, offset, buffer, Platform.BYTE_ARRAY_OFFSET, size)
      writeBuffer(buffer, 0, size.toInt)
      offset += size
    }
  }

  // 序列化写出当前Map
  private def write(
      writeBoolean: (Boolean) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    // 是否是密集模式
    writeBoolean(isDense)

    // minKey和maxKey
    writeLong(minKey)
    writeLong(maxKey)

    // key和value的数量
    writeLong(numKeys)
    writeLong(numValues)

    // 写出array的大小
    writeLong(array.length)

    // 将array的数据写出
    writeLongArray(writeBuffer, array, array.length)

    // 写出page使用的大小
    val used = ((cursor - Platform.LONG_ARRAY_OFFSET) / 8).toInt
    writeLong(used)

    // 写出page
    writeLongArray(writeBuffer, page, used)
  }

  override def writeExternal(output: ObjectOutput): Unit = {
    write(output.writeBoolean, output.writeLong, output.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    write(out.writeBoolean, out.writeLong, out.write)
  }

  // 从readBuffer读入数据，构造为Array[Long]
  private def readLongArray(
      readBuffer: (Array[Byte], Int, Int) => Unit,
      length: Int): Array[Long] = {
    val array = new Array[Long](length)
    val buffer = new Array[Byte](4 << 10) // 4096
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = length * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      readBuffer(buffer, 0, size.toInt)
      Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, array, offset, size)
      offset += size
    }
    array
  }

  // 反序列化
  private def read(
      readBoolean: () => Boolean,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {

    // 是否密集模式
    isDense = readBoolean()

    // minKey和maxKey
    minKey = readLong()
    maxKey = readLong()

    // key和value数量
    numKeys = readLong()
    numValues = readLong()

    // array大小
    val length = readLong().toInt
    mask = length - 2
    // 读入Array数据
    array = readLongArray(readBuffer, length)

    // page大小
    val pageLength = readLong().toInt
    // 读入page数据
    page = readLongArray(readBuffer, pageLength)
  }

  override def readExternal(in: ObjectInput): Unit = {
    read(in.readBoolean, in.readLong, in.readFully)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    read(in.readBoolean, in.readLong, in.readBytes)
  }
}

private[joins] class LongHashedRelation(
    private var nFields: Int,
    private var map: LongToUnsafeRowMap) extends HashedRelation with Externalizable {

  // 用于返回结果的行
  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def asReadOnlyCopy(): LongHashedRelation = new LongHashedRelation(nFields, map)

  override def estimatedSize: Long = map.getTotalMemoryConsumption

  override def get(key: InternalRow): Iterator[InternalRow] = {
    // 键为空，返回null，不为空，重载调用另一个方法
    if (key.isNullAt(0)) {
      null
    } else {
      get(key.getLong(0))
    }
  }

  override def getValue(key: InternalRow): InternalRow = {
    // 键为空，返回null，不为空，重载调用另一个方法
    if (key.isNullAt(0)) {
      null
    } else {
      getValue(key.getLong(0))
    }
  }

  // 从LongToUnsafeRowMap中获取
  override def get(key: Long): Iterator[InternalRow] = map.get(key, resultRow)

  // 从LongToUnsafeRowMap中获取
  override def getValue(key: Long): InternalRow = map.getValue(key, resultRow)

  override def keyIsUnique: Boolean = map.keyIsUnique

  override def close(): Unit = {
    map.free()
  }

  // 序列化方法
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeObject(map)
  }

  // 反序列化方法
  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    resultRow = new UnsafeRow(nFields)
    map = in.readObject().asInstanceOf[LongToUnsafeRowMap]
  }
}

/**
 * Create hashed relation with key that is long.
 *
 * LongHashedRelation中的键都是Long类型的。
 */
private[joins] object LongHashedRelation {
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager): LongHashedRelation = {

    // 创建一个LongToUnsafeRowMap
    val map = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate)

    // 根据给定行生成连接键的Projection
    val keyGenerator = UnsafeProjection.create(key)

    // Create a mapping of key -> rows
    // 创建 key -> rows 的映射关系
    var numFields = 0

    // 不断迭代输入数据
    while (input.hasNext) {
      // 将输入数据转换为UnsafeRow
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]

      // 获取字段类型
      numFields = unsafeRow.numFields()

      // Project获取连接键
      val rowKey = keyGenerator(unsafeRow)

      // 检查键是否为空
      if (!rowKey.isNullAt(0)) {
        // 不为空，将其添加到LongToUnsafeRowMap中
        val key = rowKey.getLong(0)
        map.append(key, unsafeRow)
      }
    }

    // 优化LongToUnsafeRowMap
    map.optimize()

    // 创建LongHashedRelation返回
    new LongHashedRelation(numFields, map)
  }
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation.
 * HashedRelationBroadcastMode 要求将行作为 HashedRelation 进行广播。
 **/
private[execution] case class HashedRelationBroadcastMode(key: Seq[Expression])
  extends BroadcastMode {

  // 用于将构建表转换为HashedRelation
  override def transform(rows: Array[InternalRow]): HashedRelation = {
    HashedRelation(rows.iterator, canonicalizedKey, rows.length)
  }

  private lazy val canonicalizedKey: Seq[Expression] = {
    key.map { e => e.canonicalized }
  }

  // 是否与其他BroadcastMode相兼容
  override def compatibleWith(other: BroadcastMode): Boolean = other match {
    case m: HashedRelationBroadcastMode => canonicalizedKey == m.canonicalizedKey
    case _ => false
  }
}
