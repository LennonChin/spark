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

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.common.collect.ConcurrentHashMultiset

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging


/**
 * Tracks metadata for an individual block.
 *
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
  *
  * BlockInfo用于描述块的元数据信息，包括存储级别、Block类型、大小、锁信息等。
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
  *              BlockInfo所描述的Block的存储级别，即StorageLevel。
 * @param classTag the block's [[ClassTag]], used to select the serializer
  *                 BlockInfo所描述的Block的类型。用于选择序列化器。
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 *                   BlockInfo所描述的Block在状态发生变化时是否需要告知Master。
 *                   大多数Block的该参数值为true，Broadcast类型的Block该值为false。
 */
private[storage] class BlockInfo(
    val level: StorageLevel,
    val classTag: ClassTag[_],
    val tellMaster: Boolean) {

  /**
   * The size of the block (in bytes)
    * 数据块的大小
   */
  def size: Long = _size

  // 设置数据块的大小
  def size_=(s: Long): Unit = {
    _size = s
    // 检查
    checkInvariants()
  }

  // BlockInfo所描述的Block的大小。
  private[this] var _size: Long = 0

  /**
   * The number of times that this block has been locked for reading.
    *
    * 获取读锁被锁次数
   */
  def readerCount: Int = _readerCount

  // 设置读锁被锁次数
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    // 读锁次数增加需要在数据块不可变的情况下执行
    checkInvariants()
  }

  // 读锁被锁次数
  private[this] var _readerCount: Int = 0

  /**
   * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
    *
    * 获取当前执行写操作的任务，即TaskAttemptId
   */
  def writerTask: Long = _writerTask

  // 设置执行写操作的任务，即TaskAttemptId
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    // 设置写锁任务需要在数据块不可变的情况下执行
    checkInvariants()
  }

  /**
    * TaskAttempt在对Block进行写操作前，首先必须获得对应BlockInfo的写锁。
    * _writerTask用于保存TaskAttempt的ID（每个任务在实际执行时，会多次尝试，每次尝试都会分配一个ID）。
    */
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  /**
    * 检查是否不可变，当数据块可变时，该方法会抛出异常；
    * 这个方法是为了保证一些操作只在数据块不可变的情况下被执行，即下列情况之一：
    * 1. 数据块没有进行读操作；
    * 2. 数据块正在被读，且没有写操作。
    */
  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    // 读锁被锁次数必须非负
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    // 读锁被锁次数大于0时，必须是没有写任务；即不能在读的时候写数据
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  // 初始化时检查一次不可变性
  checkInvariants()
}

private[storage] object BlockInfo {

  /**
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
    *
    * 无写操作的标识
   */
  val NO_WRITER: Long = -1

  /**
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
    *
    * 读锁被持有了，但不是被Task线程持有；如，可能被Driver线程持有，或其他测试线程持有
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 *
 * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 *
 * This class is thread-safe.
  *
  * 块信息管理器。负责对Block的元数据及锁资源进行管理。
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
    *
    * BlockId与BlockInfo之间映射关系的缓存字典
   */
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
   * Tracks the set of blocks that each task has locked for writing.
    *
    * 每次TaskAttempt的标识TaskAttemptId与获取的Block的写锁之间的映射关系。
    * TaskAttemptId与写锁之间是一对多的关系，即一次TaskAttempt执行会获取零到多个Block的写锁。
   */
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId] // 复合类型

  /**
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
    *
    * 每次TaskAttempt的标识TaskAttemptId与获取的Block的读锁之间的映射关系。
    * TaskAttemptId与读锁之间是一对多的关系，即一次TaskAttempt执行会获取零到多个Block的读锁，
    * 并且会记录对于同一个Block的读锁的占用次数。
   */
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
    *
    * 注册TaskAttemptId
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    // 读锁字典中没有记录该TaskAttempt的ID
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    // 在读锁字典中为该TaskAttempt创建对应的空集合
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
   * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
    *
    * 获取任务上下文TaskContext中当前正在执行的TaskAttempt的TaskAttemptId
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    // 如果没有将返回BlockInfo.NON_TASK_WRITER（-1024）
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
    *
    * 读锁定：
    * 1. 获取要读的BlockInfo，判断它是否正在被写；
    * 2. 如果正在被写，就判断是否指定了阻塞等待；
    *     - 如果指定了阻塞等待，则阻塞等待直到写锁释放后被唤醒，然后重新获取读锁；
    *     - 如果没有指定阻塞等待，就放弃，返回NONE
    * 3. 如果没有正在被写，就将BlockInfo的读锁次数加1，
    *     然后将维护readLocksByTask字典中的记录，并返回BlockInfo。
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized { // 加锁
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    do {
      // 从infos中获取BlockId对应的BlockInfo
      infos.get(blockId) match {
        case None => return None // 获取不到则返回None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER) { // 没有写锁
            // 由当前TaskAttempt线程持有读锁并返回BlockInfo
            info.readerCount += 1
            readLocksByTask(currentTaskAttemptId).add(blockId)
            logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
            return Some(info)
          }
      }
      // 走到这里说明无法获取读锁，有其他TaskAttempt线程正在写
      if (blocking) {
        // 如果设置了阻塞，则等待，阻塞在BlockInfoManager对象上
        wait()
      }
    } while (blocking) // 如果设置了阻塞，则一直循环获取直到获取到
    None
  }

  /**
   * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
    *
    * 写锁定：
    * 1. 获取要读的BlockInfo，判断它是否正在被写或者被读；
    * 2. 如果是，就判断是否指定了阻塞等待；
    *     - 如果指定了阻塞等待，则阻塞等待直到读锁和写锁都释放后被唤醒，然后重新获取写锁；
    *     - 如果没有指定阻塞等待，就放弃，返回NONE
    * 3. 如果没有正在被写或被读，就使用BlockInfo的writerTask记录当前TaskAttempt的ID，
    *       然后将维护writeLocksByTask字典中的记录，并返回BlockInfo。
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    do {
      // 从infos中获取BlockId对应的BlockInfo
      infos.get(blockId) match {
        case None => return None // 获取不到则返回None
        case Some(info) =>
          if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
            // 没有写锁，且没有读锁重入，则由当前TaskAttempt线程持有写锁并返回BlockInfo
            info.writerTask = currentTaskAttemptId
            writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
            logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
            return Some(info)
          }
      }
      // 走到这里说明无法获取写锁，有其他TaskAttempt线程正在写或读
      if (blocking) {
        // 如果设置了阻塞，则等待，阻塞在BlockManager对象上
        wait()
      }
    } while (blocking) // 如果设置了阻塞，则一直循环获取直到获取到
    None
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
    *
    * 判断当前TaskAttempt是否获取了指定BlockInfo的读锁
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    infos.get(blockId) match {
      case Some(info) =>
        if (info.writerTask != currentTaskAttemptId) {
          // 当前TaskAttempt没有获取该BlockInfo的读锁，抛出异常
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          // 当前TaskAttempt获取了该BlockInfo的读锁，将该BlockInfo返回
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
    *
    * 获取BlockId对应的BlockInfo
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
    *
    * 锁降级，即写锁 -> 降级为读锁
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    // 获取blockId对应的BlockInfo
    val info = get(blockId).get
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    // 调用unlock方法释放当前TaskAttempt线程从BlockId对应Block获取的写锁。
    unlock(blockId)
    // 非阻塞方式获取读锁
    val lockOutcome = lockForReading(blockId, blocking = false)
    assert(lockOutcome.isDefined)
  }

  /**
   * Release a lock on the given block.
    *
    * 释放BlockId对应的Block上的锁
   */
  def unlock(blockId: BlockId): Unit = synchronized {
    // 当前TaskAttempt释放了锁
    logTrace(s"Task $currentTaskAttemptId releasing lock for $blockId")
    // 获取对应的BlockInfo
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    if (info.writerTask != BlockInfo.NO_WRITER) { // 持有写锁
      info.writerTask = BlockInfo.NO_WRITER // 释放写锁
      writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
    } else { // 持有读锁
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      // 读锁重入次数减1
      info.readerCount -= 1
      // 获取对应的BlockId集合
      val countsForTask = readLocksByTask(currentTaskAttemptId)
      /**
       * 对TaskAttempt在readLocksByTask集合中对应的BlockId的出现次数减1，
       * 返回的次数是移除操作之前对blockId对应的块正在进行读操作的TaskAttempt数量
       */
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      /**
        * newPinCountForTask表示当前TaskAttempt持有BlockId对应的Block的读锁次数与1的差值
        * 如果newPinCountForTask次数小于0，表示读锁释放次数大于加锁次数，会抛出异常
        */
      assert(newPinCountForTask >= 0, // 该条件不成立时才会抛出异常
        s"Task $currentTaskAttemptId release lock on block $blockId more times than it acquired it")
    }
    // 唤醒其它获取读锁或写锁失败的TaskAttempt线程，让它们重新尝试获取
    notifyAll()
  }

  /**
   * Attempt to acquire the appropriate lock for writing a new block.
   *
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
    *
    * 尝试添加新的BlockInfo并获取其写锁
    * 如果对应的BlockInfo已经存在，就返回false
   *
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         a read lock on the existing block will be held. If this returns true, a write lock on
   *         the new block will be held.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    // 获取读锁
    lockForReading(blockId) match {
      case Some(info) => // 能获取得到，说明BlockId对应的Block已经存在
        // 说明有其它TaskAttempt线程竞争操作，直接返回false即可
        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        false
      case None => // 如果没有获取到，说明BlockId对应的Block还不存在
        // Block does not yet exist or is removed, so we are free to acquire the write lock
        // 将新的BlockInfo放入infos字典
        infos(blockId) = newBlockInfo
        // 尝试获取写锁，默认阻塞
        lockForWriting(blockId)
        // 获取到则返回true
        true
    }
  }

  /**
   * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the ids of blocks whose pins were released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = {
    // 记录释放了锁的数据库的BlockId
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    // 获取TaskAttempt读锁定的BlockInfo的BlockId的集合
    val readLocks = synchronized {
      readLocksByTask.remove(taskAttemptId).get
    }
    // 获取该TaskAttempt写锁定的BlockInfo的BlockId的集合
    val writeLocks = synchronized {
      writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)
    }

    // 遍历写锁定的BlockId的集合
    for (blockId <- writeLocks) {
      // 获取对应的BlockInfo
      infos.get(blockId).foreach { info =>
        // 判断当前持有该BlockInfo写锁的TaskAttempt是否是指定的TaskAttempt
        assert(info.writerTask == taskAttemptId)
        // 如果是，清除该BlockInfo的写锁
        info.writerTask = BlockInfo.NO_WRITER
      }
      // 对该BlockInfo的BlockId进行记录
      blocksWithReleasedLocks += blockId
    }

    // 遍历读锁定的BlockId的集合
    readLocks.entrySet().iterator().asScala.foreach { entry =>
      // 获取BlockId
      val blockId = entry.getElement
      // 获取读锁的次数
      val lockCount = entry.getCount
      // 记录该BlockInfo的BlockId
      blocksWithReleasedLocks += blockId
      synchronized {
        // 获取对应BlockInfo
        get(blockId).foreach { info =>
          // 释放1次读锁
          info.readerCount -= lockCount
          assert(info.readerCount >= 0)
        }
      }
    }

    synchronized {
      // 唤醒其它获取读锁或写锁失败的TaskAttempt线程，让它们重新尝试获取
      notifyAll()
    }
    // 返回记录的释放了锁的BlockInfo的BlockId集合
    blocksWithReleasedLocks
  }

  /**
   * Returns the number of blocks tracked.
    *
    * 返回infos的大小，即所有Block的数量。
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
    *
    * 返回infos的迭代器
   */
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
    *
    * 移除BlockId对应的BlockInfo。
   */
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    infos.get(blockId) match { // 获取对应的BlockInfo
      case Some(blockInfo) => // 能获取到
        if (blockInfo.writerTask != currentTaskAttemptId) { // 查看拥有写锁的线程是否就是当前线程
          // 如果不是则抛出异常
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else { // 拥有写锁的线程就是当前线程
          // 从infos中移除对应的BlockInfo
          infos.remove(blockId)
          // 将BlockInfo的读锁重入次数置为0
          blockInfo.readerCount = 0
          // 将BlockInfo的读TaskAttempt线程置为NO_WRITER
          blockInfo.writerTask = BlockInfo.NO_WRITER
          // 从writeLocksByTask移除对应TaskAttempt对应的blockId
          writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    // 唤醒其它阻塞获取的线程
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
    * 清除BlockInfoManager中的所有信息，并通知所有在BlockInfoManager管理的Block的锁上等待的线程。
   */
  def clear(): Unit = synchronized {
    // 清理所有BlockInfo的读锁重入次数和TaskAttempt读线程
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
    }
    // 清空infos数组
    infos.clear()
    // 清空读写映射关系
    readLocksByTask.clear()
    writeLocksByTask.clear()
    // 唤醒其它阻塞获取的线程
    notifyAll()
  }

}
