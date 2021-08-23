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

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a [[LocalSchedulerBackend]] and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 *
 * TaskSchedulerImpl的功能包括接收DAGScheduler给每个Stage创建的Task集合，
 * 按照调度算法将资源分配给Task，将Task交给Spark集群不同节点上的Executor运行，
 * 在这些Task执行失败时进行重试，通过推测执行减轻落后的Task对整体作业进度的影响。
 *
 * @param maxTaskFailures 任务失败的最大次数，非local模式下由spark.task.maxFailures参数决定
 * @param isLocal         是否是Local部署模式
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  // 传入的MAX_TASK_FAILURES由spark.task.maxFailures参数配置，默认值为4
  def this(sc: SparkContext) = this(sc, sc.conf.get(config.MAX_TASK_FAILURES))

  val conf = sc.conf

  // How often to check for speculative tasks
  // 任务推测执行的时间间隔。可以通过spark.speculation.interval属性进行配置，默认为100ms。
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  /**
   * 用于保证原始任务至少需要运行的时间。大小固定为100。
   * 原始任务只有超过此时间限制，才允许启动副本任务。
   * 这可以避免原始任务执行太短的时间就被推测执行副本任务。
   */
  val MIN_TIME_TO_SPECULATION = 100

  // 对任务调度进行推测执行的线程池，创建的线程以task-scheduler-speculation为前缀。
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  // 判断TaskSet饥饿的阈值。可通过spark.starvation.timeout属性配置，默认为15s。
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  // 每个Task需要分配的CPU核数。可通过spark.task.cpus属性配置，默认为1。
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  // 用于StageId、Stage Attempt、TaskSetManager的二级缓存。
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  // Task与所属TaskSetManager的映射关系
  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]

  // Task与执行此Task的Executor之间的映射关系
  val taskIdToExecutorId = new HashMap[Long, String]

  // 标记TaskSchedulerImpl是否已经接收到Task
  @volatile private var hasReceivedTask = false
  // 标记TaskSchedulerImpl接收的Task是否已经有运行过的
  @volatile private var hasLaunchedTask = false

  // 处理饥饿的定时器
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  // 用于生成新提交Task的标识
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  // 用于缓存Executor与运行在此Executor上的任务之间的映射关系，一个Executor上可以运行多个Task。
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // 获取每个Executor上运行的Task数量的字典
  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  // 用于缓存机器的Host与运行在此机器上的Executor之间的映射关系，机器与Executor之间是一对多的关系
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  // 用于缓存机器所在的机架与机架上机器的Host之间的映射关系，机架与机器之间是一对多的关系
  protected val hostsByRack = new HashMap[String, HashSet[String]]

  // Executor与Executor运行所在机器的Host之间的映射关系
  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  // DAGScheduler
  var dagScheduler: DAGScheduler = null

  // SchedulerBackend
  var backend: SchedulerBackend = null

  // MapOutputTracker
  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  // SchedulableBuilder
  var schedulableBuilder: SchedulableBuilder = null

  // 根调度池
  var rootPool: Pool = null

  // default scheduler is FIFO
  // 调度模式配置。可以通过spark.scheduler.mode属性配置，默认为FIFO。
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  /**
   * 调度模式。此属性依据schedulingModeConf获取枚举类型SchedulingMode的具体值。
   * 共有FAIR、FIFO、NONE三种枚举值。
   */
  val schedulingMode: SchedulingMode = try {
    // 由spark.scheduler.mode参数决定
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  // 通过线程池，对Slave发送的Task的执行结果进行处理。
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  // 初始化方法
  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    // 创建根调度池
    rootPool = new Pool("", schedulingMode, 0, 0)
    // 根据调度模式，创建相应的调度池构建器，默认为FIFOSchedulableBuilder
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported spark.scheduler.mode: $schedulingMode")
      }
    }
    // 构建调度池
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  // 启动TaskSchedulerImpl
  override def start() {
    // 启动SchedulerBackend
    backend.start()

    // 设置检查可推测任务的定时器，非本地模式，且打开了推测执行功能
    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      // 创建线程放入speculationScheduler线程池
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          // 检查可推测执行的Task
          checkSpeculatableTasks()
        }
        // 延迟100ms，每隔100ms执行一次
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  // 于等待SchedulerBackend准备就绪
  override def postStartHook() {
    waitBackendReady()
  }

  // 处理传入的TaskSet
  override def submitTasks(taskSet: TaskSet) {
    // 获取TaskSet中的所有Task
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      // 创建TaskSetManager
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      // TaskSet的Stage
      val stage = taskSet.stageId
      // 更新taskSetsByStageIdAndAttempt中记录的推测执行信息
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager

      // 判断是否有冲突的TaskSet，taskSetsByStageIdAndAttempt中不应该存在同属于当前Stage，但是TaskSet却不相同的情况
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }

      // 将刚创建的TaskSetManager添加到调度池构建器的调度池中
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) { // 当前应用程序不是Local模式并且TaskSchedulerImpl还没有接收到Task
        // 设置检查TaskSchedulerImpl的饥饿状况的定时器
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          // 定时检查TaskSchedulerImpl的饥饿状况
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              // 当TaskSchedulerImpl已经运行Task后，取消此定时器
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      // 表示TaskSchedulerImpl已经接收到Task
      hasReceivedTask = true
    }
    // 给Task分配资源并运行Task
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        tsm.runningTasksSet.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId, interruptThread)
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    // 获取对应的Stage对应的Stage Attempt及TaskSetManager
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      // 移除此次Stage Attempt
      taskSetsForStage -= manager.taskSet.stageAttemptId
      // 如果没有剩余的Stage Attempt，就将该Stage从taskSetsByStageIdAndAttempt中移除
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    // 从TaskSetManager的所属调度池中移除自己
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
   * 为TaskSetManager中的Task分配资源
   * @param taskSet 目标TaskSetManager
   * @param maxLocality 指定本次分配过程中该TaskSetManager支持的最高本地化级别
   * @param shuffledOffers 可供使用的Executor资源集合
   * @param availableCpus 每个Executor可供使用的CPU Core数量
   * @param tasks 最终分配到资源的Task对应的TaskDescription数组
   * @return 是否有Task被分配了资源，该返回值会决定
   */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    // 将遍历WorkerOffer序列，每个WorkerOffer表示一个可供调度的Executor
    for (i <- 0 until shuffledOffers.size) {
      // 获取WorkerOffer的Executor的身份标识
      val execId = shuffledOffers(i).executorId
      // 获取WorkerOffer的Host
      val host = shuffledOffers(i).host

      /**
       * WorkerOffer的可用的CPU核数大于等于CPUS_PER_TASK才可以继续分配，
       * CPUS_PER_TASK由spark.task.cpus参数配置，默认为1。
       */
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          // 给符合条件的待处理Task创建TaskDescription
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            // 将TaskDescription添加到tasks数组
            tasks(i) += task
            // 更新Task的身份标识与TaskSet、Executor的身份标识相关的缓存映
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            /**
              * 由于给Task分配了CPUS_PER_TASK指定数量的CPU内核数，
              * 因此WorkerOffer的可用的CPU核数减去CPUS_PER_TASK
              */
            availableCpus(i) -= CPUS_PER_TASK
            // 防止CPU Core超额分配
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    // 返回launchedTask，即是否已经给TaskSet中的某个Task分配到了资源
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   *
   * 用于给Task分配资源
   */
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    // 遍历WorkerOffer序列
    for (o <- offers) {
      // 先将资源中的主机记录更新到hostToExecutors字典中
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      // 更新Host与Executor的各种映射关系
      if (!executorIdToRunningTaskIds.contains(o.executorId)) { // 说明Executor是新添加的
        hostToExecutors(o.host) += o.executorId
        /**
         * 向DAGScheduler的DAGSchedulerEventProcessLoop投递ExecutorAdded事件
         * 告知有新的Executor添加了
         */
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        // 标记添加了新的Executor
        newExecAvail = true
      }
      // 更新Host与机架之间的关系
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    // 随机洗牌，避免将任务总是分配给同样一组Worker
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    /**
     * 根据每个WorkerOffer的可用的CPU核数创建同等尺寸的TaskDescription数组
     * 从这里可以看出，每个CPU Core只供给一个Task使用
     */
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    // 统计每个Worker的可用的CPU核数
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    // 对rootPool中所有TaskSetManager按照调度算法排序
    val sortedTaskSets = rootPool.getSortedTaskSetQueue

    // 遍历所有的TaskSetManager，如果有新的Executor添加就告诉它们，它们会重新计算支持的本地性级别。
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, numTasks: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.numTasks, taskSet.runningTasks))
      if (newExecAvail) {
        // 通知有新的Executor添加了，以触发TaskSetManager重新计算TaskSet的本地性
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    /**
     * 遍历TaskSetManager，
     * 在单个TaskSetManager中，按照最大本地性的原则（即从高本地性级别到低本地性级别）
     * 调用resourceOfferSingleTaskSet()方法，给单个TaskSet中的Task提供资源
     */
    for (taskSet <- sortedTaskSets) { // 循环根据调度算法排好序的待执行Task
      var launchedAnyTask = false
      var launchedTaskAtCurrentMaxLocality = false
      // 对单个TaskSetManager，遍历它所支持的的本地化级别，按照最大本地性的原则，给Task提供资源
      for (currentMaxLocality <- taskSet.myLocalityLevels) {
        do {
          /**
           * 调用resourceOfferSingleTaskSet()方法为单个TaskSetManager分配资源，
           * 最终分配到资源的Task对应的TaskDescription会被放入到tasks数组中，
           * 返回值表示是否有Task被分配了资源
           */
          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
        } while (launchedTaskAtCurrentMaxLocality)
      }
      // 如果在任何TaskSet所允许的本地性级别下，TaskSet中没有任何一个任务获得了资源
      if (!launchedAnyTask) {
        // 调用TaskSetManager的abortIfCompletelyBlacklisted方法，放弃在黑名单中的Task。
        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
      }
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    // 返回已经获得了资源的TaskDescription列表
    return tasks
  }

  /**
   * 更新Task的状态
   * @param tid Task的ID
   * @param state Task的最新状态
   * @param serializedData 序列化后的Task计算结果数据
   */
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        // 从taskIdToTaskSetManager中获取Task对应的TaskSetManager
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) => // 能读取到TaskSetManager
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              // 从taskIdToExecutorId中获取Task对应的Executor的身份标识
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) { // 此Executor上正在运行Task
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                /**
                 * 移除Executor，移除的原因是SlaveLost，
                 * 需要注意的是，SlaveLost的第二个参数为false，表示不是因为Worker丢失而导致的，
                 * Worker丢失与Executor丢失的区别在于，Executor丢失不会影响到计算结果数据，
                 * 而Worker丢失会导致其上的计算产生的结果数据也会丢失，因此需要重新计算。
                 */
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            if (TaskState.isFinished(state)) { // 要更新的任务状态是完成状态：FINISHED、FAILED、KILLED、LOST
              // 清除Task在taskIdToTaskSetManager、taskIdToExecutorId中的数据
              cleanupTaskState(tid)
              // 减少正在运行的任务数量
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                // 对执行成功的Task的结果进行处理
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                // 对执行失败的Task的结果进行处理
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    // 妥善安置丢失的Executor上正在运行的Task
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      /**
        * 向DAGSchedulerEventProcessLoop投递了ExecutorLost消息，
        * DAGSchedulerEventProcessLoop处理ExecutorLost消息时，
        * 将调用DagScheduler的handleExecutorLost()方法对丢失的Executor作进一步处理
        */
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      // 给Task分配资源并运行Task
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   *
   * @param execId Executor ID
   * @param accumUpdates 元素类型为二元元组的数组，每个元组记录者Task和所属于该Task的相关累加器
   * @param blockManagerId BlockManager的BlockManagerId
   * @return 返回true表示Driver可以识别传入的BlockManager，否则说明不识别，需要该BlockManager进行重新注册
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    // 更新Task相关的度量信息
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    // 通知DAGScheduler
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    // 调用了SchedulerBackend的stop()方法
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      // 使用根调度池的checkSpeculatableTasks()方法检测
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    // 如果检查到有可以推测执行的任务，则调用SchedulerBackend的reviveOffers方法
    if (shouldRevive) {
      /**
        * Local模式下LocalSchedulerBackend的reviveOffers()方法将向LocalEndpoint发送ReviveOffers消息，
        * LocalEndpoint接收到ReviveOffers消息后将调用LocalEndpoint的reviveOffers()方法分配资源并运行Task。
        */
      backend.reviveOffers()
    }
  }

  // 用于处理Executor丢失
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        /**
          * 如果executorIdToRunningTaskIds中包含指定的Executor的身份标识，
          * 说明此时在此Executor上已经有Task正在运行
          */
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        // 移除Executor
        removeExecutor(executorId, reason)
        // 将failedExecutor设置为此Executor
        failedExecutor = Some(executorId)
      } else {
        /**
          * 如果executorIdToRunningTaskIds中不包含指定的Executor的身份标识，
          * 这说明此时在此Executor上没有Task正在运行
          */
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            // 移除Executor
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      // 如果failedExecutor设置了Executor的身份标识，这说明此Executor已经被移除
      /**
        * 向DAGSchedulerEventProcessLoop投递了ExecutorLost消息，
        * DAGSchedulerEventrocessLoop处理ExecutorLost消息时，
        * 将调用DagScheduler的handleExecutorLost方法对丢失的Executor作进一步处理。
        */
      dagScheduler.executorLost(failedExecutor.get, reason)
      // 给Task分配资源并运行Task
      backend.reviveOffers()
    }
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
    *
    * 移除Executor及其对应的Host和机架等缓存信息
    *
    * @param executorId
    * @param reason 有四种子类：
    *                 - SlaveLost：Worker或Executor丢失。
    *                 - LossReasonPending：未知的原因导致的Executor退出。
    *                 - ExecutorKilled：Executor被杀死了。
    *                 - ExecutorExited：Executor退出了。
    */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    // 从executorIdToRunningTaskIds移除Executor的缓存
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      /**
        * 调用cleanupTaskState方法清除在此Executor上正在运行的
        * Task在taskIdToTaskSetManager、taskIdToExecutorId等缓存中的数据。
        */
      taskIds.foreach(cleanupTaskState)
    }

    // 从hostToExecutors中移除此Executor的信息
    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId

    /**
      * 如果hostToExecutors中此Executor所在的Host主机已经没有任何Executor了，
      * 那么从hostsByRack中移除此Host的信息。
      */
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        /**
          * 如果hostsByRack中此Executor所在的机架已经没有任何Host了，
          * 那么从hostsByRack中移除此机架的信息。
          */
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    if (reason != LossReasonPending) { // 如果移除Executor的原因不是LossReasonPending
      // 首先从executorIdToHost中移除此Executor的缓存
      executorIdToHost -= executorId
      /**
        * 调用根调度池rootPool的executorLost()方法将在此Executor上
        * 正在运行的Task作为失败任务处理，最后重新提交这些任务。
        */
      rootPool.executorLost(executorId, host, reason)
    }
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
