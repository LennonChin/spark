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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  // RPC端点数据，Inbox与RpcEndpoint、NettyRpcEndpointRef通过此EndpointData相关联。
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  // 端点实例名称与端点数据EndpointData之间映射关系的缓存，可以使用端点名称从中快速获取或删除EndpointData。
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  // 端点实例RpcEndpoint与端点实例引用RpcEndpointRef之间映射关系的缓存，可以使用端点实例从中快速获取或删除端点实例引用。
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  // 存储端点数据EndpointData的阻塞队列。只有Inbox中有消息的EndpointData才会被放入此阻塞队列。
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
    *
    * Dispatcher是否停止的状态
   */
  @GuardedBy("this")
  private var stopped = false

  // 注册RpcEndpoint
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    // 根据RpcEndpoint所在NettyRpcEnv的地址和名称构造RpcEndpointAddress对象
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    // 构造NettyRpcEndpointRef对象
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      // 检查状态
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      /**
        * 将RpcEndpoint、NettyRpcEndpointRef包装为EndpointData对象，
        * 并放入endpoints字典中（不存在才放入），如果返回值不为null说明已经存在了同名的
        */
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }

      // 放入成功，还需要将对应的RpcEndpoint和RpcEndpointRef存入endpointRefs字典
      val data = endpoints.get(name)
      endpointRefs.put(data.endpoint, data.ref)

      /**
        * 将EndpointData放入到receivers队尾，MessageLoop线程异步获取到此EndpointData，
        * 并处理其Inbox中刚刚放入的OnStart消息，注意该OnStart消息是在Inbox初始化时放入的
        * 最终调用RpcEndpoint的OnStart方法在RpcEndpoint开始处理消息之前做一些准备工作。
        */
      receivers.offer(data)  // for the OnStart message
    }
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    // 从endpoints字典移除，得到移除的EndpointData
    val data = endpoints.remove(name)
    if (data != null) {
      // 停止EndpointData中的Inbox
      data.inbox.stop()
      /**
        * 将EndpointData放入receivers，
        * 注意，在Inbox的stop()方法中，会向自己的Message队列放入一个OnStop消息，
        * Inbox在处理OnStop消息时，会调用Dispatcher的removeRpcEndpointRef()方法移除对应的RpcEndpoint，
        * 并调用RpcEndpoint的onStop()方法告知该RpcEndpoint已暂停，
        * 可以参考 {@link Inbox#process} 方法处理OnStop消息的分支
        */
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  // 取消RpcEndpoint的注册
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      // 调用unregisterRpcEndpoint()取消注册
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => logWarning(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  /**
    * Posts a message sent by a remote endpoint.
    * 投递来自远程RpcEndpoint发送的消息
    **/
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    // 回调上下文
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    // 构造RpcMessage，包装了回调上下文
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    // 使用postMessage()方法发送
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /**
    * Posts a message sent by a local endpoint.
    * 投递来自本地RpcEndpoint发送的消息
    **/
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    // 回调上下文
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    // 构造RPCMessage，包装了回调上下文
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    // 使用postMessage()方法发送
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message.
    * 投递不需要回复的RPC消息
    **/
  def postOneWayMessage(message: RequestMessage): Unit = {
    // 使用postMessage()方法发送
     postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
    *
    * 将消息投递给特定的RpcEndpoint
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    logTrace(s">>> postMessage endpointName: ${endpointName}")
    logTrace(s">>> postMessage message: ${message}")
    val error = synchronized { // 加锁
      // 获取对应的EndpointData
      val data = endpoints.get(endpointName)
      if (stopped) { // 判断Dispatcher是否在运行
        Some(new RpcEnvStoppedException())
      } else if (data == null) { // 获取的EndpointData为空
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 将消息添加到EndpointData中的Inbox中
        data.inbox.post(message)
        // 将EndpointData放入receivers队列等待处理
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    // 有错误，交给回调
    error.foreach(callbackIfStopped)
  }

  // 停止Dispatcher
  def stop(): Unit = {
    synchronized {
      // 如果已经停止则直接返回
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    // 将endpoints中所有的EndpointData全部移除，该操作会停止对应的Inbox
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    // 向receivers队尾放入"毒药"消息，它会控制关闭MessageLoop线程
    receivers.offer(PoisonPill)
    // 关掉线程池
    threadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
    * 查找对应名称的RpcEndpoint是否存在
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /**
    * Thread pool used for dispatching messages.
    * 用于对消息进行调度的线程池。
    * 此线程池运行的任务都是MessageLoop线程任务
    **/
  private val threadpool: ThreadPoolExecutor = {
    // 调度线程数
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, Runtime.getRuntime.availableProcessors()))
    // 创建固定线程数线程池，线程名前缀为dispatcher-event-loop
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    // 调度MessageLoop线程对象
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) { // 不断循环
          try {
            // 从receivers中取出EndpointData对象
            val data = receivers.take()
            if (data == PoisonPill) { // PoisonPill意思是"毒药"，用于终止当前线程
              // Put PoisonPill back so that other MessageLoops can see it.
              // 取出的是"毒药"，重新放入队列，以便终止其它线程
              receivers.offer(PoisonPill)
              // 直接返回，终止当前线程
              return
            }
            // 处理消息
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop.
    * 毒药消息
    **/
  private val PoisonPill = new EndpointData(null, null, null)
}
