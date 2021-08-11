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

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}


private[netty] sealed trait InboxMessage

// RpcEndpoint处理此类型的消息后不需要向客户端回复信息。
private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

// RPC消息，RpcEndpoint处理完此消息后需要向客户端回复信息。
private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

// 用于Inbox实例化后，再通知与此Inbox相关联的RpcEndpoint启动。
private[netty] case object OnStart extends InboxMessage

// 用于Inbox停止后，通知与此Inbox相关联的RpcEndpoint停止。
private[netty] case object OnStop extends InboxMessage

/**
  * A message to tell all endpoints that a remote process has connected.
  * 此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务建立了连接。
  **/
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/**
  * A message to tell all endpoints that a remote process has disconnected.
  * 此消息用于告诉所有的RpcEndpoint，有远端的进程已经与当前RPC服务断开了连接。
  **/
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/**
  * A message to tell all endpoints that a network error has happened.
  * 此消息用于告诉所有的RpcEndpoint，与远端某个地址之间的连接发生了错误。
  **/
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
  *
  * 端点内的盒子。
  * 每个RpcEndpoint都有一个对应的盒子，这个盒子里有个存储InboxMessage消息的列表messages。
  * 所有的消息将缓存在messages列表里面，并由RpcEndpoint异步处理这些消息。
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  // 使用链表容器保存Box内的消息
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /**
    * True if the inbox (and its associated endpoint) is stopped.
    * 标识当前Box是否停止
    **/
  @GuardedBy("this")
  private var stopped = false

  /**
    * Allow multiple threads to process messages at the same time.
    * 是否允许线程并发访问
    **/
  @GuardedBy("this")
  private var enableConcurrent = false

  /**
    * The number of threads processing messages for this inbox.
    * 处理Box中消息的线程数量
    **/
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  // InBox启动时，会默认放入一条OnStart消息
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
    * 处理消息
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized { // 并发控制
      // 如果不允许并发操作，但已激活线程数不为0，则说明已有线程在处理消息
      if (!enableConcurrent && numActiveThreads != 0) {
        // 直接返回
        return
      }
      // 从链表头取出消息
      message = messages.poll()
      if (message != null) { // 消息不为空
        // 激活线程数自增
        numActiveThreads += 1
      } else {
        // 消息为空，直接返回
        return
      }
    }

    // 走到这里说明取到消息了
    while (true) {
      // 第二个参数action是传名参数
      safelyCall(endpoint) { // 对下面操作中出现非致命的异常，都会传递给endpoint的onError()方法
        // 根据消息类型进行匹配，分别处理
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              // 发送并要求回复
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            // 发送不要求回复
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            // 收到Inbox的OnStart消息，调用RpcEndpoint的onStart()方法告知该RpcEndpoint已启动
            endpoint.onStart()
            // endpoint不是要求线程安全的RpcEndpoint
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  // 则允许Inbox的并发操作
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            // 激活线程数
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            // 从Dispatcher中移除对应的RpcEndpoint
            dispatcher.removeRpcEndpointRef(endpoint)
            // 调用RpcEndpoint的onStop()方法告知该RpcEndpoint已暂停
            endpoint.onStop()
            // OnStop消息应该是最后一条消息
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            // 调用RpcEndpoint的onConnected()方法告知该RpcEndpoint收到远程连接
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            // 调用RpcEndpoint的onDisconnected()方法告知该RpcEndpoint断开远程连接
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            // 调用RpcEndpoint的onNetworkError()方法告知该RpcEndpoint处理连接错误
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized { // 加锁
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        // 不允许并发操作且激活线程数为1时
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          // 需要需要减少激活线程数
          numActiveThreads -= 1
          return
        }
        // 再次尝试取出一条消息，如果取不到则将激活线程数再减1
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      // Inbox停止了，丢弃消息
      onDrop(message)
    } else {
      // 放入message队列
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      // 不允许并发操作
      enableConcurrent = false
      // 停止Inbox
      stopped = true
      // 添加OnStop消息
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    // 尝试执行操作
    try action catch {
      // 如果遇到非致命异常，调用RpcEndpoint的onError()方法处理
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => logError(s"Ignoring error", ee)
        }
    }
  }

}
