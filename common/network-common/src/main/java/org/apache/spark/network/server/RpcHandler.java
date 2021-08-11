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

package org.apache.spark.network.server;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 * 对调用传输客户端（TransportClient）的sendRPC方法发送的消息进行处理的程序。
 * 定义了RPC处理器的规范。
 */
public abstract class RpcHandler {

  // 适用于OneWayRpcRequest，即无需响应的请求，该回调仅用于打印日志。
  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   *
   * This method will not be called in parallel for a single TransportClient (i.e., channel).
   *
   * 用来接收单一的RPC消息，具体处理逻辑需要子类去实现。
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC. 用于对请求处理结束后进行回调，无论处理结果是成功还是失败，该回调都会被调用一次
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   * 获取StreamManager，主要是用于跟踪当前正在被TransportClient拉取的流的状态。
   */
  public abstract StreamManager getStreamManager();

  /**
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   *
   * 处理无需响应的RPC请求。
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    // 默认传入的callback是ONE_WAY_CALLBACK，它的回调方法只打印了一些日志，没有其他操作
    receive(client, message, ONE_WAY_CALLBACK);
  }

  /**
   * Invoked when the channel associated with the given client is active.
   * 当与给定客户端相关联的Channel处于活动状态时调用
   */
  public void channelActive(TransportClient client) { }

  /**
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   * 当与给定客户端相关联的Channel处于非活动状态时调用
   */
  public void channelInactive(TransportClient client) { }

  // 当Channel产生异常时调用
  public void exceptionCaught(Throwable cause, TransportClient client) { }

  // 定义的无需回复的消息的回调
  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    // 仅仅记录日志
    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    // 仅仅记录日志
    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

}
