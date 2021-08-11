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

import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.spark.network.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 *
 * 用于处理客户端的请求并在写完块数据后返回的处理程序。
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  private final Channel channel;

  /** Client on the same channel allowing us to talk back to the requester. */
  private final TransportClient reverseClient;

  /** Handles all RPC messages.
   * 用于处理RPC消息
   **/
  private final RpcHandler rpcHandler;

  /** Returns each chunk part of a stream.
   * 用于处理流请求消息
   **/
  private final StreamManager streamManager;

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    rpcHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelActive() {
    rpcHandler.channelActive(reverseClient);
  }

  @Override
  public void channelInactive() {
    if (streamManager != null) {
      try {
        streamManager.connectionTerminated(channel);
      } catch (RuntimeException e) {
        logger.error("StreamManager connectionTerminated() callback failed.", e);
      }
    }
    rpcHandler.channelInactive(reverseClient);
  }


  // 根据请求类型处理各类请求
  @Override
  public void handle(RequestMessage request) {
    logger.trace(">>> handle() " + request.type());
    if (request instanceof ChunkFetchRequest) {
      // 处理块获取请求
      processFetchRequest((ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      // 处理需要回复的RPC请求
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      // 处理无需回复的RPC请求
      processOneWayMessage((OneWayMessage) request);
    } else if (request instanceof StreamRequest) {
      // 处理流请求
      processStreamRequest((StreamRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  // 处理块获取请求
  private void processFetchRequest(final ChunkFetchRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        req.streamChunkId);
    }

    ManagedBuffer buf;
    try {
      // 检查权限，校验客户端是否有权限从给定的流读取数据
      streamManager.checkAuthorization(reverseClient, req.streamChunkId.streamId);
      // 将流与客户端的一个TCP连接进行关联，保证对于单个的流只会有一个客户端读取，流关闭后就不能重用了
      streamManager.registerChannel(channel, req.streamChunkId.streamId);
      // 获取单个的块，被封装为ManagedBuffer对象，不能并行调用
      buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        req.streamChunkId, getRemoteAddress(channel)), e);
      // 读取出错，封装为ChunkFetchFailure后由respond()方法返回
      respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
      return;
    }

    // 读取成功，封装为ChunkFetchSuccess后由respond()方法返回
    respond(new ChunkFetchSuccess(req.streamChunkId, buf));
  }

  // 处理流请求
  private void processStreamRequest(final StreamRequest req) {
    ManagedBuffer buf;
    try {
      // 使用StreamManager将获取到的流数据封装为ManagedBuffer
      buf = streamManager.openStream(req.streamId);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
      // 失败时将响应包装为StreamFailure进行响应
      respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
      return;
    }

    if (buf != null) {
      // 成功时将响应包装为StreamResponse进行响应
      respond(new StreamResponse(req.streamId, buf.size(), buf));
    } else {
      // 失败时将响应包装为StreamFailure进行响应
      respond(new StreamFailure(req.streamId, String.format(
        "Stream '%s' was not found.", req.streamId)));
    }
  }

  // 处理需要回复的RPC请求
  private void processRpcRequest(final RpcRequest req) {
    try {
      /**
       * 将RpcRequest消息的内容体、发送消息的客户端及RpcResponseCallback回调传递给RpcHandler的receive方法
       * 具体的处理有RPCHandler具体的实现类的receive()方法处理，
       * 最终一定会调用RpcResponseCallback回调对象的相关方法响应处理
       */
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  // 处理无需回复的RPC请求
  private void processOneWayMessage(OneWayMessage req) {
    try {
      // 使用RpcHandler具体实现类的receive()方法处理，没有传入回调参数，即默认回调为ONE_WAY_CALLBACK，该回调仅打印日志
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
    } finally {
      req.body().release();
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private void respond(final Encodable result) {
    // 获取远程地址用于打印日志
    final SocketAddress remoteAddress = channel.remoteAddress();
    logger.trace(">>> respond to: {}, message type: {}", remoteAddress, ((Message)result).type());
    // 写出数据
    channel.writeAndFlush(result).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            logger.trace("Sent result {} to client {}", result, remoteAddress);
          } else {
            logger.error(String.format("Error sending result %s to %s; closing connection",
              result, remoteAddress), future.cause());
            channel.close();
          }
        }
      }
    );
  }
}
