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

package org.apache.spark.network;

import java.util.List;

import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 *
 * 传输上下文，包含了用于创建传输服务端（TransportServer）和传输客户端工厂（TransportClientFactory）的上下文信息，
 * 并支持使用TransportChannelHandler设置Netty提供的SocketChannel的Pipeline的实现。
 */
public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  // 传输上下文的配置信息
  private final TransportConf conf;
  // 发送消息的处理器
  private final RpcHandler rpcHandler;
  // 标记是否关闭空闲连接
  private final boolean closeIdleConnections;

  // 消息编码器
  private final MessageEncoder encoder;
  // 消息解码器
  private final MessageDecoder decoder;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.encoder = new MessageEncoder();
    this.decoder = new MessageDecoder();
    this.closeIdleConnections = closeIdleConnections;
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   *
   * 创建TransportClientFactory工厂类
   *
   * @param bootstraps 客户端引导程序TransportClientBootstrap的列表
   * @return
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(Lists.<TransportClientBootstrap>newArrayList());
  }

  /**
   * Create a server which will attempt to bind to a specific port.
   * 构造传输服务端TransportServer的实例，绑定到特定端口
   * */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port.
   * 构造传输服务端TransportServer的实例，绑定到特定主机名和端口
   **/
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /**
   * Creates a new server, binding to any available ephemeral port.
   * 构造传输服务端TransportServer的实例，绑定到任意可用的临时端口
   **/
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, Lists.<TransportServerBootstrap>newArrayList());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      // 创建TransportChannelHandler，InboundHandler
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      // 添加Handler
      channel.pipeline()
        .addLast("encoder", encoder) // 消息编码，MessageEncoder，OutboundHandler
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder()) // InboundHandler
        .addLast("decoder", decoder) // 消息解码，MessageDecoder，InboundHandler
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000)) // 心跳检测，OutboundHandler，ChannelInboundHandler
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler); // InboundHandler

      logger.trace(">>> TransportServer Channel localAddress: {}", channel.localAddress());
      logger.trace(">>> TransportServer Channel remoteAddress: {}", channel.remoteAddress());

      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   *
   * TransportChannelHandler在服务端将代理Transport-RequestHandler对请求消息进行处理，
   * 并在客户端代理TransportResponseHandler对响应消息进行处理。
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    // 创建处理传输响应的处理器
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    // 创建TransportClient
    TransportClient client = new TransportClient(channel, responseHandler);
    // 创建处理传输请求的处理器
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client, rpcHandler);

    logger.trace(" >>> TransportResponseHandler: {}", responseHandler);
    logger.trace(" >>> TransportRequestHandler: {}", requestHandler);
    logger.trace(" >>> TransportClient: {}", client.hashCode());

    // 将TransportClient、TransportResponseHandler和TransportRequestHandler绑定在一个TransportChannelHandler上
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }

  public TransportConf getConf() { return conf; }
}
