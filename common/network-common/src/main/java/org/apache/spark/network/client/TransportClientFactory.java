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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 *
 * 创建TransportClient的传输客户端工厂类。
 */
public class TransportClientFactory implements Closeable {

  /**
   * A simple data structure to track the pool of clients between two peer nodes.
   * 在两个对等节点间维护的关于TransportClient的池子。
   **/
  private static class ClientPool {
    TransportClient[] clients;
    // 该集合中的对象与clients集合中的对象按照索引对应，提供锁对象
    Object[] locks;

    ClientPool(int size) {
      // 这里会创建clients集合
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  // 对参数传递的TransportContext的引用
  private final TransportContext context;
  // TransportConf实例，可以通过调用TransportContext的getConf获取
  private final TransportConf conf;
  // 参数传递的TransportClientBootstrap列表
  private final List<TransportClientBootstrap> clientBootstraps;

  // 针对每个Socket地址的连接池
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /**
   * Random number generator for picking connections between peers.
   * 对Socket地址对应的连接池ClientPool中缓存的TransportClient进行随机选择，对每个连接做负载均衡。
   * */
  private final Random rand;
  /**
   * 每个远程地址的ClientPool池内最多可以存放的TransportClient的数量
   * 由TransportConf的spark.模块名.io.numConnectionsPerPeer参数配置，默认为1
   * 模块名实际为TransportConf的module字段
   */
  private final int numConnectionsPerPeer;

  // 客户端Channel被创建时使用的类
  private final Class<? extends Channel> socketChannelClass;
  // 根据Netty的规范，客户端只有worker组，所以此处创建workerGroup。
  private EventLoopGroup workerGroup;
  // 汇集ByteBuf但对本地线程缓存禁用的分配器。
  private PooledByteBufAllocator pooledAllocator;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    // 赋值初始化各个字段
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = new ConcurrentHashMap<>();
    /**
     * 每个远程地址的ClientPool池内最多可以存放的TransportClient的数量
     * 由spark.模块名.io.numConnectionsPerPeer参数配置，默认为1
     */
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    // IO模式，即从TransportConf获取key为“spark.模块名.io.mode”的属性值。默认值为NIO，Spark还支持EPOLL
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    // 根据IOMode创建SocketChannel，NIO模式为NioSocketChannel，Epoll模式为EpollSocketChannel
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    // TODO: Make thread pool name configurable.
    // 根据IOMode、配置的客户端线程数创建EventLoopGroup
    this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
    // 创建ByteBuf池分配器
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   *
   * 创建连接远程Server的客户端
   *
   * @param remoteHost 远程Server主机名
   * @param remotePort 远程Server端口
   * @return
   * @throws IOException
   */
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    // 主机和IP地址封装为InetSocketAddress
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    // 尝试从connectionPool中获取该地址对应的ClientPool，如果没有就创建一个新的
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      /**
       * 使用“spark.模块名.io.numConnectionsPerPeer”属性配置获取numConnectionsPerPeer，
       * ClientPool的构造过程中会创建其clients集合
       */
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
    }

    // 随机选择一个TransportClient
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    // 获取并返回激活的TransportClient
    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      /**
       * 更新TransportClient的Channel中配置的TransportChannelHandler内部的TransportResponseHandler
       * 最后一次使用时间，确保TransportClient没有超时
       */
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      // 检查TransportClient是否是激活状态
      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    // 走到这里，说明ClientPool中没有缓存的TransportClient
    // 构造远程连接地址的InetSocketAddress，同时侦测DNS解析时间
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    // 由于此处可能有多个线程同时处理，为每个ClientPool进行加锁，确保对应的ClientPool不会出现线程安全问题
    synchronized (clientPool.locks[clientIndex]) {
      // 先获取
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) { // 不为空
        // 检查是否激活
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          // 如果激活就返回
          return cachedClient;
        } else {
          // 否则只是打印日志
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      // 创建一个TransportClient放置到池中对应的索引位置
      clientPool.clients[clientIndex] = createClient(resolvedAddress);
      // 返回TransportClient
      return clientPool.clients[clientIndex];
    }
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address);
  }

  /** Create a completely new {@link TransportClient} to the remote address.
   * 使用TransportClientFactory创建TransportClient对象，
   * 内部会创建Netty的Bootstrap，并连接到指定的远程地址。
   **/
  private TransportClient createClient(InetSocketAddress address) throws IOException {
    logger.debug("Creating new connection to {}", address);

    // 构建根引导程序Bootstrap应对其进行配置
    Bootstrap bootstrap = new Bootstrap();
    // 设置线程组
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    // 记录TransportClient和Channel
    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    // 为根引导程序设置管道初始化回调函数
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      // 该回调方法会在bootstrap连接到远程服务器时被调用
      @Override
      public void initChannel(SocketChannel ch) {
        /**
         * 根据Channel初始化TransportContext的Pipeline
         * 在该过程中会创建TransportChannelHandler对象，
         * 而创建TransportChannelHandler会关联TransportResponseHandler和TransportRequestHandler及TransportClient三个对象。
         * 这里会获取其中的TransportClient，该TransportClient关联了Channel对象。
         */
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        // 记录TransportClient和Channel
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    // 使用根引导程序连接远程服务器
    ChannelFuture cf = bootstrap.connect(address);

    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    // 获取TransportClient和Channel
    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();

    logger.trace(">>> TransportClient Channel localAddress: {}", channel.localAddress());
    logger.trace(">>> TransportClient Channel remoteAddress: {}", channel.remoteAddress());

    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);

    // 遍历clientBootstraps，执行每个引导程序的doBootstrap()方法
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        // 给TransportClient设置客户端引导程序
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
