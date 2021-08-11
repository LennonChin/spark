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

package org.apache.spark.network.util;

import com.google.common.primitives.Ints;

/**
 * A central location that tracks all the settings we expose to users.
 * 用于记录传输上下文TransportContext对象的配置信息
 */
public class TransportConf {

  static {
    // Set this due to Netty PR #5661 for Netty 4.0.37+ to work
    System.setProperty("io.netty.maxDirectMemory", "0");
  }

  private final String SPARK_NETWORK_IO_MODE_KEY;
  private final String SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY;
  private final String SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_BACKLOG_KEY;
  private final String SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
  private final String SPARK_NETWORK_IO_SERVERTHREADS_KEY;
  private final String SPARK_NETWORK_IO_CLIENTTHREADS_KEY;
  private final String SPARK_NETWORK_IO_RECEIVEBUFFER_KEY;
  private final String SPARK_NETWORK_IO_SENDBUFFER_KEY;
  private final String SPARK_NETWORK_SASL_TIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_MAXRETRIES_KEY;
  private final String SPARK_NETWORK_IO_RETRYWAIT_KEY;
  private final String SPARK_NETWORK_IO_LAZYFD_KEY;

  // 真正的配置提供者
  private final ConfigProvider conf;
  // 配置的模块名称
  private final String module;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
    SPARK_NETWORK_IO_MODE_KEY = getConfKey("io.mode"); // spark.[module].io.mode
    SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs"); // spark.[module].io.preferDirectBufs
    SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout"); // spark.[module].io.connectionTimeout
    SPARK_NETWORK_IO_BACKLOG_KEY = getConfKey("io.backLog"); // spark.[module].io.backLog
    SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY =  getConfKey("io.numConnectionsPerPeer"); // spark.[module].io.numConnectionsPerPeer
    SPARK_NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads"); // spark.[module].io.serverThreads
    SPARK_NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads"); // spark.[module].io.clientThreads
    SPARK_NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer"); // spark.[module].io.receiveBuffer
    SPARK_NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer"); // spark.[module].io.sendBuffer
    SPARK_NETWORK_SASL_TIMEOUT_KEY = getConfKey("sasl.timeout"); // spark.[module].sasl.timeout
    SPARK_NETWORK_IO_MAXRETRIES_KEY = getConfKey("io.maxRetries"); // spark.[module].io.maxRetries
    SPARK_NETWORK_IO_RETRYWAIT_KEY = getConfKey("io.retryWait"); // spark.[module].io.retryWait
    SPARK_NETWORK_IO_LAZYFD_KEY = getConfKey("io.lazyFD"); // spark.[module].io.lazyFD
  }

  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }

  private String getConfKey(String suffix) {
    return "spark." + module + "." + suffix;
  }

  /** IO mode: nio or epoll */
  public String ioMode() { return conf.get(SPARK_NETWORK_IO_MODE_KEY, "NIO").toUpperCase(); }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  public boolean preferDirectBufs() {
    return conf.getBoolean(SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY, true);
  }

  /** Connect timeout in milliseconds. Default 120 secs. */
  public int connectionTimeoutMs() {
    // 默认为120s
    long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(
      conf.get("spark.network.timeout", "120s"));
    // spark.模块.io.connectionTimeout
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, defaultNetworkTimeoutS + "s")) * 1000;
    return (int) defaultTimeoutMs;
  }

  /** Number of concurrent connections between two nodes for fetching data. */
  public int numConnectionsPerPeer() {
    return conf.getInt(SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
  }

  /** Requested maximum length of the queue of incoming connections. Default -1 for no backlog. */
  public int backLog() { return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY, -1); }

  /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
  public int serverThreads() { return conf.getInt(SPARK_NETWORK_IO_SERVERTHREADS_KEY, 0); }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  public int clientThreads() { return conf.getInt(SPARK_NETWORK_IO_CLIENTTHREADS_KEY, 0); }

  /**
   * Receive buffer size (SO_RCVBUF).
   * Note: the optimal size for receive buffer and send buffer should be
   *  latency * network_bandwidth.
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   *  buffer size should be ~ 1.25MB
   */
  public int receiveBuf() { return conf.getInt(SPARK_NETWORK_IO_RECEIVEBUFFER_KEY, -1); }

  /** Send buffer size (SO_SNDBUF). */
  public int sendBuf() { return conf.getInt(SPARK_NETWORK_IO_SENDBUFFER_KEY, -1); }

  /** Timeout for a single round trip of SASL token exchange, in milliseconds. */
  public int saslRTTimeoutMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_SASL_TIMEOUT_KEY, "30s")) * 1000;
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request.
   * If set to 0, we will not do any retries.
   */
  public int maxIORetries() { return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY, 3); }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
   * Only relevant if maxIORetries &gt; 0.
   */
  public int ioRetryWaitTimeMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY, "5s")) * 1000;
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  public int memoryMapBytes() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.storage.memoryMapThreshold", "2m")));
  }

  /**
   * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are
   * created only when data is going to be transferred. This can reduce the number of open files.
   *
   * 是否以懒加载的方式初始化FileDescriptor，
   * 如果是，FileDescriptor只会在数据将要被传输时创建，
   * 这种方式可以减少打开文件的数量
   */
  public boolean lazyFileDescriptor() {
    // spark.模块.io.lazyFD，默认为true
    return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY, true);
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  public int portMaxRetries() {
    return conf.getInt("spark.port.maxRetries", 16);
  }

  /**
   * Maximum number of bytes to be encrypted at a time when SASL encryption is enabled.
   */
  public int maxSaslEncryptedBlockSize() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.network.sasl.maxEncryptedBlockSize", "64k")));
  }

  /**
   * Whether the server should enforce encryption on SASL-authenticated connections.
   */
  public boolean saslServerAlwaysEncrypt() {
    return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
  }

}
