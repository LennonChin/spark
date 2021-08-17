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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.sasl.{SaslClientBootstrap, SaslServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at at time.
 *
  * @param conf SparkConf
  * @param securityManager 安全管理器
  * @param bindAddress 绑定的地址
  * @param hostName 绑定的主机名
  * @param _port 绑定的端口
  * @param numCores 使用的CPU Core数量
  */
private[spark] class NettyBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    bindAddress: String,
    override val hostName: String,
    _port: Int,
    numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  // Java序列化器；提示说明不要用Java序列化器，应该用一个版本兼容性更好的序列化器
  private val serializer = new JavaSerializer(conf)
  // 是否开启了安全认证
  private val authEnabled = securityManager.isAuthenticationEnabled()
  // 传输层TransportConf配置对象
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  // 传输层TransportContext上下文对象
  private[this] var transportContext: TransportContext = _
  // TransportServer服务端
  private[this] var server: TransportServer = _
  // 创建TransportClient的工厂
  private[this] var clientFactory: TransportClientFactory = _
  // BlockTransferService服务的应用的ID
  private[this] var appId: String = _

  // 初始化方法
  override def init(blockDataManager: BlockDataManager): Unit = {
    /**
      * 创建NettyBlockRpcServer，NettyBlockRpcServer继承了RpcHandler，
      * 服务端对客户端的Block读写请求的处理都交给了RpcHandler的实现类，
      * NettyBlockRpcServer将处理Block块的RPC请求。
      */
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    // 准备服务端和客户端的引导程序
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    // 如果开启了认证，需要在客户端和服务端分别添加支持认证的引导程序
    if (authEnabled) {
      serverBootstrap = Some(new SaslServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new SaslClientBootstrap(transportConf, conf.getAppId, securityManager,
        securityManager.isSaslEncryptionEnabled()))
    }
    // 创建TransportContext，使用NettyBlockRpcServer作为RpcHandler。
    transportContext = new TransportContext(transportConf, rpcHandler)
    // 创建TransportClientFactory
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    // 创建TransportClient
    server = createServer(serverBootstrap.toList)
    // 获取当前应用的ID
    appId = conf.getAppId
    logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports.
    * 创建TransportServer
    **/
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    // 定义启动TransportServer的方法
    def startService(port: Int): (TransportServer, Int) = {
      // 使用TransportContext创建服务端，绑定大特定的地址和端口上
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      // 返回TransportServer和绑定的端口
      (server, server.getPort)
    }

    // 这个方法用于真正创建和启动TransportServer，它会在端口被占用的情况下逐个重试启动
    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  // 作为默认的Shuffle客户端下载Blocks
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      // 创建RetryingBlockFetcher.BlockFetchStarter匿名对象
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          // 创建TransportClient
          val client = clientFactory.createClient(host, port)
          // 创建OneForOneBlockFetcher对象并调用其start()方法
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener).start()
        }
      }

      // 最大重试次数，由spark.模块.io.maxRetries实现决定，这里为spark.shuffle.io.maxRetries
      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) { // 重试次数大于0
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        // 创建RetryingBlockFetcher并调用start方法，传入了上面创建的RetryingBlockFetcher.BlockFetchStarter对象
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        /**
         * 调用blockFetchStarter（即RetryingBlockFetcher.BlockFetchStarter对象）的createAndStart()方法，
         * 本质是使用OneForOneBlockFetcher拉取一次，就算失败也不重试。
         */
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  /**
    * 上传Block
    * @param hostname 上传目的节点的主机地址
    * @param port 上传目的节点的端口
    * @param execId 上传目的节点的Executor ID
    * @param blockId 上传数据块的BlockId
    * @param blockData 上传数据块的数据
    * @param level 上传数据块的存储级别
    * @return
    */
  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = {
    // 创建一个空Promise，调用方将持有此Promise的Future
    val result = Promise[Unit]()
    // 创建TransportClient
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    // 将存储级别StorageLevel和类型标记classTag等元数据序列化
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // Convert or copy nio buffer into array in order to serialize it.
    // 调用ManagedBuffer（实际是实现类NettyManagedBuffer）的nioByteBuffer方法将Block的数据转换或者复制为Nio的ByteBuffer类型。
    val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

    /**
      * 调用TransportClient的sendRpc方法发送RPC消息UploadBlock，其中：
      *   - appId是应用程序的标识
      *   - execId是上传目的地的Executor的标识
      *
      * RpcResponseCallback是匿名的实现类。
      * 上传成功则回调匿名RpcResponseCallback的onSuccess方法，进而调用Promise的success方法；
      * 上传失败则回调匿名RpcResponseCallback的onFailure方法，进而调用Promise的failure方法。
      */
    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, array).toByteBuffer,
      new RpcResponseCallback {
        // 上传你成功
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          // 调用Promise的方法
          result.success((): Unit)
        }
        // 上传失败
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          // 调用Promise的方法
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
  }
}
