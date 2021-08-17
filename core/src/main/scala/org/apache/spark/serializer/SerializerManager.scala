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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * Component which configures serialization, compression and encryption for various Spark
 * components, including automatic selection of which [[Serializer]] to use for shuffles.
 *
  * @param defaultSerializer 默认的序列化器
  * @param encryptionKey 加密使用的密钥
  */
private[spark] class SerializerManager(
    defaultSerializer: Serializer,
    conf: SparkConf,
    encryptionKey: Option[Array[Byte]]) {

  def this(defaultSerializer: Serializer, conf: SparkConf) = this(defaultSerializer, conf, None)

  // 采用Google提供的Kryo序列化库实现
  private[this] val kryoSerializer = new KryoSerializer(conf)

  // 字符串类型标记
  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]

  // 原生类型及原生类型数组的类型标记的集合
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  // Whether to compress broadcast variables that are stored
  // 是否对广播对象进行压缩，可以通过spark.broadcast.compress属性配置，默认为true。
  private[this] val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  // 是否对Shuffle输出数据压缩，可以通过spark.shuffle.compress属性配置，默认为true。
  private[this] val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  // 是否对RDD压缩，可以通过spark.rdd.compress属性配置，默认为false。
  private[this] val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  // 是否对溢出到磁盘的Shuffle数据压缩，可以通过spark.shuffle.spill.compress属性配置，默认为true。
  private[this] val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet.
   * SerializerManager使用的压缩编解码器。延迟初始化。
   **/
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
    * 当前SerializerManager是否支持加密。
    * 要支持加密，必须在构造SerializerManager的时候就传入encryptionKey。
    * 可以通过
    *   spark.io.encryption.enabled（允许加密）、
    *   spark.io.encryption.keySizeBits（密钥长度，有128、192、256三种长度）、
    *   spark.io.encryption.keygen.algorithm（加密算法，默认为HmacSHA1）
    * 等属性进行具体的配置。
    */
  def encryptionEnabled: Boolean = encryptionKey.isDefined

  // 判断对于指定的类型标记ct，是否能使用kryoSerializer进行序列化。
  def canUseKryo(ct: ClassTag[_]): Boolean = {
    // 当类型标记ct属于基本类型、基本类型数组或者字符串类型时，canUseKryo方法才返回真。
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  // SPARK-18617: As feature in SPARK-13990 can not be applied to Spark Streaming now. The worst
  // result is streaming job based on `Receiver` mode can not run on Spark 2.x properly. It may be
  // a rational choice to close `kryo auto pick` feature for streaming in the first step.
  /**
    * 获取序列化器。
    * 如果autoPick为true（即BlockId不为StreamBlockId时），
    * 并且调用canUseKryo的结果为true时选择kryoSerializer，
    * 否则选择defaultSerializer。
    */
  def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
    if (autoPick && canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * Pick the best serializer for shuffling an RDD of key-value pairs.
    * 获取序列化器。
    * 如果对于keyClassTag和valueClassTag，
    * 调用canUseKryo的结果都为true时选择kryoSerializer，
    * 否则选择defaultSerializer。
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  // 不同类型的数据块是否能够被压缩
  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle // spark.shuffle.compress，默认true
      case _: BroadcastBlockId => compressBroadcast // spark.broadcast.compress，默认true
      case _: RDDBlockId => compressRdds // spark.rdd.compress，默认false
      case _: TempLocalBlockId => compressShuffleSpill // spark.shuffle.spill.compress，默认true
      case _: TempShuffleBlockId => compressShuffle // spark.rdd.compress，默认true
      case _ => false
    }
  }

  /**
   * Wrap an input stream for encryption and compression
    * 对Block的输入流进行压缩与加密。
   */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an output stream for encryption and compression
    * 对Block的输出流进行压缩与加密。
   */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an input stream for encryption if shuffle encryption is enabled
    * 对输入流进行加密。
   */
  def wrapForEncryption(s: InputStream): InputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for encryption if shuffle encryption is enabled
    * 对输出流进行加密。
   */
  def wrapForEncryption(s: OutputStream): OutputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
    * 对输出流进行压缩。
   */
  private[this] def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    // 对数据块进行判断是否需要被压缩，如果需要则使用compressionCodec包装为压缩输出流
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
    * 对输入流进行压缩
   */
  private[this] def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    // 对数据块进行判断是否需要被压缩，如果需要则使用compressionCodec包装为压缩输出流
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream.
    * 对Block的输出流序列化。
    **/
  def dataSerializeStream[T: ClassTag](
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[T]): Unit = {
    // 包装为缓冲输出流
    val byteStream = new BufferedOutputStream(outputStream)
    // 非SteamBlock数据块是使用Kryo序列化器的前提
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    // 获取序列化器实例
    val ser = getSerializer(implicitly[ClassTag[T]], autoPick).newInstance()
    // 使用序列化器实例进行序列化，并将values的数据写出到outputStream
    ser.serializeStream(wrapStream(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a chunked byte buffer.
    * 序列化为ChunkedByteBuffer
    **/
  def dataSerialize[T: ClassTag](blockId: BlockId, values: Iterator[T]): ChunkedByteBuffer = {
    // 调用dataSerializeWithExplicitClassTag()方法
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
  }

  /** Serializes into a chunked byte buffer.
    * 使用明确的类型标记，序列化为ChunkedByteBuffer
    **/
  def dataSerializeWithExplicitClassTag(
      blockId: BlockId,
      values: Iterator[_],
      classTag: ClassTag[_]): ChunkedByteBuffer = {
    // 包装为分块的字节缓冲输出流
    val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
    // 包装为缓冲输出流
    val byteStream = new BufferedOutputStream(bbos)
    // 非SteamBlock数据块是使用Kryo序列化器的前提
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    // 获取序列化器实例
    val ser = getSerializer(classTag, autoPick).newInstance()
    // 使用序列化器实例进行序列化，并将values的数据写出到bbos输出流
    ser.serializeStream(wrapStream(blockId, byteStream)).writeAll(values).close()
    // 将bbos输出流转换为ChunkedByteBuffer对象
    bbos.toChunkedByteBuffer
  }

  /**
   * Deserializes an InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
    * 将输入流反序列化为值的迭代器Iterator[T]。
   */
  def dataDeserializeStream[T](
      blockId: BlockId,
      inputStream: InputStream)
      (classTag: ClassTag[T]): Iterator[T] = {
    // 包装为缓冲输入流
    val stream = new BufferedInputStream(inputStream)
    // 非SteamBlock数据块是使用Kryo序列化器的前提
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    // 获取对应的序列化器实例，并使用该序列化器实例进行反序列化
    getSerializer(classTag, autoPick)
      .newInstance()
      .deserializeStream(wrapStream(blockId, stream))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}
