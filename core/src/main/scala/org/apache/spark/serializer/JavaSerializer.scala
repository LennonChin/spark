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

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

private[spark] class JavaSerializationStream(
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {
  // 使用ObjectOutputStream包装传入的输出流
  private val objOut = new ObjectOutputStream(out)
  // 序列化计数器，用于判断是否需要重置objOut对象
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
    *
    * 将对象序列化并写出到ObjectOutputStream流
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      // 将数据写出到objOut流
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    // 没写一个计数器自增1
    counter += 1
    // 当计数器值大于counterReset时，说明需要重置ObjectOutputStream流，防止内存泄漏
    if (counterReset > 0 && counter >= counterReset) {
      // 进行重置
      objOut.reset()
      // 计数器重置
      counter = 0
    }
    this
  }

  // 刷写
  def flush() { objOut.flush() }
  // 关闭流
  def close() { objOut.close() }
}

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {

  // 构造ObjectInputStream包装传入的输入流
  private val objIn = new ObjectInputStream(in) {
    // 用于加载反序列化后的对象所属的类
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        // 可能需要使用自定义的类加载器
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  // 读取为一个对象，使用ObjectInputStream读取并转换
  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  // 关闭流
  def close() { objIn.close() }
}

private object JavaDeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void]
  )
}

private[spark] class JavaSerializerInstance(
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  // 序列化特定类型的对象，并将数据存放在ByteBuffer中返回
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    // 创建字节缓冲输出流
    val bos = new ByteBufferOutputStream()
    // 使用serializeStream()方法包装输出流
    val out = serializeStream(bos)
    // 写出对象到输出流中
    out.writeObject(t)
    // 关闭流
    out.close()
    // 将输出流转换为ByteBuffer
    bos.toByteBuffer
  }

  // 将ByteBuffer中的数据反序列化为特定类型的对象，会使用当前线程的类加载器
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    // 创建字节缓冲输入流
    val bis = new ByteBufferInputStream(bytes)
    // 使用deserializeStream()方法以当前线程的类加载器包装输入流
    val in = deserializeStream(bis)
    // 从输入流中读取数据并返回
    in.readObject()
  }

  // 使用指定的类加载器将ByteBuffer中的数据反序列化为特定类型的对象
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    // 创建字节缓冲输入流
    val bis = new ByteBufferInputStream(bytes)
    // 使用deserializeStream()方法以特定的类加载器包装输入流
    val in = deserializeStream(bis, loader)
    // 从输入流中读取数据并返回
    in.readObject()
  }

  // 包装输出流为SerializationStream序列化流
  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  // 包装输入流为DeserializationStream反序列化流，会使用当前线程的类加载器
  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  // 使用指定的类加载器包装输入流为DeserializationStream反序列化流
  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses Java's built-in serialization.
 *
 * @note This serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
  *
  * 使用Java内置的序列化机制构建的Spark序列化器
  *
  * 该序列化器不保证在不同版本Spark之间保持线性兼容，
  * 它主要是在Spark作业中用于对数据序列化或反序列化。
 */
@DeveloperApi
class JavaSerializer(conf: SparkConf) extends Serializer with Externalizable {
  // 序列化时会缓存对象防止写多余的数据，但这些对象就不会被GC，默认缓存100个对象。
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)
  private var extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true)

  protected def this() = this(new SparkConf())  // For deserialization only

  // 获得一个JavaSerializerInstance实例
  override def newInstance(): SerializerInstance = {
    // 构造序列化器，类加载器使用本线程的ContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    // 直接返回了JavaSerializerInstance
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  // 控制JavaSerializer自身的序列化
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  // 控制JavaSerializer自身的反序列化
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}
