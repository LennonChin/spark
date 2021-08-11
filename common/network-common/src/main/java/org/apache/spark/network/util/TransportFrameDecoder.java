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

import java.util.LinkedList;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with harcoded parameters that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 *
 * 对从管道中读取的ByteBuf按照数据帧进行解析。
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {

  // 处理器名
  public static final String HANDLER_NAME = "frameDecoder";
  // 表示帧大小的数据长度
  private static final int LENGTH_SIZE = 8;
  // 最大帧大小，为Integer.MAX_VALUE
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
  // 标记字段，用于标记帧大小记录是无效的
  private static final int UNKNOWN_FRAME_SIZE = -1;

  // 存储ByteBuf的链表，收到的ByteBuf会存入该链表
  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  // 存放帧大小的ByteBuf
  private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);

  // 当次总共可读字节
  private long totalSize = 0;
  // 下一个帧的大小
  private long nextFrameSize = UNKNOWN_FRAME_SIZE;
  // 拦截器
  private volatile Interceptor interceptor;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    // 将传入的数据转换为Netty的ByteBuf
    ByteBuf in = (ByteBuf) data;
    // 添加到LinkedList类型的buffers链表中进行记录
    buffers.add(in);
    // 增加总共可读取的字节数
    totalSize += in.readableBytes();

    // 遍历buffers链表
    while (!buffers.isEmpty()) {
      // First, feed the interceptor, and if it's still, active, try again.
      if (interceptor != null) { // 有拦截器，让拦截器处理，拦截器只会处理一次
        // 取出链表头的ByteBuf
        ByteBuf first = buffers.getFirst();
        // 计算可读字节数
        int available = first.readableBytes();
        // 先使用interceptor处理数据
        if (feedInterceptor(first)) {
          assert !first.isReadable() : "Interceptor still active but buffer has data.";
        }

        // 计算已读字节数
        int read = available - first.readableBytes();
        // 如果全部读完，就将该ByteBuf从buffers链表中移除
        if (read == available) {
          buffers.removeFirst().release();
        }
        // 维护可读字节计数
        totalSize -= read;
      } else { // 没有拦截器
        // Interceptor is not active, so try to decode one frame.
        // 尝试解码帧数据
        ByteBuf frame = decodeNext();
        // 解码出来的帧数据为null，直接跳出循环
        if (frame == null) {
          break;
        }
        // 能够解码得到数据，传递给下一个Handler
        ctx.fireChannelRead(frame);
      }
    }
  }

  /**
   * 该方法用于从接收的数据中读取帧大小；接收到的数据中，
   * |----- frameSize -----|----------- frame data -----------|
   * |------ 8 Bytes ------|------ frameSize - 8 Bytes -------|
   *
   * 接收的数据中，前8个字节表示帧大小，8个字节之后的数据才是帧数据，
   * 需要注意的是，帧大小表示整个帧的大小，包括前8个字节的长度，
   * 因此帧数据的真实长度应该是帧大小减去8。例如：
   * - 前8个字节表示的Long型值为64，那说明整个帧大小为64，
   * - 因为前8个字节用做表示帧大小了，所以帧数据只有 64 -8 = 56 字节。
   *
   * |--------- 64 --------|------ frame data -------|
   * |------ 8 Bytes ------|-------- 56 Bytes -------|
   */
  private long decodeFrameSize() {
    /**
     * nextFrameSize记录下一个将要读取的帧的大小，如果不为-1，说明该帧还未读完；
     * totalSize < LENGTH_SIZE表示接收到的数据还不能解码表示一个帧大小的长度（8字节），因此还可以继续读取。
     */
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
      return nextFrameSize;
    }

    // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
    // hold the bytes for the frame length in a composite buffer until we have enough data to read
    // the frame size. Normally, it should be rare to need more than one buffer to read the frame
    // size.
    // 走到这里，说明nextFrameSize没有值或者还没有读到可以解码表示一个帧的大小的数据
    ByteBuf first = buffers.getFirst();
    if (first.readableBytes() >= LENGTH_SIZE) { // 读到的数据可以解码得到帧大小
      // 读取帧大小，注意此处需要减去帧头8个字节
      nextFrameSize = first.readLong() - LENGTH_SIZE;
      // 减去已读数据大小
      totalSize -= LENGTH_SIZE;
      // 如果ByteBuf不可读了，则将其从buffers中移除并释放
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      // 返回帧大小
      return nextFrameSize;
    }

    // 读到的数据还不能解码得到帧大小，循环获取buffers中的ByteBuf，直到能读到8个字节
    while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
      ByteBuf next = buffers.getFirst();
      int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
      // 将数据读到frameLenBuf中，最多只会读8个字节
      frameLenBuf.writeBytes(next, toRead);
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }

    // 解析帧大小
    nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
    // 减去已读数据大小
    totalSize -= LENGTH_SIZE;
    // 清空frameLenBuf
    frameLenBuf.clear();

    // 返回帧大小
    return nextFrameSize;
  }

  private ByteBuf decodeNext() throws Exception {
    // 得到帧大小
    long frameSize = decodeFrameSize();
    // 检查帧大小的合法性
    if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
      return null;
    }

    // Reset size for next frame.
    nextFrameSize = UNKNOWN_FRAME_SIZE;

    Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE, "Too large frame: %s", frameSize);
    Preconditions.checkArgument(frameSize > 0, "Frame length should be positive: %s", frameSize);

    // If the first buffer holds the entire frame, return it.
    // 剩余可读帧数
    int remaining = (int) frameSize;
    /**
     * 如果buffers中第一个ByteBuf的可读字节数大于等于可读帧数，
     * 表示这一个ByteBuf包含了一整个帧的数据，可以一次读到一个帧
     */
    if (buffers.getFirst().readableBytes() >= remaining) {
      // 读取一个帧的数据并返回
      return nextBufferForFrame(remaining);
    }

    // Otherwise, create a composite buffer.
    // 此时说明一个ByteBuf的数据不够一个帧，构造一个复合ByteBuf
    CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
    // 当还没读到一个帧的数据时，循环处理
    while (remaining > 0) {
      /**
       * 获取buffers链表头的ByteBuf中remaining长度的数据
       * 读取过程中如果将链表头的ByteBuf读完了，会将其从buffers中移除
       */
      ByteBuf next = nextBufferForFrame(remaining);
      // 减去读到的数据
      remaining -= next.readableBytes();
      // 添加到CompositeByteBuf
      frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
    }
    assert remaining == 0;

    // 终于读完一个帧了，返回复合ByteBuf
    return frame;
  }

  /**
   * Takes the first buffer in the internal list, and either adjust it to fit in the frame
   * (by taking a slice out of it) or remove it from the internal list.
   */
  private ByteBuf nextBufferForFrame(int bytesToRead) {
    ByteBuf buf = buffers.getFirst();
    ByteBuf frame;

    if (buf.readableBytes() > bytesToRead) {
      frame = buf.retain().readSlice(bytesToRead);
      totalSize -= bytesToRead;
    } else {
      frame = buf;
      buffers.removeFirst();
      totalSize -= frame.readableBytes();
    }

    return frame;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    for (ByteBuf b : buffers) {
      b.release();
    }
    if (interceptor != null) {
      interceptor.channelInactive();
    }
    frameLenBuf.release();
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (interceptor != null) {
      interceptor.exceptionCaught(cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  public void setInterceptor(Interceptor interceptor) {
    Preconditions.checkState(this.interceptor == null, "Already have an interceptor.");
    this.interceptor = interceptor;
  }

  /**
   * @return Whether the interceptor is still active after processing the data.
   */
  private boolean feedInterceptor(ByteBuf buf) throws Exception {
    if (interceptor != null && !interceptor.handle(buf)) {
      interceptor = null;
    }
    return interceptor != null;
  }

  public interface Interceptor {

    /**
     * Handles data received from the remote end.
     *
     * @param data Buffer containing data.
     * @return "true" if the interceptor expects more data, "false" to uninstall the interceptor.
     */
    boolean handle(ByteBuf data) throws Exception;

    /** Called if an exception is thrown in the channel pipeline. */
    void exceptionCaught(Throwable cause) throws Exception;

    /** Called if the channel is closed and the interceptor is still installed. */
    void channelInactive() throws Exception;

  }

}
