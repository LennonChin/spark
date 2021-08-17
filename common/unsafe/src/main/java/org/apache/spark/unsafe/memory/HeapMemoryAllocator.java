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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 *
 * Tungsten在堆内存模式下使用的内存分配器，与onHeapExecutionMemoryPool配合使用。
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  private static final Logger logger = LoggerFactory.getLogger(HeapMemoryAllocator.class);

  // 关于MemoryBlock的弱引用的缓冲池，用于Page页（即MemoryBlock）的分配。
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();

  // 池化阈值，只有在池化的MemoryBlock大于该值时，才需要被池化
  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   *
   * 用于判断对于指定大小的MemoryBlock，
   * 是否需要采用池化机制（即从缓冲池bufferPoolsBySize中存取MemoryBlock）。
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    // 当要分配的内存大小大于等于1MB时，需要从bufferPoolsBySize中获取MemoryBlock。
    return size >= POOLING_THRESHOLD_BYTES; // 1MB
  }

  // 用于分配指定大小（size）的MemoryBlock
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {

    logger.debug("Tungsten allocate heap memory, size (bytes): " + size);

    if (shouldPool(size)) { // 指定大小（size）的MemoryBlock需要采用池化机制
      synchronized (this) {
        // 从bufferPoolsBySize的弱引用中获取指定大小的MemoryBlock链表
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        // 池链不为空
        if (pool != null) {
          // 当池链中还存在MemoryBlock时
          while (!pool.isEmpty()) {
            // 取出池链头的MemoryBlock
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) { // 取出的MemoryBlock不为空
              // MemoryBlock的大小需要与分配的大小相同
              assert (memory.size() == size);
              // 从MemoryBlock的缓存中获取指定大小的MemoryBlock并返回
              return memory;
            }
          }
          // 池链内没有指定大小的MemoryBlock，移除指定大小的MemoryBlock缓存
          bufferPoolsBySize.remove(size);
        }
      }
    }

    /**
     * 走到此处，说明满足以下任意一点：
     * 1. 指定大小的MemoryBlock不需要采用池化机制。
     * 2. bufferPoolsBySize中没有指定大小的MemoryBlock。
     *
     * MemoryBlock中以Long类型数组装载数据，所以需要对申请的大小进行转换，
     * 由于申请的是字节数，因此先为其多分配7个字节，避免最终分配的字节数不够，
     * 除以8是按照Long类型由8个字节组成来计算的。
     *
     * 例如：申请字节数为50，理想情况应该分配56字节，即7个Long型数据。
     * 如果直接除以8，会得到6，即6个Long型数据，导致只会分配48个字节，
     * 但先加7后再除以8，即 (50 + 7) / 8 = 7个Long型数据，满足分配要求。
     */
    long[] array = new long[(int) ((size + 7) / 8)];
    // 创建MemoryBlock并返回
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    // Debug相关
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) { // spark.memory.debugFill
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE); // 0xa5, jemalloc's debug fill values.
    }
    // 返回创建的MemoryBlock
    return memory;
  }

  // 用于释放MemoryBlock
  @Override
  public void free(MemoryBlock memory) {

    logger.debug("Tungsten free heap memory, size (bytes): " + memory.size());

    // 获取待释放MemoryBlock的大小
    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) { // spark.memory.debugFill
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE); // 0x5a, jemalloc's debug fill values.
    }
    if (shouldPool(size)) { // MemoryBlock的大小需要采用池化机制
      // 将MemoryBlock的弱引用放入bufferPoolsBySize中
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(size, pool);
        }
        // 将MemoryBlock的弱引用放入bufferPoolsBySize中
        pool.add(new WeakReference<>(memory));
      }
    } else {
      // Do nothing
    }
  }
}
