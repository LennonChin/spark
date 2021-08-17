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

import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 *
 * Tungsten在堆外内存模式下使用的内存分配器，与offHeap ExecutionMemoryPool配合使用。
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeMemoryAllocator.class);

  // 用于分配指定大小（size）的MemoryBlock。
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {

    logger.debug("Tungsten allocate unsafe memory, size (bytes): " + size);

    /**
     * 在堆外内存分配指定大小的内存。
     * Platform的allocateMemory()方法实际代理了sun.misc.Unsafe的allocateMemory()方法，
     * sun.misc.Unsafe的allocateMemory()方法将返回分配的内存地址。
     */
    long address = Platform.allocateMemory(size);
    // 创建MemoryBlock，可以看到未传递MemoryBlock的obj属性
    MemoryBlock memory = new MemoryBlock(null, address, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    // 返回MemoryBlock
    return memory;
  }

  // 用于释放MemoryBlock。
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    logger.debug("Tungsten allocate unsafe memory, offset (bytes): " + memory.offset);

    // 调用了Platform的freeMemory()方法，后者实际代理了sun.misc.Unsafe的freeMemory方法。
    Platform.freeMemory(memory.offset);
  }
}
