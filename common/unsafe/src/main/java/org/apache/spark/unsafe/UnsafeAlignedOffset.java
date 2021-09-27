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

package org.apache.spark.unsafe;

/**
 * Class to make changes to record length offsets uniform through out
 * various areas of Apache Spark core and unsafe.  The SPARC platform
 * requires this because using a 4 byte Int for record lengths causes
 * the entire record of 8 byte Items to become misaligned by 4 bytes.
 * Using a 8 byte long for record length keeps things 8 byte aligned.
 *
 * 该类用于在 Apache Spark 核心和不安全的各个区域中对记录长度偏移进行统一更改。
 * SPARC 平台需要这样做，因为对记录长度使用 4 字节 Int 会导致 8 字节项目的整个记录错位 4 字节。
 * 使用 8 字节长的记录长度可以保持 8 字节对齐。
 */
public class UnsafeAlignedOffset {

  private static final int UAO_SIZE = Platform.unaligned() ? 4 : 8;

  public static int getUaoSize() {
    return UAO_SIZE;
  }

  public static int getSize(Object object, long offset) {
    switch (UAO_SIZE) {
      case 4:
        return Platform.getInt(object, offset);
      case 8:
        return (int)Platform.getLong(object, offset);
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }

  public static void putSize(Object object, long offset, int value) {
    switch (UAO_SIZE) {
      case 4:
        Platform.putInt(object, offset, value);
        break;
      case 8:
        Platform.putLong(object, offset, value);
        break;
      default:
        throw new AssertionError("Illegal UAO_SIZE");
    }
  }
}
