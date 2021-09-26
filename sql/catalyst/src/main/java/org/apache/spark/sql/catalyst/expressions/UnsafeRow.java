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

package org.apache.spark.sql.catalyst.expressions;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

/**
 * An Unsafe implementation of Row which is backed by raw memory instead of Java objects.
 *
 * Each tuple has three parts: [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word. For
 * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
 * base address of the row) that points to the beginning of the variable-length field, and length
 * (they are combined into a long).
 *
 * Instances of `UnsafeRow` act as pointers to row data stored in this format.
 *
 * 使用原始内存代替Java对象的方式，对数据行Row的一种Unsafe实现。
 *
 * 每个元组由三个部分组成：[null的位设置] [值] [可变长度部分]
 *
 * 位设置用于null的跟踪，并且会根据8字节位宽进行对其。每个字段存储1个比特位。
 *
 * 在“值”的区域内，为每个字段存储1个8字节位宽。
 * 对于固定宽度的基本类型字段，像long、double、int，直接存储值在8字节位宽里。
 * 对于变长宽度的非基本类型字段，存储指向变长字段开始的相对偏移量（即在行值中的基本地址）以及长度（偏移量和长度组合为long类型值）。
 *
 * UnsafeRow的实例作为指向以该格式存储的行原始数据的指针。
 *
 * 假设N为字段数量，下图中以字节位单位：
 *
 * |------ max(1, N) ------+---------- 8 ----------+---------- 8 ----------+ ... +----------------+----------------+-----+
 * |   null比特位宽字节数    |   第1个字段的偏移量长度  |   第2个字段的偏移量长度  | ... | 第1个字段的数据  | 第2个字段的数据  | ... |
 * |-----------------------+-----------------------+-----------------------+ ... +----------------+----------------+-----+
 *                         |   Offset  |   Length  |      baseOffset + Offset -> |                |
 *                         +-----------------------+                             +---- Length ----+
 *
 *
 * 不采用Java对象存储的方式，避免了JVM中垃圾回收（GC）的代价。此外，UnsafeRow对行数据进行了特定的编码，使得存储更加高效。
 * 作为Tungsten计划的重要内容。
 */
public final class UnsafeRow extends InternalRow implements Externalizable, KryoSerializable {

  //////////////////////////////////////////////////////////////////////////////
  // Static methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * 计算字段需要的比特位宽，以字节表示。每个字段占1比特位。
   * 字段数量在 1 ~ 64 个内，都用8位字节表示，8位字节一共64比特位；即比特位宽最少也为8个字节。
   * 超过64个，比特位宽字节数则以8的倍数递增。
   */
  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields + 63)/ 64) * 8;
  }

  /**
   * 计算固定位宽部分的字节数。
   * 固定位宽部分包括Null比特位和字段偏移量的描述这两个部分。
   */
  public static int calculateFixedPortionByteSize(int numFields) {
    return 8 * numFields + calculateBitSetWidthInBytes(numFields);
  }

  /**
   * Field types that can be updated in place in UnsafeRows (e.g. we support set() for these types)
   *
   * UnsafeRow可以原地更新的字段类型集合。包含：
   * NullType、BooleanType、ByteType、ShortType、IntegerType、LongType、FloatType、DoubleType、DateType、TimestampType
   */
  public static final Set<DataType> mutableFieldTypes;

  // DecimalType is also mutable
  static {
    mutableFieldTypes = Collections.unmodifiableSet(
      new HashSet<>(
        Arrays.asList(new DataType[] {
          NullType,
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          DateType,
          TimestampType
        })));
  }

  // 判断数据类型是否是固定长度
  public static boolean isFixedLength(DataType dt) {
    if (dt instanceof DecimalType) {
      // DecimalType单独判断，精度需要小于等于18
      return ((DecimalType) dt).precision() <= Decimal.MAX_LONG_DIGITS();
    } else {
      // 其他可变类型都是固定长度
      return mutableFieldTypes.contains(dt);
    }
  }

  /**
   * 判断子类类型是否是可变的（可原地更新），包含：
   * NullType、BooleanType、ByteType、ShortType、IntegerType、LongType、FloatType、DoubleType、DateType、TimestampType、DecimalType
   */
  public static boolean isMutable(DataType dt) {
    return mutableFieldTypes.contains(dt) || dt instanceof DecimalType;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Private fields and methods
  //////////////////////////////////////////////////////////////////////////////

  private Object baseObject;
  private long baseOffset;

  /**
   * The number of fields in this row, used for calculating the bitset width (and in assertions)
   * 数据行中的字段数量，用于计算比特位宽。
   **/
  private int numFields;

  /**
   * The size of this row's backing data, in bytes)
   * 数据行的大小，字节单位表示。
   **/
  private int sizeInBytes;

  /**
   * The width of the null tracking bit set, in bytes
   * 用于跟踪null值的比特位宽度，字节单位表示。
   **/
  private int bitSetWidthInBytes;

  /**
   * 根据索引获取字段的偏移量，baseOffset是指整个UnsafeRow在baseObject中的偏移量
   * bitSetWidthInBytes即是前面null值比特位宽的大小。
   */
  private long getFieldOffset(int ordinal) {
    return baseOffset + bitSetWidthInBytes + ordinal * 8L;
  }

  // 检查索引是否合法，需要大于等于0，且小于字段数量。
  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should < " + numFields;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Public methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Construct a new UnsafeRow. The resulting row won't be usable until `pointTo()` has been called,
   * since the value returned by this constructor is equivalent to a null pointer.
   *
   * 构造新的UnsafeRow。
   * 返回的结果必须在调用 `pointTo()` 方法后才可以被使用，
   * 当前构造器返回的结果相当于一个空指针。
   *
   * @param numFields the number of fields in this row
   */
  public UnsafeRow(int numFields) {
    this.numFields = numFields;
    // 根据字段数量计算null比特位宽
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
  }

  // for serializer
  public UnsafeRow() {}

  // baseObject、baseOffset、sizeInBytes
  public Object getBaseObject() { return baseObject; }
  public long getBaseOffset() { return baseOffset; }
  public int getSizeInBytes() { return sizeInBytes; }

  @Override
  public int numFields() { return numFields; }

  /**
   * Update this UnsafeRow to point to different backing data.
   *
   * 更新当前的UnsafeRow，以指向不同的底层数据。
   *
   * @param baseObject the base object
   *                   存储数据的基础对象。
   * @param baseOffset the offset within the base object
   *                   数据在基础对象中的偏移量。
   * @param sizeInBytes the size of this row's backing data, in bytes
   *                    该数据行使用到的底层数据的大小。
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Update this UnsafeRow to point to the underlying byte array.
   *
   * 更新当前的UnsafeRow，以指向底层的字节数组
   *
   * @param buf byte array to point to
   *            指向的字节数组对象
   * @param sizeInBytes the number of bytes valid in the byte array
   *                    该数据行使用到的底层数据的大小。
   */
  public void pointTo(byte[] buf, int sizeInBytes) {
    pointTo(buf, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
  }

  // 设置总大小
  public void setTotalSize(int sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  // 将某个索引位的字段设置为非Null，底层会更新null的比特位宽中对应的比特位。
  public void setNotNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.unset(baseObject, baseOffset, i);
  }

  // 将某个索引位的字段设置为Null，底层会更新null的比特位宽中对应的比特位。
  @Override
  public void setNullAt(int i) {
    assertIndexIsValid(i);
    BitSetMethods.set(baseObject, baseOffset, i);
    // To preserve row equality, zero out the value when setting the column to null.
    // Since this row does does not currently support updates to variable-length values, we don't
    // have to worry about zeroing out that data.
    Platform.putLong(baseObject, getFieldOffset(i), 0);
  }

  // 不支持直接更新字段值。
  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  /**
   * 下面的方法是根据索引位设置Int、Long、Double、Boolean、Short、Byte、Float等类型的字段值，主要分三步：
   * 1. assertIndexIsValid方法检查索引是否合法。
   * 2. setNotNullAt方法设置对应的字段为非null。
   * 3. 使用Platform的Unsafe进行底层数据更新。
   * @param ordinal
   * @param value
   */
  @Override
  public void setInt(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    Platform.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    setNotNullAt(ordinal);
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    Platform.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  /**
   * Updates the decimal column.
   *
   * Note: In order to support update a decimal with precision > 18, CAN NOT call
   * setNullAt() for this column.
   *
   * 更新Decimal类型的列。
   * 注意：为了支持精度大于18位的Decimal的更新，不能对该列调用setNullAt()方法。
   *
   * 精度小于18的Decimal，可以直接转为Long值存储。
   * 精度大于18的Decimal，需要进行两段寻址。
   */
  @Override
  public void setDecimal(int ordinal, Decimal value, int precision) {
    // 检查索引是否合法
    assertIndexIsValid(ordinal);

    // 精度小于等于18，Long型数据最大只能表示18位的精度
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // compact format
      if (value == null) {
        setNullAt(ordinal);
      } else {
        // 非空，转换为Long类型。
        setLong(ordinal, value.toUnscaledLong());
      }
    } else {
      // fixed length
      // 取出索引位上的Long型值的高32位，作为游标
      long cursor = getLong(ordinal) >>> 32;
      assert cursor > 0 : "invalid cursor " + cursor;
      // zero-out the bytes
      // 将游标后的16个字节都置为0。
      Platform.putLong(baseObject, baseOffset + cursor, 0L);
      Platform.putLong(baseObject, baseOffset + cursor + 8, 0L);

      if (value == null) {
        setNullAt(ordinal);
        // keep the offset for future update
        // 空值，只在高32存游标值，低32位都存0值
        Platform.putLong(baseObject, getFieldOffset(ordinal), cursor << 32);
      } else {
        // 非空，将Decimal值转为BigInteger，然后再转为Byte数组，要求数组的长度小于等于16（即最多128位）
        final BigInteger integer = value.toJavaBigDecimal().unscaledValue();
        byte[] bytes = integer.toByteArray();
        assert(bytes.length <= 16);

        // Write the bytes to the variable length portion.
        // 将BigInteger字节数组存放到游标指向的偏移量开始的位置
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, baseObject, baseOffset + cursor, bytes.length);
        // 更新游标值，高32位为偏移量，低32位为数据长度
        setLong(ordinal, (cursor << 32) | ((long) bytes.length));
      }
    }
  }

  // 根据索引和数据类型获取字段值的通用方法。
  @Override
  public Object get(int ordinal, DataType dataType) {
    if (isNullAt(ordinal) || dataType instanceof NullType) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof CalendarIntervalType) {
      return getInterval(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).size());
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof UserDefinedType) {
      return get(ordinal, ((UserDefinedType)dataType).sqlType());
    } else {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString());
    }
  }

  // 判断索引位上的值是否是null
  @Override
  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSetMethods.isSet(baseObject, baseOffset, ordinal);
  }

  /**
   * 下面的方法是根据索引位获取Int、Long、Double、Boolean、Short、Byte、Float等类型的字段值，主要分三步：
   * 1. assertIndexIsValid方法检查索引是否合法。
   * 2. getFieldOffset方法根据索引获取字段的偏移量信息。
   * 3. 使用Platform的Unsafe进行根据偏移量获取底层数据。
   */
  @Override
  public boolean getBoolean(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getBoolean(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getByte(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getShort(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getInt(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getLong(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getFloat(baseObject, getFieldOffset(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    assertIndexIsValid(ordinal);
    return Platform.getDouble(baseObject, getFieldOffset(ordinal));
  }

  /**
   * 根据索引、精度、规模获取Decimal类型的值。
   */
  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) {
      return null;
    }
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // 非空，精度小于等于18位，直接获取Long值，然后转换为Decimal
      return Decimal.createUnsafe(getLong(ordinal), precision, scale);
    } else {
      // 非空，精度大于18位，先获取二进制数据
      byte[] bytes = getBinary(ordinal);
      // 二进制数据转为BigInteger
      BigInteger bigInteger = new BigInteger(bytes);
      // BigInteger转为Decimal
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  // 获取索引位的字符串
  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (isNullAt(ordinal)) return null;
    // 获取字符串的偏移量和大小信息
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;
    // 从baseObject中获取字符串
    return UTF8String.fromAddress(baseObject, baseOffset + offset, size);
  }

  // 获取字节数组
  @Override
  public byte[] getBinary(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      // 获取字节数据的偏移量和大小信息
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      final byte[] bytes = new byte[size];
      // 从baseObject中获取字符串
      Platform.copyMemory(
        baseObject,
        baseOffset + offset,
        bytes,
        Platform.BYTE_ARRAY_OFFSET,
        size
      );
      return bytes;
    }
  }

  // 获取Interval
  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      // 获取Interval数据的偏移量
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      // Long类型月份值，8位
      final int months = (int) Platform.getLong(baseObject, baseOffset + offset);
      // Long类型微秒值，8位
      final long microseconds = Platform.getLong(baseObject, baseOffset + offset + 8);
      return new CalendarInterval(months, microseconds);
    }
  }

  // 获取嵌套UnsafeRow表示的Struct类型值
  @Override
  public UnsafeRow getStruct(int ordinal, int numFields) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      // 获取Struct数据的偏移量
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      // 创建新的UnsafeRow，指向底层数据相应的偏移量位置
      final UnsafeRow row = new UnsafeRow(numFields);
      row.pointTo(baseObject, baseOffset + offset, size);
      return row;
    }
  }

  // 获取嵌套UnsafeArrayData表示的Array类型值
  @Override
  public UnsafeArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      // 获取Array数据的偏移量
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      // 创建新的UnsafeArrayData，指向底层数据相应的偏移量位置
      final UnsafeArrayData array = new UnsafeArrayData();
      array.pointTo(baseObject, baseOffset + offset, size);
      return array;
    }
  }

  // 获取嵌套UnsafeMapData表示的Map类型值
  @Override
  public UnsafeMapData getMap(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      // 获取Map数据的偏移量
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) offsetAndSize;
      // 创建新的UnsafeMapData，指向底层数据相应的偏移量位置
      final UnsafeMapData map = new UnsafeMapData();
      map.pointTo(baseObject, baseOffset + offset, size);
      return map;
    }
  }

  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   *
   * 拷贝当前的数据行，返回一个“自管理”的UnsafeRow对象，它的数据存放在内部的一个字节数组中，并没有存放在数据页中。
   */
  @Override
  public UnsafeRow copy() {
    UnsafeRow rowCopy = new UnsafeRow(numFields);
    // 创建自管理的字节数组。
    final byte[] rowDataCopy = new byte[sizeInBytes];
    // 拷贝数据到自管理的字节数组中，并将UnsafeRow指向该字节数组
    Platform.copyMemory(
      baseObject,
      baseOffset,
      rowDataCopy,
      Platform.BYTE_ARRAY_OFFSET,
      sizeInBytes
    );
    rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
    return rowCopy;
  }

  /**
   * Creates an empty UnsafeRow from a byte array with specified numBytes and numFields.
   * The returned row is invalid until we call copyFrom on it.
   *
   * 根据指定的字节大小和字段数量，通过Byte数组创建空的UnsafeRow。
   * 返回的UnsafeRow在调用copyFrom之前是不合法的。
   */
  public static UnsafeRow createFromByteArray(int numBytes, int numFields) {
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(new byte[numBytes], numBytes);
    return row;
  }

  /**
   * Copies the input UnsafeRow to this UnsafeRow, and resize the underlying byte[] when the
   * input row is larger than this row.
   *
   * 拷贝传入的row到当前的UnsafeRow，当传入的row比当前的row底层数组大时需要进行resize操作。
   */
  public void copyFrom(UnsafeRow row) {
    // copyFrom is only available for UnsafeRow created from byte array.
    assert (baseObject instanceof byte[]) && baseOffset == Platform.BYTE_ARRAY_OFFSET;
    if (row.sizeInBytes > this.sizeInBytes) {
      // resize the underlying byte[] if it's not large enough.
      // 当传入的row比当前的row底层数组大时需要进行resize操作
      this.baseObject = new byte[row.sizeInBytes];
    }
    // 拷贝数据到当前row中
    Platform.copyMemory(
      row.baseObject, row.baseOffset, this.baseObject, this.baseOffset, row.sizeInBytes);
    // update the sizeInBytes.
    this.sizeInBytes = row.sizeInBytes;
  }

  /**
   * Write this UnsafeRow's underlying bytes to the given OutputStream.
   *
   * 写出当前UnsafeRow底层的字节数据到给定的输出流
   *
   * @param out the stream to write to.
   *            写出的输出流
   * @param writeBuffer a byte array for buffering chunks of off-heap data while writing to the
   *                    output stream. If this row is backed by an on-heap byte array, then this
   *                    buffer will not be used and may be null.
   *                    用于在写出到输出流时缓冲off-heap数据。
   *                    如果当前UnsafeRow是基于on-heap的字节数组，该缓冲区不会被用到。
   */
  public void writeToStream(OutputStream out, byte[] writeBuffer) throws IOException {
    if (baseObject instanceof byte[]) {
      // on-heap字节数组
      int offsetInByteArray = (int) (Platform.BYTE_ARRAY_OFFSET - baseOffset);
      out.write((byte[]) baseObject, offsetInByteArray, sizeInBytes);
    } else {
      // off-heap数据
      int dataRemaining = sizeInBytes;
      long rowReadPosition = baseOffset;
      // 不断循环拷贝baseObject中的数据并写出到输出流
      while (dataRemaining > 0) {
        int toTransfer = Math.min(writeBuffer.length, dataRemaining);
        Platform.copyMemory(
          baseObject, rowReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        out.write(writeBuffer, 0, toTransfer);
        rowReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
    }
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42);
  }

  // 字节长度相等，字节数据也相同，才视为是两个相同的UnsafeRow。
  @Override
  public boolean equals(Object other) {
    if (other instanceof UnsafeRow) {
      UnsafeRow o = (UnsafeRow) other;
      return (sizeInBytes == o.sizeInBytes) &&
        ByteArrayMethods.arrayEquals(baseObject, baseOffset, o.baseObject, o.baseOffset,
          sizeInBytes);
    }
    return false;
  }

  /**
   * Returns the underlying bytes for this UnsafeRow.
   *
   * 返回UnsafeRow底层的字节数据。
   */
  public byte[] getBytes() {
    if (baseObject instanceof byte[] && baseOffset == Platform.BYTE_ARRAY_OFFSET
      && (((byte[]) baseObject).length == sizeInBytes)) {
      return (byte[]) baseObject;
    } else {
      byte[] bytes = new byte[sizeInBytes];
      Platform.copyMemory(baseObject, baseOffset, bytes, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
      return bytes;
    }
  }

  // This is for debugging
  @Override
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      if (i != 0) build.append(',');
      build.append(java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)));
    }
    build.append(']');
    return build.toString();
  }

  // 是否存在null值字段
  @Override
  public boolean anyNull() {
    return BitSetMethods.anySet(baseObject, baseOffset, bitSetWidthInBytes / 8);
  }

  /**
   * Writes the content of this row into a memory address, identified by an object and an offset.
   * The target memory address must already been allocated, and have enough space to hold all the
   * bytes in this string.
   *
   * 将当前行的内容写到内存地址中，用一个object对象和偏移量进行表示。
   * 目标内存地址必须已经被分配，并且有足够的空闲空间来存储写出的数据。
   */
  public void writeToMemory(Object target, long targetOffset) {
    Platform.copyMemory(baseObject, baseOffset, target, targetOffset, sizeInBytes);
  }

  // 将当前行的内容写出到ByteBuffer中
  public void writeTo(ByteBuffer buffer) {
    assert (buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  /**
   * Write the bytes of var-length field into ByteBuffer
   *
   * Note: only work with HeapByteBuffer
   *
   * 将当前行中某个变长字段写出到ByteBuffer中。
   * 注意：只对HeapByteBuffer可工作。
   */
  public void writeFieldTo(int ordinal, ByteBuffer buffer) {
    final long offsetAndSize = getLong(ordinal);
    final int offset = (int) (offsetAndSize >> 32);
    final int size = (int) offsetAndSize;

    buffer.putInt(size);
    int pos = buffer.position();
    buffer.position(pos + size);
    Platform.copyMemory(
      baseObject,
      baseOffset + offset,
      buffer.array(),
      Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset() + pos,
      size);
  }

  // 将数据行的底层字节数组写出到输出流
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.writeInt(this.numFields);
    out.write(bytes);
  }

  // 从输入流读取数据并填充到当前UnsafeRow中
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt(); // 数据大小
    this.numFields = in.readInt(); // 字段数量
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields); // null比特位
    this.baseObject = new byte[sizeInBytes]; // 底层字节数组
    in.readFully((byte[]) baseObject); // 读取数据到底层字节数组
  }

  // 将底层字节数组写出到输出流；Kryo压缩未实现
  @Override
  public void write(Kryo kryo, Output out) {
    byte[] bytes = getBytes();
    out.writeInt(bytes.length);
    out.writeInt(this.numFields);
    out.write(bytes);
  }

  // 从输入流读取数据并填充到当前UnsafeRow中；Kryo压缩未实现
  @Override
  public void read(Kryo kryo, Input in) {
    this.baseOffset = BYTE_ARRAY_OFFSET;
    this.sizeInBytes = in.readInt(); // 数据大小
    this.numFields = in.readInt(); // 字段数量
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields); // null比特位
    this.baseObject = new byte[sizeInBytes]; // 底层字节数组
    in.read((byte[]) baseObject); // 读取数据到底层字节数组
  }
}
