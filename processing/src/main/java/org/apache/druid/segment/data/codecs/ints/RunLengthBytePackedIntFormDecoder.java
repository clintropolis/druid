/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.data.codecs.ints;

import org.apache.druid.segment.data.ShapeShiftingColumn;
import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.BaseFormDecoder;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * layout:
 * | header: IntCodecs.RLE_BYTEPACK (byte) | numBytes (byte) | encoded values ((2 * numDistinctRuns * numBytes) + (numSingleValues * numBytes)) |
 */
public final class RunLengthBytePackedIntFormDecoder extends BaseFormDecoder<ShapeShiftingColumnarInts>
{
  static final int value_mask_int8 = 0x7F;
  static final int value_mask_int16 = 0x7FFF;
  static final int value_mask_int24 = 0x7FFFFF;
  static final int value_mask_int32 = 0x7FFFFFFF;

  static final int run_mask_int8 = 0x80;
  static final int run_mask_int16 = 0x8000;
  static final int run_mask_int24 = 0x800000;
  static final int run_mask_int32 = 0x80000000;

  private static final Unsafe unsafe = ShapeShiftingColumn.getTheUnsafe();

  public RunLengthBytePackedIntFormDecoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  @Override
  public void transform(ShapeShiftingColumnarInts columnarInts)
  {
    final ByteBuffer buffer = columnarInts.getCurrentValueBuffer();
    // metadata is always in base buffer at current chunk start offset
    final ByteBuffer metaBuffer = columnarInts.getBuffer();
    final int metaOffset = columnarInts.getCurrentChunkStartOffset();
    final byte numBytes = metaBuffer.get(metaOffset);
    final int numValues = columnarInts.getCurrentChunkNumValues();
    final int startOffset = columnarInts.getCurrentValuesStartOffset();
    final ByteBuffer decodedDataBuffer = columnarInts.getDecodedDataBuffer();

    if (buffer.isDirect() && decodedDataBuffer.isDirect() && byteOrder.equals(ByteOrder.nativeOrder())) {
      final long addr = columnarInts.getCurrentValuesAddress();
      decodeIntsUnsafe(addr, numValues, decodedDataBuffer, numBytes, byteOrder);
    } else {
      decodeIntsBuffer(buffer, startOffset, numValues, decodedDataBuffer, numBytes, byteOrder);
    }

    columnarInts.setCurrentBytesPerValue(Integer.BYTES);
    columnarInts.setCurrentValueBuffer(decodedDataBuffer);
    if (decodedDataBuffer.isDirect() && byteOrder.equals(ByteOrder.nativeOrder())) {
      columnarInts.setCurrentValuesAddress(((DirectBuffer) decodedDataBuffer).address());
    }
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.RLE_BYTEPACK;
  }

  @Override
  public int getMetadataSize()
  {
    return 1;
  }

  private static void decodeIntsUnsafe(
      long addr,
      final int numValues,
      final ByteBuffer decodedValuesBuffer,
      final int bytePerValue,
      final ByteOrder byteOrder
  )
  {
    int runCount;
    int runValue;
    decodedValuesBuffer.clear();
    long destAddr = ((DirectBuffer) decodedValuesBuffer).address();

    final boolean isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    final DecodeAddressFunction decode;
    final int runMask;
    final int valueMask;
    switch (bytePerValue) {
      case 1:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt8Unsafe;
        runMask = run_mask_int8;
        valueMask = value_mask_int8;
        break;
      case 2:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt16Unsafe;
        runMask = run_mask_int16;
        valueMask = value_mask_int16;
        break;
      case 3:
        decode = isBigEndian
                 ? RunLengthBytePackedIntFormDecoder::decodeBigEndianInt24Unsafe
                 : RunLengthBytePackedIntFormDecoder::decodeInt24Unsafe;
        runMask = run_mask_int24;
        valueMask = value_mask_int24;
        break;
      default:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt32Unsafe;
        runMask = run_mask_int32;
        valueMask = value_mask_int32;
        break;
    }

    for (int i = 0; i < numValues; i++) {
      final int nextVal = decode.get(addr);
      addr += bytePerValue;
      if ((nextVal & runMask) == 0) {
        unsafe.putInt(destAddr, nextVal);
        destAddr += Integer.BYTES;
      } else {
        runCount = nextVal & valueMask;
        runValue = decode.get(addr);
        addr += bytePerValue;
        do {
          unsafe.putInt(destAddr, runValue);
          destAddr += Integer.BYTES;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
    decodedValuesBuffer.limit(numValues * Integer.BYTES);
  }


  private static int decodeInt8Unsafe(long addr)
  {
    return unsafe.getByte(addr) & 0xFF;
  }

  private static int decodeInt16Unsafe(long addr)
  {
    return unsafe.getShort(addr) & 0xFFFF;
  }

  private static int decodeBigEndianInt24Unsafe(long addr)
  {
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    return unsafe.getInt(addr) >>> BytePackedIntFormDecoder.BIG_ENDIAN_INT_24_SHIFT;
  }

  private static int decodeInt24Unsafe(long addr)
  {
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    return unsafe.getInt(addr) & BytePackedIntFormDecoder.LITTLE_ENDIAN_INT_24_MASK;
  }

  private static int decodeInt32Unsafe(long addr)
  {
    return unsafe.getInt(addr);
  }

  public static void decodeIntsBuffer(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final ByteBuffer decodedValuesBuffer,
      final int bytePerValue,
      final ByteOrder byteOrder
  )
  {

    int bufferPosition = startOffset;
    int runCount;
    int runValue;

    decodedValuesBuffer.clear();
    final boolean isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    final DecodeBufferFunction decode;
    final int runMask;
    final int valueMask;
    switch (bytePerValue) {
      case 1:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt8;
        runMask = run_mask_int8;
        valueMask = value_mask_int8;
        break;
      case 2:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt16;
        runMask = run_mask_int16;
        valueMask = value_mask_int16;
        break;
      case 3:
        decode = isBigEndian
                 ? RunLengthBytePackedIntFormDecoder::decodeBigEndianInt24
                 : RunLengthBytePackedIntFormDecoder::decodeInt24;
        runMask = run_mask_int24;
        valueMask = value_mask_int24;
        break;
      default:
        decode = RunLengthBytePackedIntFormDecoder::decodeInt32;
        runMask = run_mask_int32;
        valueMask = value_mask_int32;
        break;
    }

    for (int i = 0; i < numValues; i++) {
      final int nextVal = decode.get(buffer, bufferPosition);
      bufferPosition += bytePerValue;
      if ((nextVal & runMask) == 0) {
        decodedValuesBuffer.putInt(nextVal);
      } else {
        runCount = nextVal & valueMask;
        runValue = decode.get(buffer, bufferPosition);
        bufferPosition += bytePerValue;
        do {
          decodedValuesBuffer.putInt(runValue);
        } while (--runCount > 0 && ++i < numValues);
      }
    }
    decodedValuesBuffer.flip();
  }

  private static int decodeInt8(
      final ByteBuffer buffer,
      final int offset
  )
  {
    return buffer.get(offset) & 0xFF;
  }

  private static int decodeInt16(
      final ByteBuffer buffer,
      final int offset
  )
  {
    return buffer.getShort(offset) & 0xFFFF;
  }

  private static int decodeBigEndianInt24(
      final ByteBuffer buffer,
      final int offset
  )
  {
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    return buffer.getInt(offset) >>> BytePackedIntFormDecoder.BIG_ENDIAN_INT_24_SHIFT;
  }

  private static int decodeInt24(
      final ByteBuffer buffer,
      final int offset
  )
  {
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    return buffer.getInt(offset) & BytePackedIntFormDecoder.LITTLE_ENDIAN_INT_24_MASK;
  }

  private static int decodeInt32(
      final ByteBuffer buffer,
      final int offset
  )
  {
    return buffer.getInt(offset);
  }

  @FunctionalInterface
  public interface DecodeBufferFunction
  {
    int get(ByteBuffer buffer, int offset);
  }

  @FunctionalInterface
  public interface DecodeAddressFunction
  {
    int get(long address);
  }
}
