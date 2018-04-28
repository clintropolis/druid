/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data.codecs.ints;

import io.druid.segment.data.ShapeShiftingColumnarInts;
import io.druid.segment.data.codecs.RandomAccessShapeShiftingFormDecoder;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BytePackedIntFormDecoder extends RandomAccessShapeShiftingFormDecoder<ShapeShiftingColumnarInts>
{
  static final Unsafe unsafe = ShapeShiftingColumnarInts.getTheUnsafe();
  public static final int bigEndianShift3 = Integer.SIZE - 24;
  public static final int littleEndianMask3 = (int) ((1L << 24) - 1);
  final DecoderFunction oddFunction;
  final UnsafeDecoderFunction oddFunctionUnsafe;
  final boolean isBigEndian;

  public BytePackedIntFormDecoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
    this.isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    this.oddFunction = !isBigEndian
                       ? BytePackedIntFormDecoder::decodeLittleEndianOddSizedInts
                       : BytePackedIntFormDecoder::decodeBigEndianOddSizedInts;
    this.oddFunctionUnsafe = !isBigEndian
                             ? BytePackedIntFormDecoder::decodeLittleEndianOddSizedIntsUnsafe
                             : BytePackedIntFormDecoder::decodeBigEndianOddSizedIntsUnsafe;
  }

  @Override
  public final void transform(
      ShapeShiftingColumnarInts columnarInts,
      int startOffset,
      int endOffset,
      int numValues
  )
  {
    final ByteBuffer buffer = columnarInts.getCurrentReadBuffer();
    final int[] decodedChunk = columnarInts.getDecodedValues();
    final byte numBytes = buffer.get(startOffset);

    if (!byteOrder.equals(ByteOrder.nativeOrder())) {
      switch (numBytes) {
        case 1:
          decodeByteSizedInts(buffer, startOffset + 1, numValues, decodedChunk, numBytes);
          break;
        case 2:
          decodeShortSizedInts(buffer, startOffset + 1, numValues, decodedChunk, numBytes);
          break;
        case 3:
          oddFunction.decode(buffer, startOffset + 1, numValues, decodedChunk, numBytes);
          break;
        case 4:
          decodeIntSizedInts(buffer, startOffset + 1, numValues, decodedChunk, numBytes);
          break;
      }
    } else {

      long addr = ((DirectBuffer) buffer).address() + startOffset + 1;
      switch (numBytes) {
        case 1:
          decodeByteSizedIntsUnsafe(addr, numValues, decodedChunk, numBytes);
          break;
        case 2:
          decodeShortSizedIntsUnsafe(addr, numValues, decodedChunk, numBytes);
          break;
        case 3:
          oddFunctionUnsafe.decode(addr, numValues, decodedChunk, numBytes);
          break;
        case 4:
          decodeIntSizedIntsUnsafe(addr, numValues, decodedChunk, numBytes);
          break;
      }
    }
  }

  @Override
  public void transformBuffer(
      ShapeShiftingColumnarInts columnarInts,
      int startOffset,
      int endOffset,
      int numValues
  )
  {
    final ByteBuffer buffer = columnarInts.getCurrentReadBuffer();
    final byte numBytes = buffer.get(startOffset);
    columnarInts.setCurrentBytesPerValue(numBytes);
    columnarInts.setCurrentBufferOffset(startOffset + 1);
  }

  @Override
  public void transformUnsafe(
      ShapeShiftingColumnarInts columnarInts,
      int startOffset,
      int endOffset,
      int numValues
  )
  {
    final ByteBuffer buffer = columnarInts.getCurrentReadBuffer();
    final byte numBytes = buffer.get(startOffset);
    columnarInts.setCurrentBytesPerValue(numBytes);
    columnarInts.setCurrentBaseAddress(((DirectBuffer) buffer).address() + startOffset + 1);
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.BYTEPACK;
  }

  static void decodeByteSizedInts(
      ByteBuffer buffer,
      int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    for (int i = 0, pos = startOffset; i < numValues; i++, pos++) {
      decoded[i] = buffer.get(pos) & 0xFF;
    }
  }

  static void decodeShortSizedInts(
      ByteBuffer buffer,
      int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    for (int i = 0, pos = startOffset; i < numValues; i++, pos += Short.BYTES) {
      decoded[i] = buffer.get(pos) & 0xFFFF;
    }
  }

  static void decodeBigEndianOddSizedInts(
      ByteBuffer buffer,
      int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    for (int i = 0, pos = startOffset; i < numValues; i++, pos += numBytes) {
      decoded[i] = buffer.getInt(pos) >>> bigEndianShift3;
    }
  }

  static void decodeLittleEndianOddSizedInts(
      ByteBuffer buffer,
      int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    for (int i = 0, pos = startOffset; i < numValues; i++, pos += numBytes) {
      decoded[i] = buffer.getInt(pos) & littleEndianMask3;
    }
  }

  static void decodeIntSizedInts(
      ByteBuffer buffer,
      int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    for (int i = 0, pos = startOffset; i < numValues; i++, pos += Integer.BYTES) {
      decoded[i] = buffer.getInt(pos);
    }
  }

  private static void decodeByteSizedIntsUnsafe(long addr, final int numValues, final int[] decoded, final int numBytes)
  {
    for (int i = 0; i < numValues; i++, addr++) {
      decoded[i] = unsafe.getByte(addr) & 0xFF;
    }
  }

  private static void decodeShortSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    for (int i = 0; i < numValues; i++, addr += 2) {
      decoded[i] = unsafe.getShort(addr) & 0xFFFF;
    }
  }

  private static void decodeBigEndianOddSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    for (int i = 0; i < numValues; i++, addr += numBytes) {
      decoded[i] = unsafe.getInt(addr) >>> bigEndianShift3;
    }
  }

  private static void decodeLittleEndianOddSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    for (int i = 0; i < numValues; i++, addr += numBytes) {
      decoded[i] = unsafe.getInt(addr) & littleEndianMask3;
    }
  }

  private static void decodeIntSizedIntsUnsafe(long addr, final int numValues, final int[] decoded, final int numBytes)
  {
    for (int i = 0; i < numValues; i++, addr += 4) {
      decoded[i] = unsafe.getInt(addr);
    }
  }

  @FunctionalInterface
  public interface DecoderFunction
  {
    void decode(ByteBuffer buffer, int startOffset, int numValues, int[] decoded, int numBytes);
  }

  @FunctionalInterface
  public interface UnsafeDecoderFunction
  {
    void decode(long address, int numValues, int[] decoded, int numBytes);
  }
}
