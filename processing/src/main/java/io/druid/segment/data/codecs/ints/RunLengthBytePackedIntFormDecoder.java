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
import io.druid.segment.data.codecs.ShapeShiftingFormDecoder;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RunLengthBytePackedIntFormDecoder extends ShapeShiftingFormDecoder<ShapeShiftingColumnarInts>
{
  static final int mask1 = 0x7F;
  static final int mask2 = 0x7FFF;
  static final int mask3 = 0x7FFFFF;
  static final int mask4 = 0x7FFFFFFF;

  static final int runMask1 = 0x80;
  static final int runMask2 = 0x8000;
  static final int runMask3 = 0x800000;
  static final int runMask4 = 0x80000000;

  public static final int bigEndianShift3 = Integer.SIZE - 24;
  public static final int littleEndianMask3 = (int) ((1L << 24) - 1);

  private static final Unsafe unsafe = ShapeShiftingColumnarInts.getTheUnsafe();


  final BytePackedIntFormDecoder.DecoderFunction oddFunction;
  final BytePackedIntFormDecoder.UnsafeDecoderFunction oddFunctionUnsafe;


  public RunLengthBytePackedIntFormDecoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
    boolean isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    this.oddFunction = !isBigEndian
                       ? RunLengthBytePackedIntFormDecoder::decodeLittleEndianOddSizedInts
                       : RunLengthBytePackedIntFormDecoder::decodeBigEndianOddSizedInts;
    this.oddFunctionUnsafe = !isBigEndian
                             ? RunLengthBytePackedIntFormDecoder::decodeLittleEndianOddSizedIntsUnsafe
                             : RunLengthBytePackedIntFormDecoder::decodeBigEndianOddSizedIntsUnsafe;

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
  public byte getHeader()
  {
    return IntCodecs.RLE_BYTEPACK;
  }


  private static void decodeByteSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    int runCount;
    int runValue;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = unsafe.getByte(addr++) & 0xFF;
      if ((nextVal & runMask1) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask1;
        runValue = unsafe.getByte(addr++) & 0xFF;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeShortSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    int runCount;
    int runValue;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = unsafe.getShort(addr) & 0xFFFF;
      addr += 2;
      if ((nextVal & runMask2) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask2;
        runValue = unsafe.getShort(addr) & 0xFFFF;
        addr += 2;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }


  private static void decodeBigEndianOddSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // todo: numBytes is always 3...
    // example for numBytes = 3
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    int runCount;
    int runValue;
    for (int i = 0; i < numValues; i++) {
      final int nextVal = unsafe.getInt(addr) >>> bigEndianShift3;
      addr += 3;
      if ((nextVal & runMask3) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask3;
        runValue = unsafe.getInt(addr) >>> bigEndianShift3;
        addr += 3;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeLittleEndianOddSizedIntsUnsafe(
      long addr,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // todo: numBytes is always 3...
    // example for numBytes = 3
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    int runCount;
    int runValue;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = unsafe.getInt(addr) & littleEndianMask3;
      addr += 3;
      if ((nextVal & runMask3) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask3;
        runValue = unsafe.getInt(addr) & littleEndianMask3;
        addr += 3;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeIntSizedIntsUnsafe(long addr, final int numValues, final int[] decoded, final int numBytes)
  {
    int runCount;
    int runValue;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = unsafe.getInt(addr);
      addr += 4;
      if ((nextVal & runMask4) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask4;
        runValue = unsafe.getInt(addr);
        addr += 4;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeByteSizedInts(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    int runCount;
    int runValue;
    int bufferPosition = startOffset;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = buffer.get(bufferPosition++) & 0xFF;
      if ((nextVal & runMask1) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask1;
        runValue = buffer.get(bufferPosition++) & 0xFF;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeShortSizedInts(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    int bufferPosition = startOffset;
    int runCount;
    int runValue;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = buffer.getShort(bufferPosition) & 0xFFFF;
      bufferPosition += Short.BYTES;
      if ((nextVal & runMask2) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask2;
        runValue = buffer.get(bufferPosition) & 0xFFFF;
        bufferPosition += Short.BYTES;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeBigEndianOddSizedInts(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // todo: numBytes is always 3...
    // example for numBytes = 3
    // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
    int runCount;
    int runValue;
    int bufferPosition = startOffset;
    for (int i = 0; i < numValues; i++) {
      final int nextVal = buffer.getInt(bufferPosition) >>> bigEndianShift3;
      bufferPosition += numBytes;
      if ((nextVal & runMask3) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask3;
        runValue = buffer.getInt(bufferPosition) >>> bigEndianShift3;
        bufferPosition += numBytes;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeLittleEndianOddSizedInts(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    // todo: numBytes is always 3...
    // example for numBytes = 3
    // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
    int runCount;
    int runValue;
    int bufferPosition = startOffset;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = buffer.getInt(bufferPosition) & littleEndianMask3;
      bufferPosition += numBytes;
      if ((nextVal & runMask3) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask3;
        runValue = buffer.getInt(bufferPosition) & littleEndianMask3;
        bufferPosition += numBytes;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }

  private static void decodeIntSizedInts(
      ByteBuffer buffer,
      final int startOffset,
      final int numValues,
      final int[] decoded,
      final int numBytes
  )
  {
    int runCount;
    int runValue;
    int bufferPosition = startOffset;

    for (int i = 0; i < numValues; i++) {
      final int nextVal = buffer.getInt(bufferPosition);
      bufferPosition += Integer.BYTES;
      if ((nextVal & runMask4) == 0) {
        decoded[i] = nextVal;
      } else {
        runCount = nextVal & mask4;
        runValue = buffer.getInt(bufferPosition);
        bufferPosition += Integer.BYTES;
        do {
          decoded[i] = runValue;
        } while (--runCount > 0 && ++i < numValues);
      }
    }
  }
}
