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

package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.druid.collections.ResourceHolder;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.codecs.DirectFormDecoder;
import io.druid.segment.data.codecs.FormDecoder;
import io.druid.segment.data.codecs.ints.BytePackedIntFormDecoder;
import io.druid.segment.data.codecs.ints.ConstantIntFormDecoder;
import io.druid.segment.data.codecs.ints.IntCodecs;
import io.druid.segment.data.codecs.ints.LemireIntFormDecoder;
import io.druid.segment.data.codecs.ints.RunLengthBytePackedIntFormDecoder;
import io.druid.segment.data.codecs.ints.UnencodedIntFormDecoder;
import io.druid.segment.data.codecs.ints.ZeroIntFormDecoder;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public class ShapeShiftingColumnarInts extends ShapeShiftingColumn<ShapeShiftingColumnarInts> implements ColumnarInts
{
  public static final byte VERSION = 0x4; // todo: idk..

  protected static final Unsafe unsafe = getTheUnsafe();

  private final SkippableIntegerCODEC fastPforCodec = new SkippableComposition(new FastPFOR(), new VariableByte());
  protected final GetIntBuffer oddSizeValueGet;
  protected final GetIntUnsafe oddSizeValueGetUnsafe;
  final Map<Byte, FormDecoder<ShapeShiftingColumnarInts>> decoders;
  ResourceHolder<int[]> decodedValuesHolder;
  ResourceHolder<int[]> tmpValuesHolder;

  private final Supplier<int[]> tmpSupplier;
  private final Supplier<int[]> decodedValuesSupplier;

  protected int[] tmp;
  protected int[] decodedValues;
  protected DecodeIndex currentForm;
  protected int currentBytesPerValue = 4;
  protected int currentConstant = 0;

  public ShapeShiftingColumnarInts(ShapeShiftingColumnData sourceData)
  {
    super(sourceData);

    this.tmpSupplier = Suppliers.memoize(() -> {
      tmpValuesHolder = CompressedPools.getShapeshiftIntsEncodedValuesArray(logValuesPerChunk);
      return tmpValuesHolder.get();
    });
    this.decodedValuesSupplier = Suppliers.memoize(() -> {
      decodedValuesHolder = CompressedPools.getShapeshiftIntsDecodedValuesArray(logValuesPerChunk);
      return decodedValuesHolder.get();
    });

    // todo: make more better, this is fragile and burried
    this.decoders = ImmutableMap.<Byte, FormDecoder<ShapeShiftingColumnarInts>>builder()
        .put(IntCodecs.ZERO, new ZeroIntFormDecoder(logValuesPerChunk, byteOrder))
        .put(IntCodecs.CONSTANT, new ConstantIntFormDecoder(logValuesPerChunk, byteOrder))
        .put(IntCodecs.UNENCODED, new UnencodedIntFormDecoder(logValuesPerChunk, byteOrder))
        .put(IntCodecs.BYTEPACK, new BytePackedIntFormDecoder(logValuesPerChunk, byteOrder))
        .put(IntCodecs.RLE_BYTEPACK, new RunLengthBytePackedIntFormDecoder(logValuesPerChunk, byteOrder))
        .put(IntCodecs.COMPRESSED, new CompressedFormDecoder(logValuesPerChunk, byteOrder, IntCodecs.COMPRESSED))
        .put(IntCodecs.FASTPFOR, new LemireIntFormDecoder(logValuesPerChunk, IntCodecs.FASTPFOR, fastPforCodec))
        .build();
    oddSizeValueGet = byteOrder.equals(ByteOrder.LITTLE_ENDIAN)
                      ? (_buffer, pos) -> _buffer.getInt(pos) & BytePackedIntFormDecoder.littleEndianMask3
                      : (_buffer, pos) -> _buffer.getInt(pos) >>> BytePackedIntFormDecoder.bigEndianShift3;
    oddSizeValueGetUnsafe = byteOrder.equals(ByteOrder.LITTLE_ENDIAN)
                            ? (pos) -> unsafe.getInt(pos) & BytePackedIntFormDecoder.littleEndianMask3
                            : (pos) -> unsafe.getInt(pos) >>> BytePackedIntFormDecoder.bigEndianShift3;
  }

  @Override
  public int size()
  {
    return numValues;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // todo: idk
    super.inspectRuntimeShape(inspector);
    inspector.visit("tmp", tmp);
    inspector.visit("decodedValues", decodedValuesSupplier);
  }

  @Override
  public void close() throws IOException
  {
    super.close();
    if (tmpValuesHolder != null) {
      tmpValuesHolder.close();
    }
    if (decodedValuesHolder != null) {
      decodedValuesHolder.close();
    }
  }

  @Override
  protected int headerSize()
  {
    return ShapeShiftingColumnarIntsSerializer.HEADER_BYTES;
  }

  @Override
  protected FormDecoder<ShapeShiftingColumnarInts> getFormDecoder(byte header)
  {
    return decoders.get(header);
  }

  @Override
  public int get(final int index)
  {
    final int desiredChunk = index >> logValuesPerChunk;

    if (desiredChunk != currentChunk) {
      loadChunk(desiredChunk);
    }

    return currentForm.decode(index & chunkIndexMask);
  }

  /**
   * temporary working integer array sized to number of values + 1024, for use by transformations that
   * require copying values to an int array before decoding
   *
   * @return
   */
  public final int[] getTmp()
  {
    return tmpSupplier.get();
  }

  /**
   * integer array sized to number of values, to allow {@link FormDecoder} a place for fully
   * decoded values upon transformation
   *
   * @return
   */
  public final int[] getDecodedValues()
  {
    return decodedValues = decodedValuesSupplier.get();
  }

  /**
   * current 'constant' value, for constant chunk transformations
   *
   * @return
   */
  public final int getCurrentConstant()
  {
    return currentConstant;
  }

  /**
   * Allows {@link FormDecoder} to set current 'constant' value during a transformation.
   *
   * @param currentConstant
   */
  public final void setCurrentConstant(int currentConstant)
  {
    this.currentConstant = currentConstant;
  }

  /**
   * Get current number of bytes used per value for random access transformations.
   *
   * @return
   */
  public int getCurrentBytesPerValue()
  {
    return currentBytesPerValue;
  }

  /**
   * Allows {@link FormDecoder} to set current number of bytes for value, for random access transformations
   *
   * @param currentBytesPerValue
   */
  public void setCurrentBytesPerValue(int currentBytesPerValue)
  {
    this.currentBytesPerValue = currentBytesPerValue;
  }

  /**
   * Transform {@link ShapeShiftingColumnarInts} to the form of the requested chunk, which may either be eagerly
   * decoded entirely to {@link ShapeShiftingColumnarInts#decodedValuesSupplier} with values retrieved by
   * {@link ShapeShiftingColumnarInts#decodeBlockForm(int)}, or randomly accessible, which may set
   * {@link ShapeShiftingColumnarInts#currentValuesAddress}, {@link ShapeShiftingColumnarInts#currentValuesStartOffset},
   * {@link ShapeShiftingColumnarInts#currentBytesPerValue}, {@link ShapeShiftingColumnarInts#currentConstant} and be
   * decoded by {@link ShapeShiftingColumnarInts#decodeBufferForm(int)}.
   *
   * @param nextForm
   */
  @Override
  protected void transform(FormDecoder<ShapeShiftingColumnarInts> nextForm)
  {
    currentBytesPerValue = 4;
    currentConstant = 0;
    if (nextForm instanceof DirectFormDecoder) {
      if (getCurrentValueBuffer().isDirect() && byteOrder.equals(ByteOrder.nativeOrder())) {
        currentForm = this::decodeUnsafeForm;
        ((DirectFormDecoder<ShapeShiftingColumnarInts>) nextForm).transformUnsafe(this);
      } else {
        currentForm = this::decodeBufferForm;
        ((DirectFormDecoder<ShapeShiftingColumnarInts>) nextForm).transformBuffer(this);
      }
    } else {
      currentForm = this::decodeBlockForm;
      nextForm.transform(this);
    }
  }

  /**
   * get value at index produced {@link FormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeBlockForm(int index)
  {
    return decodedValues[index];
  }

  /**
   * get value (unsafe) at index produced by {@link DirectFormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeUnsafeForm(int index)
  {
    final long pos = currentValuesAddress + (index * currentBytesPerValue);
    switch (currentBytesPerValue) {
      case 1:
        return unsafe.getByte(pos) & 0xFF;
      case 2:
        return unsafe.getShort(pos) & 0xFFFF;
      case 3:
        return oddSizeValueGetUnsafe.getInt(pos);
      case 4:
        return unsafe.getInt(pos);
      default:
        return currentConstant;
    }
  }

  /**
   * get value at index produced by {@link DirectFormDecoder} transformation
   *
   * @param index masked index into the chunk array (index & {@link ShapeShiftingColumnarInts#chunkIndexMask})
   *
   * @return decoded row value at index
   */
  private int decodeBufferForm(int index)
  {
    final int pos = getCurrentValuesStartOffset() + (index * currentBytesPerValue);
    final ByteBuffer buffer = getCurrentValueBuffer();
    switch (currentBytesPerValue) {
      case 1:
        return buffer.get(pos) & 0xFF;
      case 2:
        return buffer.getShort(pos) & 0xFFFF;
      case 3:
        return oddSizeValueGet.getInt(buffer, pos);
      case 4:
        return buffer.getInt(pos);
      default:
        return currentConstant;
    }
  }

  @FunctionalInterface
  public interface DecodeIndex
  {
    int decode(int index);
  }

  @FunctionalInterface
  protected interface GetIntUnsafe
  {
    int getInt(long index);
  }

  @FunctionalInterface
  protected interface GetIntBuffer
  {
    int getInt(ByteBuffer buffer, int index);
  }
}
