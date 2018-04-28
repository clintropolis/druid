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
import io.druid.collections.ResourceHolder;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.codecs.ShapeShiftingFormDecoder;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 */
public abstract class ShapeShiftingColumn implements Closeable
{
  public static Unsafe getTheUnsafe()
  {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      return (Unsafe) theUnsafe.get(null);
    }
    catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  final ByteBuffer buffer;
  final int numChunks;
  final int numValues;
  final byte logValuesPerChunk;
  final int valuesPerChunk;
  final int chunkIndexMask;
  final int offsetsSize;
  final ByteOrder byteOrder;
  // shared chunk data areas for decoders that need space
  ResourceHolder<ByteBuffer> bufHolder;
  private final Supplier<ByteBuffer> decompressedDataBuffer;

  int currentChunk = -1;
  private ByteBuffer currentReadBuffer;
  private long currentBaseAddress;
  private int currentBufferOffset;

  public ShapeShiftingColumn(
      final ByteBuffer buffer,
      final int numChunks,
      final int numValues,
      final byte logValuesPerChunk,
      final int offsetsSize,
      final ByteOrder byteOrder
  )
  {
    this.buffer = buffer;
    this.numChunks = numChunks;
    this.numValues = numValues;
    this.logValuesPerChunk = logValuesPerChunk;
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.chunkIndexMask = valuesPerChunk - 1;
    this.offsetsSize = offsetsSize;
    this.byteOrder = byteOrder;

    // todo: meh side effects...
    this.decompressedDataBuffer = Suppliers.memoize(() -> {
      this.bufHolder = logValuesPerChunk == 14 ?
                       CompressedPools.getByteBuf(byteOrder) :
                       logValuesPerChunk == 13 ?
                       CompressedPools.getSmallerByteBuf(byteOrder) :
                       CompressedPools.getSmallestByteBuf(byteOrder);
      return this.bufHolder.get();
    });
  }

  @Override
  public void close() throws IOException
  {
    if (bufHolder != null) {
      bufHolder.close();
    }
  }

  final void loadChunk(int desiredChunk)
  {
    // todo: needed?
    //CHECKSTYLE.OFF: Regexp
//      Preconditions.checkArgument(
//          desiredChunk < numChunks,
//          "desiredChunk[%s] < numChunks[%s]",
//          desiredChunk,
//          numChunks
//      );
    //CHECKSTYLE.ON: Regexp

    currentReadBuffer = buffer;
    currentBaseAddress = -1;
    currentBufferOffset = -1;
    currentChunk = -1;

    // Determine chunk size.
    final int chunkStartReadFrom = ShapeShiftingColumnarIntsSerializer.HEADER_BYTES
                                   + (Integer.BYTES * desiredChunk);
    final int chunkStartByte = buffer.getInt(chunkStartReadFrom)
                               + ShapeShiftingColumnarIntsSerializer.HEADER_BYTES
                               + offsetsSize;
    final int chunkEndByte = buffer.getInt(chunkStartReadFrom + Integer.BYTES)
                             + ShapeShiftingColumnarIntsSerializer.HEADER_BYTES
                             + offsetsSize;

    final int chunkNumValues;

    if (desiredChunk == numChunks - 1) {
      chunkNumValues = (numValues - ((numChunks - 1) * valuesPerChunk));
    } else {
      chunkNumValues = valuesPerChunk;
    }

    final byte chunkCodec = buffer.get(chunkStartByte);

    transform(chunkCodec, chunkStartByte + 1, chunkEndByte, chunkNumValues);

    currentChunk = desiredChunk;
  }

  protected abstract void transform(byte chunkCodec, int chunkStartByte, int chunkEndByte, int chunkNumValues);

  public final ByteBuffer getCurrentReadBuffer()
  {
    return currentReadBuffer;
  }

  ByteBuffer getDecompressedDataBuffer()
  {
    return this.decompressedDataBuffer.get();
  }

  public void setCurrentReadBuffer(ByteBuffer currentReadBuffer)
  {
    this.currentReadBuffer = currentReadBuffer;
  }

  public final long getCurrentBaseAddress()
  {
    return currentBaseAddress;
  }

  public final void setCurrentBaseAddress(long currentBaseAddress)
  {
    this.currentBaseAddress = currentBaseAddress;
  }

  public final int getCurrentBufferOffset()
  {
    return currentBufferOffset;
  }

  public final void setCurrentBufferOffset(int currentBufferOffset)
  {
    this.currentBufferOffset = currentBufferOffset;
  }

  /**
   * Generic Shapeshifting form decoder for chunks that are block compressed using any of {@link CompressionStrategy}.
   * Data is decompressed to 'decompressedDataBuffer' to be further decoded by another {@link ShapeShiftingFormDecoder},
   * via calling 'transform' again on the decompressed chunk.
   *
   * @param <TColumn>
   */
  public class CompressedFormDecoder<TColumn extends ShapeShiftingColumn> extends ShapeShiftingFormDecoder<TColumn>
  {
    private final byte header;

    CompressedFormDecoder(byte logValuesPerChunk, ByteOrder byteOrder, byte header)
    {
      super(logValuesPerChunk, byteOrder);
      this.header = header;
    }

    @Override
    public void transform(
        TColumn columnarInts, int startOffset, int endOffset, int numValues
    )
    {
      final ByteBuffer buffer = columnarInts.getCurrentReadBuffer();
      final ByteBuffer decompressed = getDecompressedDataBuffer();

      final CompressionStrategy.Decompressor decompressor =
          CompressionStrategy.forId(buffer.get(startOffset)).getDecompressor();

      final int size = endOffset - startOffset - 1;
      decompressed.clear();
      decompressor.decompress(buffer, startOffset + 1, size, decompressed);
      columnarInts.setCurrentReadBuffer(decompressed);
      columnarInts.setCurrentBufferOffset(1);
      final byte chunkCodec = decompressed.get(0);
      ShapeShiftingColumn.this.transform(chunkCodec, 1, decompressed.limit(), numValues);
    }

    @Override
    public byte getHeader()
    {
      return header;
    }
  }

  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // todo: idk
    inspector.visit("decompressedDataBuffer", decompressedDataBuffer);
  }
}
