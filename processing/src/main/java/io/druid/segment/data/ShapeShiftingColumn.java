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
import io.druid.segment.data.codecs.BaseFormDecoder;
import io.druid.segment.data.codecs.FormDecoder;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Base type for reading 'shape shifting' columns, which divide row values into log 2 sized chunks with varying encoding
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
  protected ByteBuffer currentReadBuffer;
  protected ByteBuffer currentMetadataBuffer;
  protected long currentBaseAddress;
  protected int currentBufferOffset;
  protected int currentMetadataOffset;

  public ShapeShiftingColumn(ShapeShiftingColumnData sourceData)
  {
    this.buffer = sourceData.getBaseBuffer();
    this.numChunks = sourceData.getNumChunks();
    this.numValues = sourceData.getNumValues();
    this.logValuesPerChunk = sourceData.getLogValuesPerChunk();
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.chunkIndexMask = valuesPerChunk - 1;
    this.offsetsSize = sourceData.getOffsetsSize();
    this.byteOrder = sourceData.getByteOrder();

    // todo: meh side effects...
    this.decompressedDataBuffer = Suppliers.memoize(() -> {
      // todo: lame af, use enum
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
    currentMetadataBuffer = buffer;
    currentBaseAddress = -1;
    currentBufferOffset = -1;
    currentMetadataOffset = -1;
    currentChunk = -1;

    final int headerSize = headerSize();
    // Determine chunk size.
    final int chunkStartReadFrom = headerSize + (Integer.BYTES * desiredChunk);
    int chunkStartByte = buffer.getInt(chunkStartReadFrom) + headerSize + offsetsSize;
    final int chunkEndByte = buffer.getInt(chunkStartReadFrom + Integer.BYTES) + headerSize + offsetsSize;

    final int chunkNumValues;

    if (desiredChunk == numChunks - 1) {
      chunkNumValues = (numValues - ((numChunks - 1) * valuesPerChunk));
    } else {
      chunkNumValues = valuesPerChunk;
    }

    final byte chunkCodec = buffer.get(chunkStartByte++);
    FormDecoder decoder = getFormDecoder(chunkCodec);
    currentMetadataOffset = chunkStartByte;
    chunkStartByte += decoder.getMetadataSize();
    currentBufferOffset = chunkStartByte;
    transform(chunkCodec, chunkStartByte, chunkEndByte, chunkNumValues);

    currentChunk = desiredChunk;
  }

  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // todo: idk
    inspector.visit("decompressedDataBuffer", decompressedDataBuffer);
  }

  /**
   * Transform this shapeshifting column to be able to read row values for the specified chunk
   *
   * @param chunkCodec     byte value indicating which form decoder to use
   * @param chunkStartByte start byte of chunk in {@link ShapeShiftingColumn#getCurrentReadBuffer()}
   * @param chunkEndByte   end byte of chunk in {@link ShapeShiftingColumn#getCurrentReadBuffer()}}
   * @param chunkNumValues number of values in chunk
   */
  protected abstract void transform(byte chunkCodec, int chunkStartByte, int chunkEndByte, int chunkNumValues);

  protected abstract int headerSize();

  protected abstract FormDecoder<? extends ShapeShiftingColumn> getFormDecoder(byte header);

  ByteBuffer getBuffer()
  {
    return this.buffer;
  }

  ByteBuffer getDecompressedDataBuffer()
  {
    return this.decompressedDataBuffer.get();
  }

  public final ByteBuffer getCurrentReadBuffer()
  {
    return currentReadBuffer;
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

  public ByteBuffer getCurrentMetadataBuffer()
  {
    return currentMetadataBuffer;
  }

  public void setCurrentMetadataBuffer(ByteBuffer currentMetadataBuffer)
  {
    this.currentMetadataBuffer = currentMetadataBuffer;
  }

  public int getCurrentMetadataOffset()
  {
    return currentMetadataOffset;
  }

  public void setCurrentMetadataOffset(int currentMetadataOffset)
  {
    this.currentMetadataOffset = currentMetadataOffset;
  }

  /**
   * Generic Shapeshifting form decoder for chunks that are block compressed using any of {@link CompressionStrategy}.
   * Data is decompressed to 'decompressedDataBuffer' to be further decoded by another {@link BaseFormDecoder},
   * via calling 'transform' again on the decompressed chunk.
   *
   * @param <TColumn>
   */
  public final class CompressedFormDecoder<TColumn extends ShapeShiftingColumn> extends BaseFormDecoder<TColumn>
  {
    private final byte header;

    CompressedFormDecoder(byte logValuesPerChunk, ByteOrder byteOrder, byte header)
    {
      super(logValuesPerChunk, byteOrder);
      this.header = header;
    }

    @Override
    public void transform(
        TColumn shapeshiftingColumn, int startOffset, int endOffset, int numValues
    )
    {
      final ByteBuffer buffer = shapeshiftingColumn.getBuffer();
      final ByteBuffer decompressed = shapeshiftingColumn.getDecompressedDataBuffer();
      decompressed.clear();

      final CompressionStrategy.Decompressor decompressor =
          CompressionStrategy.forId(buffer.get(startOffset++)).getDecompressor();

      // metadata for inner encoding is stored outside of the compressed values chunk, so set column metadata offsets
      // accordingly
      final byte chunkCodec = buffer.get(startOffset++);
      shapeshiftingColumn.setCurrentMetadataBuffer(buffer);
      shapeshiftingColumn.setCurrentMetadataOffset(startOffset);
      final int metaSize = shapeshiftingColumn.getFormDecoder(chunkCodec).getMetadataSize();
      startOffset += metaSize;
      final int size = endOffset - startOffset;

      decompressor.decompress(buffer, startOffset, size, decompressed);
      shapeshiftingColumn.setCurrentReadBuffer(decompressed);
      shapeshiftingColumn.setCurrentBufferOffset(0);
      // transform again, against the the decompressed buffer which is now the columns read buffer
      shapeshiftingColumn.transform(chunkCodec, 0, decompressed.limit(), numValues);
    }

    @Override
    public byte getHeader()
    {
      return header;
    }
  }
}
