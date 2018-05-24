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
 * todo: write stuff
 * Base type for reading 'shape shifting' columns, which divide row values into chunks sized to a power of 2
 */
public abstract class ShapeShiftingColumn<TShapeShiftImpl extends ShapeShiftingColumn> implements Closeable
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
  protected ByteBuffer currentValueBuffer;
  protected ByteBuffer currentMetadataBuffer;
  protected long currentValuesAddress;
  protected int currentValuesStartOffset;
  protected int currentChunkStartOffset;
  protected int currentChunkSize;
  protected int currentChunkNumValues;

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

    currentValueBuffer = buffer;
    currentMetadataBuffer = buffer;
    currentValuesAddress = -1;
    currentValuesStartOffset = -1;
    currentChunkStartOffset = -1;
    currentChunk = -1;
    currentChunkSize = -1;

    final int headerSize = headerSize();
    // Determine chunk size.
    final int chunkStartReadFrom = headerSize + (Integer.BYTES * desiredChunk);
    final int chunkStartByte = buffer.getInt(chunkStartReadFrom) + headerSize + offsetsSize;
    final int chunkEndByte = buffer.getInt(chunkStartReadFrom + Integer.BYTES) + headerSize + offsetsSize;

    if (desiredChunk == numChunks - 1) {
      currentChunkNumValues = (numValues - ((numChunks - 1) * valuesPerChunk));
    } else {
      currentChunkNumValues = valuesPerChunk;
    }

    final byte chunkCodec = buffer.get(chunkStartByte);
    FormDecoder<TShapeShiftImpl> nextForm = getFormDecoder(chunkCodec);

    currentChunkStartOffset = chunkStartByte + 1;
    currentChunkSize = chunkEndByte - currentChunkStartOffset;
    currentValuesStartOffset = currentChunkStartOffset + nextForm.getMetadataSize();
    transform(nextForm);

    currentChunk = desiredChunk;
  }

  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // todo: idk
    inspector.visit("decompressedDataBuffer", decompressedDataBuffer);
  }

  /**
   * Transform this shapeshifting column to be able to read row values for the specified chunk
   * @param nextForm decoder for the form of the next chunk of values
   */
  protected abstract void transform(FormDecoder<TShapeShiftImpl> nextForm);

  protected abstract int headerSize();

  protected abstract FormDecoder<TShapeShiftImpl> getFormDecoder(byte header);

  public ByteBuffer getBuffer()
  {
    return this.buffer;
  }

  ByteBuffer getDecompressedDataBuffer()
  {
    return this.decompressedDataBuffer.get();
  }

  public final ByteBuffer getCurrentValueBuffer()
  {
    return currentValueBuffer;
  }

  public void setCurrentValueBuffer(ByteBuffer currentValueBuffer)
  {
    this.currentValueBuffer = currentValueBuffer;
  }

  public final long getCurrentValuesAddress()
  {
    return currentValuesAddress;
  }

  public final void setCurrentValuesAddress(long currentValuesAddress)
  {
    this.currentValuesAddress = currentValuesAddress;
  }

  public final int getCurrentValuesStartOffset()
  {
    return currentValuesStartOffset;
  }

  public final void setCurrentValuesStartOffset(int currentValuesStartOffset)
  {
    this.currentValuesStartOffset = currentValuesStartOffset;
  }

  public int getCurrentChunkStartOffset()
  {
    return currentChunkStartOffset;
  }

  public void setCurrentChunkStartOffset(int currentChunkStartOffset)
  {
    this.currentChunkStartOffset = currentChunkStartOffset;
  }

  public int getCurrentChunkSize()
  {
    return currentChunkSize;
  }

  public void setCurrentChunkSize(int currentChunkSize)
  {
    this.currentChunkSize = currentChunkSize;
  }

  public int getCurrentChunkNumValues()
  {
    return currentChunkNumValues;
  }

  public void setCurrentChunkNumValues(int currentChunkNumValues)
  {
    this.currentChunkNumValues = currentChunkNumValues;
  }
  /**
   * Generic Shapeshifting form decoder for chunks that are block compressed using any of {@link CompressionStrategy}.
   * Data is decompressed to 'decompressedDataBuffer' to be further decoded by another {@link BaseFormDecoder},
   * via calling 'transform' again on the decompressed chunk.
   *
   */
  public final class CompressedFormDecoder extends BaseFormDecoder<TShapeShiftImpl>
  {
    private final byte header;

    CompressedFormDecoder(byte logValuesPerChunk, ByteOrder byteOrder, byte header)
    {
      super(logValuesPerChunk, byteOrder);
      this.header = header;
    }

    @Override
    public void transform(TShapeShiftImpl shapeshiftingColumn)
    {
      final ByteBuffer buffer = shapeshiftingColumn.getBuffer();
      final ByteBuffer decompressed = shapeshiftingColumn.getDecompressedDataBuffer();
      int startOffset = shapeshiftingColumn.getCurrentChunkStartOffset();
      int endOffset = startOffset + shapeshiftingColumn.getCurrentChunkSize();
      decompressed.clear();

      final CompressionStrategy.Decompressor decompressor =
          CompressionStrategy.forId(buffer.get(startOffset++)).getDecompressor();

      // metadata for inner encoding is stored outside of the compressed values chunk, so set column metadata offsets
      // accordingly
      final byte chunkCodec = buffer.get(startOffset++);
      shapeshiftingColumn.setCurrentChunkStartOffset(startOffset);
      FormDecoder<? extends ShapeShiftingColumn> innerForm = shapeshiftingColumn.getFormDecoder(chunkCodec);
      startOffset += innerForm.getMetadataSize();
      final int size = endOffset - startOffset;

      decompressor.decompress(buffer, startOffset, size, decompressed);
      shapeshiftingColumn.setCurrentValueBuffer(decompressed);
      shapeshiftingColumn.setCurrentValuesStartOffset(0);
      // transform again, this time using the inner form against the the decompressed buffer
      shapeshiftingColumn.transform(innerForm);
    }

    @Override
    public byte getHeader()
    {
      return header;
    }
  }
}
