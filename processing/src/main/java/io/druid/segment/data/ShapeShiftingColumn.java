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
import io.druid.segment.data.codecs.FormDecoder;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Base type for reading 'shape shifting' columns, which divide row values into chunks sized to a power of 2, each
 * potentially encoded with a different algorithm which was chosen as optimal for size and speed for the given values
 * at indexing time. This class generically provides common structure for loading and decoding chunks of values for
 * all shape shifting column implementations, with the help of matching {@link FormDecoder<TShapeShiftImpl>}. Like
 * some other column decoding strategies, shape shifting columns operate with a 'currentChunk' which is loaded when
 * a 'get' operation for a row index is done, and remains until a row index on a different chunk is requested, so
 * performs best of row selection is done in an ordered manner.
 *
 * Shape shifting columns are designed to place row retrieval functions within the column implementation for optimal
 * performance with the jvm. Each chunk has a byte header that uniquely maps to a {@link FormDecoder}, andChunks are
 * decoded by passing them column into {@link FormDecoder} which are tightly coupled to know how to mutate the
 * implementation so row values can be retrieved. What this means is implementation specific, but for the sake of
 * example, could be decoding all values of the chunk to a primitive array or setting offsets to read values directly
 * from a {@link ByteBuffer}. See specific implementations for further details.
 *
 * @param <TShapeShiftImpl> type of {@link ShapeShiftingColumn} implementation to strongly associate {@link FormDecoder}
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

  protected int currentChunk = -1;
  protected ByteBuffer currentValueBuffer;
  protected ByteBuffer currentMetadataBuffer;
  protected long currentValuesAddress;
  protected int currentValuesStartOffset;
  protected int currentChunkStartOffset;
  protected int currentChunkSize;
  protected int currentChunkNumValues;

  protected final Map<Byte, FormDecoder<TShapeShiftImpl>> decoders;
  final ShapeShiftingColumnData columnData;

  public ShapeShiftingColumn(ShapeShiftingColumnData sourceData, Map<Byte, FormDecoder<TShapeShiftImpl>> decoders)
  {
    this.buffer = sourceData.getBaseBuffer();
    this.numChunks = sourceData.getNumChunks();
    this.numValues = sourceData.getNumValues();
    this.logValuesPerChunk = sourceData.getLogValuesPerChunk();
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.chunkIndexMask = valuesPerChunk - 1;
    this.offsetsSize = sourceData.getOffsetsSize();
    this.byteOrder = sourceData.getByteOrder();
    this.decoders = decoders;
    this.columnData = sourceData;

    // todo: meh side effects...
    this.decompressedDataBuffer = Suppliers.memoize(() -> {
      this.bufHolder = CompressedPools.getShapeshiftDecodedValuesBuffer(
          logValuesPerChunk + sourceData.getLogBytesPerValue(),
          byteOrder
      );
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

  /**
   * This method loads and decodes a chunk of values, and should be called in column 'get' methods to change the current
   * chunk
   *
   * @param desiredChunk
   */
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

    // Determine chunk size.
    final int chunkStartReadFrom = columnData.getOffsetsStartOffset() + (Integer.BYTES * desiredChunk);
    final int chunkStartByte = columnData.getValueChunksStartOffset() + buffer.getInt(chunkStartReadFrom);
    final int chunkEndByte = columnData.getValueChunksStartOffset() +
                             buffer.getInt(chunkStartReadFrom + Integer.BYTES);

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
   *
   * @param nextForm decoder for the form of the next chunk of values
   */
  public abstract void transform(FormDecoder<TShapeShiftImpl> nextForm);

  /**
   * Get header size, used to correctly calculate chunk and offset locations in underlying bytebuffer.
   *
   * @return
   */
  protected abstract int headerSize();

  /**
   * Get form decoder mapped to chunk header, used to transform this column to prepare for value reading
   *
   * @param header
   *
   * @return
   */
  public abstract FormDecoder<TShapeShiftImpl> getFormDecoder(byte header);

  /**
   * Get underlying column buffer sliced from mapped smoosh
   *
   * @return
   */
  public ByteBuffer getBuffer()
  {
    return this.buffer;
  }

  /**
   * Get shared temporary bytebuffer for decompression
   *
   * @return
   */
  public ByteBuffer getDecompressedDataBuffer()
  {
    return this.decompressedDataBuffer.get();
  }

  /**
   * Get current {@link ByteBuffer} to read row values from
   *
   * @return
   */
  public final ByteBuffer getCurrentValueBuffer()
  {
    return currentValueBuffer;
  }

  /**
   * Set bytebuffer to read row values from
   *
   * @param currentValueBuffer
   */
  public void setCurrentValueBuffer(ByteBuffer currentValueBuffer)
  {
    this.currentValueBuffer = currentValueBuffer;
  }

  /**
   * Get 'unsafe' memory address of current value chunk direct buffer
   *
   * @return
   */
  public final long getCurrentValuesAddress()
  {
    return currentValuesAddress;
  }

  /**
   * Set 'unsafe' memory address of current value chunk direct buffer
   *
   * @param currentValuesAddress
   */
  public final void setCurrentValuesAddress(long currentValuesAddress)
  {
    this.currentValuesAddress = currentValuesAddress;
  }

  /**
   * Get buffer offset of base column buffer for values of current chunk. If chunk has it's own encoding metadata, this
   * may be offset from the start of the chunk itself.
   *
   * @return
   */
  public final int getCurrentValuesStartOffset()
  {
    return currentValuesStartOffset;
  }

  /**
   * Set buffer offset of base column buffer for current value chunk. If chunk has it's own encoding metadata, this
   * may be offset from the start of the chunk itself
   */
  public final void setCurrentValuesStartOffset(int currentValuesStartOffset)
  {
    this.currentValuesStartOffset = currentValuesStartOffset;
  }

  /**
   * Get buffer offset of base column buffer for start of current chunk.
   *
   * @return
   */
  public int getCurrentChunkStartOffset()
  {
    return currentChunkStartOffset;
  }

  /**
   * Set buffer offset of base column buffer for current chunk.
   *
   * @param currentChunkStartOffset
   */
  public void setCurrentChunkStartOffset(int currentChunkStartOffset)
  {
    this.currentChunkStartOffset = currentChunkStartOffset;
  }

  /**
   * Get size in bytes of current chunk
   *
   * @return
   */
  public int getCurrentChunkSize()
  {
    return currentChunkSize;
  }

  /**
   * Set size in bytes of current chunk
   *
   * @param currentChunkSize
   */
  public void setCurrentChunkSize(int currentChunkSize)
  {
    this.currentChunkSize = currentChunkSize;
  }

  /**
   * Get number of rows in current chunk
   *
   * @return
   */
  public int getCurrentChunkNumValues()
  {
    return currentChunkNumValues;
  }

  /**
   * Set number of rows in current chunk
   *
   * @param currentChunkNumValues
   */
  public void setCurrentChunkNumValues(int currentChunkNumValues)
  {
    this.currentChunkNumValues = currentChunkNumValues;
  }
}
