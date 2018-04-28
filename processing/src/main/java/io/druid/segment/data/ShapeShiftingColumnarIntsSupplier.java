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

import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class ShapeShiftingColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  private final ByteBuffer buffer;
  private final int numChunks;
  private final int numValues;
  private final byte logValuesPerChunk;
  private final int offsetsSize;
  private final ByteOrder byteOrder;
  private final byte decodeStrategy;
  private ResourceHolder<ByteBuffer> bufHolder;

  private ShapeShiftingColumnarIntsSupplier(
      final ByteBuffer buffer,
      final int numChunks,
      final int numValues,
      final byte logValuesPerChunk,
      final byte decodeStrategy,
      final int offsetsSize,
      final ByteOrder byteOrder
  )
  {
    this.buffer = buffer;
    this.numChunks = numChunks;
    this.numValues = numValues;
    this.logValuesPerChunk = logValuesPerChunk;
    this.decodeStrategy = decodeStrategy;
    this.offsetsSize = offsetsSize;
    this.byteOrder = byteOrder;
  }

  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder
  )
  {
    final ByteBuffer ourBuffer = buffer.slice().order(byteOrder);
    final int numChunks = ourBuffer.getInt(1);
    final int numValues = ourBuffer.getInt(1 + Integer.BYTES);
    final byte logValuesPerChunk = ourBuffer.get(1 + 2 * Integer.BYTES);
    final byte decodeStrategy = ourBuffer.get(1 + (2 * Integer.BYTES) + 1);
    final int offsetsSize = ourBuffer.getInt(1 + (2 * Integer.BYTES) + 1 + 1);

    ourBuffer.limit(
        ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + offsetsSize +
        ourBuffer.getInt(ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + numChunks * Integer.BYTES)
    );

    buffer.position(buffer.position() + ourBuffer.remaining());

    return new ShapeShiftingColumnarIntsSupplier(
        ourBuffer.slice().order(byteOrder),
        numChunks,
        numValues,
        logValuesPerChunk,
        decodeStrategy,
        offsetsSize,
        byteOrder
    );
  }

  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder,
      final byte overrideDecodeStrategy
  )
  {
    final ByteBuffer ourBuffer = buffer.slice().order(byteOrder);
    final int numChunks = ourBuffer.getInt(1);
    final int numValues = ourBuffer.getInt(1 + Integer.BYTES);
    final byte logValuesPerChunk = ourBuffer.get(1 + 2 * Integer.BYTES);
    final int offsetsSize = ourBuffer.getInt(1 + (2 * Integer.BYTES) + 1 + 1);

    ourBuffer.limit(
        ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + offsetsSize +
        ourBuffer.getInt(ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + numChunks * Integer.BYTES)
    );

    buffer.position(buffer.position() + ourBuffer.remaining());

    return new ShapeShiftingColumnarIntsSupplier(
        ourBuffer.slice().order(byteOrder),
        numChunks,
        numValues,
        logValuesPerChunk,
        overrideDecodeStrategy,
        offsetsSize,
        byteOrder
    );
  }

  @Override
  public ColumnarInts get()
  {
    return decodeStrategy == 0 ?
           new ShapeShiftingColumnarInts(
               buffer,
               numChunks,
               numValues,
               logValuesPerChunk,
               offsetsSize,
               byteOrder
           ) :
           new ShapeShiftingBlockColumnarInts(
               buffer,
               numChunks,
               numValues,
               logValuesPerChunk,
               offsetsSize,
               byteOrder
           );
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return buffer.remaining();
  }

  @Override
  public void writeTo(
      final WritableByteChannel channel,
      final FileSmoosher smoosher
  ) throws IOException
  {
    throw new UnsupportedOperationException();
  }
}
