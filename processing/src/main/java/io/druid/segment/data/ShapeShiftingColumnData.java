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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Materialized version of outer buffer contents of a {@link ShapeShiftingColumn}, providing all header information
 * as well as a buffer prepared for use
 */
public class ShapeShiftingColumnData
{
  private final int numChunks;
  private final int numValues;
  private final byte logValuesPerChunk;
  private final byte decodeStrategy;
  private final int offsetsSize;
  private final ByteBuffer baseBuffer;
  private final ByteOrder byteOrder;

  public ShapeShiftingColumnData(ByteBuffer buffer, ByteOrder byteOrder)
  {
    this(buffer, byteOrder, null, false);
  }

  public ShapeShiftingColumnData(ByteBuffer buffer, ByteOrder byteOrder, boolean moveSourceBufferPosition)
  {
    this(buffer, byteOrder, null, moveSourceBufferPosition);
  }

  public ShapeShiftingColumnData(
      ByteBuffer buffer,
      ByteOrder byteOrder,
      @Nullable Byte override,
      boolean moveSourceBufferPosition
  )
  {
    ByteBuffer ourBuffer = buffer.slice().order(byteOrder);
    this.numChunks = ourBuffer.getInt(1);
    this.numValues = ourBuffer.getInt(1 + Integer.BYTES);
    this.logValuesPerChunk = ourBuffer.get(1 + 2 * Integer.BYTES);
    this.decodeStrategy = override == null ? ourBuffer.get(1 + (2 * Integer.BYTES) + 1) : override;
    this.offsetsSize = ourBuffer.getInt(1 + (2 * Integer.BYTES) + 1 + 1);

    ourBuffer.limit(
        ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + offsetsSize +
        ourBuffer.getInt(ShapeShiftingColumnarIntsSerializer.HEADER_BYTES + (numChunks * Integer.BYTES))
    );

    if (moveSourceBufferPosition) {
      buffer.position(buffer.position() + ourBuffer.remaining());
    }

    this.baseBuffer = ourBuffer.slice().order(byteOrder);
    this.byteOrder = byteOrder;
  }


  public int getNumChunks()
  {
    return numChunks;
  }

  public int getNumValues()
  {
    return numValues;
  }

  public byte getLogValuesPerChunk()
  {
    return logValuesPerChunk;
  }

  public byte getDecodeStrategy()
  {
    return decodeStrategy;
  }

  public int getOffsetsSize()
  {
    return offsetsSize;
  }

  public ByteBuffer getBaseBuffer()
  {
    return baseBuffer;
  }

  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }
}
