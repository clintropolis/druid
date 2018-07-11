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

package io.druid.segment.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Materialized version of outer buffer contents of a {@link ShapeShiftingColumn}, extracting all header information
 * as well as a sliced buffer, prepared for reading, allowing suppliers a tidy structure to instantiate
 * {@link ShapeShiftingColumn} objects.
 *
 * layout:
 * | version (byte) | headerSize (int) | numValues (int) | numChunks (int) | logValuesPerChunk (byte) | offsetsSize (int) |  compositionSize (int) | composition | offsets | values |
 */
public class ShapeShiftingColumnData
{
  private final int headerSize;
  private final int numValues;
  private final int numChunks;
  private final byte logValuesPerChunk;
  private final byte logBytesPerValue;
  private final int compositionSize;
  private final Map<Byte, Integer> composition;
  private final int offsetsSize;
  private final ByteBuffer baseBuffer;
  private final ByteOrder byteOrder;

  public ShapeShiftingColumnData(ByteBuffer buffer, byte logBytesPerValue, ByteOrder byteOrder)
  {
    this(buffer, logBytesPerValue, byteOrder, false);
  }

  public ShapeShiftingColumnData(
      ByteBuffer buffer,
      byte logBytesPerValue,
      ByteOrder byteOrder,
      boolean moveSourceBufferPosition
  )
  {
    ByteBuffer ourBuffer = buffer.slice().order(byteOrder);
    this.byteOrder = byteOrder;
    this.logBytesPerValue = logBytesPerValue;
    this.headerSize = ourBuffer.getInt(1);
    this.numValues = ourBuffer.getInt(1 + Integer.BYTES);
    this.numChunks = ourBuffer.getInt(1 + (2 * Integer.BYTES));
    this.logValuesPerChunk = ourBuffer.get(1 + (3 * Integer.BYTES));
    this.offsetsSize = ourBuffer.getInt(1 + (3 * Integer.BYTES) + 1);
    this.compositionSize = ourBuffer.getInt(1 + (3 * Integer.BYTES) + 1 + Integer.BYTES);

    int compositionOffset = 1 + (3 * Integer.BYTES) + 1 + (2 * Integer.BYTES);
    this.composition = new HashMap<>();
    // 5 bytes per composition entry
    for (int i = 0; i < compositionSize; i += 5) {
      byte header = ourBuffer.get(compositionOffset + i);
      int count = ourBuffer.getInt(compositionOffset + i + 1);
      composition.put(header, count);
    }

    ourBuffer.limit(
        getValueChunksStartOffset() +
        ourBuffer.getInt(getOffsetsStartOffset() + (numChunks * Integer.BYTES))
    );

    if (moveSourceBufferPosition) {
      buffer.position(buffer.position() + ourBuffer.remaining());
    }

    this.baseBuffer = ourBuffer.slice().order(byteOrder);
  }

  /**
   * Total 'header' size, to future proof by allowing us to always be able to find offsets and values sections offsets,
   * but stuffing any additional data into the header.
   *
   * @return
   */
  public int getHeaderSize()
  {
    return headerSize;
  }

  /**
   * Total number of rows in this column
   *
   * @return
   */
  public int getNumValues()
  {
    return numValues;
  }

  /**
   * Number of 'chunks' of values this column is divided into
   *
   * @return
   */
  public int getNumChunks()
  {
    return numChunks;
  }

  /**
   * log base 2 max number of values per chunk
   *
   * @return
   */
  public byte getLogValuesPerChunk()
  {
    return logValuesPerChunk;
  }

  /**
   * log base 2 number of bytes per value
   *
   * @return
   */
  public byte getLogBytesPerValue()
  {
    return logBytesPerValue;
  }

  /**
   * Size in bytes of chunk offset data
   *
   * @return
   */
  public int getOffsetsSize()
  {
    return offsetsSize;
  }

  /**
   * Size in bytes of composition data
   *
   * @return
   */
  public int getCompositionSize()
  {
    return compositionSize;
  }

  /**
   * Get composition of codecs used in column and their counts, allowing column suppliers to optimize at query time.
   * Note that 'compressed' blocks are counted twice, once as compression and once as inner codec, so the total
   * count here may not match {@link ShapeShiftingColumnData#numChunks}
   *
   * @return
   */
  public Map<Byte, Integer> getComposition()
  {
    return composition;
  }

  /**
   * Start offset of {@link ShapeShiftingColumnData#baseBuffer} for the 'chunk offsets' section
   *
   * @return
   */
  public int getOffsetsStartOffset()
  {
    return headerSize;
  }

  /**
   * Start offset of {@link ShapeShiftingColumnData#baseBuffer} for the 'chunk values' section
   *
   * @return
   */
  public int getValueChunksStartOffset()
  {
    return headerSize + offsetsSize;
  }

  /**
   * {@link ByteBuffer} View of column data, sliced from underlying mapped segment smoosh buffer.
   *
   * @return
   */
  public ByteBuffer getBaseBuffer()
  {
    return baseBuffer;
  }

  /**
   * Column byte order
   *
   * @return
   */
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }
}
