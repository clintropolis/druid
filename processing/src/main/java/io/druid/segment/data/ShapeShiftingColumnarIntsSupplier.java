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

import com.google.common.annotations.VisibleForTesting;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Reads mapped buffer contents into {@link ShapeShiftingColumnData} to supply {@link ShapeShiftingColumnarInts}
 */
public class ShapeShiftingColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  private final ShapeShiftingColumnData columnData;

  private ShapeShiftingColumnarIntsSupplier(
      ShapeShiftingColumnData columnData
  )
  {
    this.columnData = columnData;
  }

  /**
   * Create a new instance from a {@link ByteBuffer} with position set to the start of a
   * {@link ShapeShiftingColumnarInts}
   * @param buffer
   * @param byteOrder
   * @return
   */
  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder
  )
  {
    return fromByteBuffer(buffer, byteOrder, null);
  }

  /**
   * Create from a {@link ByteBuffer} with position set to the start of a {@link ShapeShiftingColumnarInts}, optionally
   * overriding the {@link ShapeShiftingColumnSerializer.DecodeStrategy} for testing, etc.
   * @param buffer
   * @param byteOrder
   * @param overrideDecodeStrategy
   * @return
   */
  @VisibleForTesting
  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder,
      @Nullable Byte overrideDecodeStrategy
  )
  {
    ShapeShiftingColumnData columnData =
        new ShapeShiftingColumnData(buffer, byteOrder, overrideDecodeStrategy, true);

    return new ShapeShiftingColumnarIntsSupplier(columnData);
  }

  /**
   * Supply a {@link ShapeShiftingColumnarInts}
   * @return
   */
  @Override
  public ColumnarInts get()
  {
    if (columnData.getDecodeStrategy() == ShapeShiftingColumnSerializer.DecodeStrategy.BLOCK.byteValue) {
      return new ShapeShiftingBlockColumnarInts(columnData);
    }
    return new ShapeShiftingColumnarInts(columnData);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return columnData.getBaseBuffer().remaining();
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
