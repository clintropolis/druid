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

package org.apache.druid.segment.data;

import it.unimi.dsi.fastutil.bytes.Byte2IntMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.data.codecs.FormDecoder;
import org.apache.druid.segment.data.codecs.ints.IntCodecs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Reads mapped buffer contents into {@link ShapeShiftingColumnData} to supply {@link ShapeShiftingColumnarInts}
 */
public class ShapeShiftingColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  private static Logger log = new Logger(ShapeShiftingColumnarIntsSupplier.class);

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
   *
   * @param buffer
   * @param byteOrder
   *
   * @return
   */
  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder
  )
  {
    ShapeShiftingColumnData columnData =
        new ShapeShiftingColumnData(buffer, (byte) 2, byteOrder, true);

    return new ShapeShiftingColumnarIntsSupplier(columnData);
  }

  /**
   * Supply a {@link ShapeShiftingColumnarInts}
   *
   * @return
   */
  @Override
  public ColumnarInts get()
  {
    Byte2IntMap composition = columnData.getComposition();

    Byte2ObjectMap<FormDecoder<ShapeShiftingColumnarInts>> decoders = IntCodecs.getDecoders(
        composition.keySet(),
        columnData.getLogValuesPerChunk(),
        columnData.getByteOrder()
    );

    return new ShapeShiftingColumnarInts(columnData, decoders);
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
    // todo: idk
    //    ByteBuffer intToBytesHelperBuffer = ByteBuffer.allocate(Integer.BYTES).order(columnData.getByteOrder());

    //    ShapeShiftingColumnSerializer.writeShapeShiftHeader(
    //        channel,
    //        intToBytesHelperBuffer,
    //        ShapeShiftingColumnarInts.VERSION,
    //        columnData.getNumChunks(),
    //        columnData.getNumValues(),
    //        columnData.getLogValuesPerChunk(),
    //        columnData.getCompositionSize(),
    //        columnData.getOffsetsSize()
    //    );
    channel.write(columnData.getBaseBuffer());
  }
}
