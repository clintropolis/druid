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

import com.google.common.annotations.VisibleForTesting;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.data.codecs.DirectFormDecoder;
import io.druid.segment.data.codecs.FormDecoder;
import io.druid.segment.data.codecs.ints.IntCodecs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads mapped buffer contents into {@link ShapeShiftingColumnData} to supply {@link ShapeShiftingColumnarInts}
 */
public class ShapeShiftingColumnarIntsSupplier implements WritableSupplier<ColumnarInts>
{
  private static Logger log = new Logger(ShapeShiftingColumnarIntsSupplier.class);

  private final ShapeShiftingColumnData columnData;
  // null = default, false = force ShapeShiftingColumnarInts, true = force ShapeShiftingBlockColumnarInts
  private final ShapeShiftingColumnarIntsDecodeOptimization overrideOptimization;

  private ShapeShiftingColumnarIntsSupplier(
      ShapeShiftingColumnData columnData,
      @Nullable ShapeShiftingColumnarIntsDecodeOptimization overrideOptimization
  )
  {
    this.columnData = columnData;
    this.overrideOptimization = overrideOptimization;
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

    return new ShapeShiftingColumnarIntsSupplier(columnData, null);
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
  @VisibleForTesting
  public static ShapeShiftingColumnarIntsSupplier fromByteBuffer(
      final ByteBuffer buffer,
      final ByteOrder byteOrder,
      ShapeShiftingColumnarIntsDecodeOptimization overrideOptimization
  )
  {
    ShapeShiftingColumnData columnData =
        new ShapeShiftingColumnData(buffer, (byte) 2, byteOrder, true);

    return new ShapeShiftingColumnarIntsSupplier(columnData, overrideOptimization);
  }

  /**
   * Supply a {@link ShapeShiftingColumnarInts}
   *
   * @return
   */
  @Override
  public ColumnarInts get()
  {
    Map<Byte, Integer> composition = columnData.getComposition();

    Map<Byte, FormDecoder<ShapeShiftingColumnarInts>> decoders = IntCodecs.getDecoders(
        composition.keySet().stream().collect(Collectors.toList()),
        columnData.getLogValuesPerChunk(),
        columnData.getByteOrder()
    );

    ShapeShiftingColumnarIntsDecodeOptimization optimization =
        overrideOptimization != null
        ? overrideOptimization
        : ShapeShiftingColumnarIntsDecodeOptimization.fromComposition(columnData, decoders);

    switch (optimization) {
      case BLOCK:
        return new ShapeShiftingBlockColumnarInts(columnData, decoders);
      case MIXED:
      default:
        return new ShapeShiftingColumnarInts(columnData, decoders);
    }
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

  public enum ShapeShiftingColumnarIntsDecodeOptimization
  {
    MIXED,
    BLOCK;

    public static ShapeShiftingColumnarIntsDecodeOptimization fromComposition(
        ShapeShiftingColumnData columnData,
        Map<Byte, FormDecoder<ShapeShiftingColumnarInts>> decoders
    )
    {
      int numDirectAccess = 0;
      int preferDirectAccess = 0;
      for (FormDecoder<ShapeShiftingColumnarInts> intDecoder : decoders.values()) {
        if (intDecoder instanceof DirectFormDecoder) {
          numDirectAccess++;
          if (((DirectFormDecoder) intDecoder).preferDirectAccess()) {
            preferDirectAccess++;
          }
        }
      }

      //todo: legit? if less than 1/n prefer direct access, block optimize?
      //todo: buffer/unsafe optimized version?
      if (preferDirectAccess < columnData.getNumChunks() / 10) {
        log.info(
            "Using block optimized strategy, %d:%d have random access, %d prefer random access",
            numDirectAccess,
            columnData.getNumChunks(),
            preferDirectAccess
        );
        return BLOCK;
      } else {
        log.info(
            "Using mixed access strategy, %d:%d have random access, %d prefer random access",
            numDirectAccess,
            columnData.getNumChunks(),
            preferDirectAccess
        );
        return MIXED;
      }
    }
  }
}
