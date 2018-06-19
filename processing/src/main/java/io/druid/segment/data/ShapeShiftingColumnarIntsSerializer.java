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
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.io.Closer;
import io.druid.segment.CompressedPools;
import io.druid.segment.IndexSpec;
import io.druid.segment.data.codecs.ints.IntFormEncoder;
import io.druid.segment.data.codecs.ints.IntFormMetrics;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * {@link ShapeShiftingColumnSerializer} implementation for {@link ShapeShiftingColumnarInts}, using
 * {@link IntFormEncoder} to encode values and {@link IntFormMetrics} to analyze them and assist with decisions of how
 * the value chunks will be encoded when 'flushed' to the {@link SegmentWriteOutMedium}.
 */
public class ShapeShiftingColumnarIntsSerializer
    extends ShapeShiftingColumnSerializer<int[], IntFormMetrics>
    implements SingleValueColumnarIntsSerializer
{
  private ResourceHolder<int[]> unencodedValuesHolder;

  public ShapeShiftingColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final IntFormEncoder[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftAggressionLevel aggroLevel,
      @Nullable final ByteOrder overrideByteOrder
  )
  {
    this(
        segmentWriteOutMedium,
        codecs,
        optimizationTarget,
        aggroLevel,
        overrideByteOrder,
        null
    );
  }

  @VisibleForTesting
  public ShapeShiftingColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final IntFormEncoder[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftAggressionLevel aggroLevel,
      @Nullable final ByteOrder overrideByteOrder,
      @Nullable final Byte overrideLogValuesPerChunk
  )
  {
    super(
        segmentWriteOutMedium,
        codecs, optimizationTarget,
        aggroLevel,
        2,
        ShapeShiftingColumnarInts.VERSION,
        overrideByteOrder,
        overrideLogValuesPerChunk
    );

    Closer closer = segmentWriteOutMedium.getCloser();
    if (closer != null) {
      closer.register(() -> {
        if (unencodedValuesHolder != null) {
          unencodedValuesHolder.close();
        }
      });
    }
  }

  @Override
  public void initializeChunk()
  {
    unencodedValuesHolder = CompressedPools.getShapeshiftIntsDecodedValuesArray(logValuesPerChunk);
    currentChunk = unencodedValuesHolder.get();
  }

  @Override
  public void resetChunkCollector()
  {
    chunkMetrics = new IntFormMetrics(optimizationTarget);
  }

  /**
   * Adds a value to the current chunk of ints, stored in an array, analyzing values with {@link IntFormMetrics}, and
   * flushing to the {@link SegmentWriteOutMedium} if the current chunk is full.
   * @param val
   * @throws IOException
   */
  @Override
  public void addValue(final int val) throws IOException
  {
    if (currentChunkPos == valuesPerChunk) {
      flushCurrentChunk();
    }

    chunkMetrics.processNextRow(val);

    currentChunk[currentChunkPos++] = val;
    numValues++;
  }
}
