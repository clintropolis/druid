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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.IndexSpec;
import io.druid.segment.data.codecs.ints.IntFormEncoder;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class ShapeShiftingColumnarIntsSerializer extends SingleValueColumnarIntsSerializer
{
  /**
   * | version (byte) | numChunks (int) | numValues (int) | logValuesPerChunk (byte) | decodeStrategy (byte) | offsetsOutSize (int) | offsets | values |
   */
  static final int HEADER_BYTES = 1 + (2 * Integer.BYTES) + 1 + 1 + Integer.BYTES;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IntFormEncoder[] codecs;
  private final byte logValuesPerChunk;
  private final int valuesPerChunk;
  private final ByteBuffer intToBytesHelperBuffer;
  private final Map<String, Integer> usage = Maps.newHashMap();
  private final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;
  private WriteOutBytes offsetsOut;
  private WriteOutBytes valuesOut;
  private int[] currentChunk;
  private int currentChunkPos = 0;
  private int numChunks = 0;
  private int numChunksWithRandomAccess = 0;
  private int numValues = 0;
  private IntFormMetrics chunkMetrics;
  private boolean wroteFinalOffset = false;

  public ShapeShiftingColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final IntFormEncoder[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftAggressionLevel aggroLevel,
      @Nullable final ByteOrder overrideByteOrder
  )
  {
    this.segmentWriteOutMedium = Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    this.logValuesPerChunk = aggroLevel.getBlockSize();
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.codecs = codecs;
    this.optimizationTarget = optimizationTarget;
    ByteOrder byteOrder = overrideByteOrder == null ? ByteOrder.nativeOrder() : overrideByteOrder;
    this.intToBytesHelperBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
  }

  @Override
  public void open() throws IOException
  {
    offsetsOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    currentChunk = new int[valuesPerChunk];
    this.chunkMetrics = new IntFormMetrics(optimizationTarget, codecs.length == 1);
  }

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

  @Override
  public long getSerializedSize() throws IOException
  {
    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeFinalOffset();

    return HEADER_BYTES + offsetsOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(
      final WritableByteChannel channel,
      final FileSmoosher smoosher
  ) throws IOException
  {
    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeFinalOffset();

    byte decodeStrategy = 0;
    if (numChunksWithRandomAccess > numChunks / 3) {   // 1/3 random accessible?
      System.out.println(String.format(
          "Using random access optimized strategy, %d:%d have random access",
          numChunksWithRandomAccess,
          numChunks
      ));
    } else {
      decodeStrategy = 1;
      System.out.println(String.format(
          "Using block optimized strategy, %d:%d have random access",
          numChunksWithRandomAccess,
          numChunks
      ));
    }

    channel.write(ByteBuffer.wrap(new byte[]{ShapeShiftingColumnarInts.VERSION}));
    channel.write(toBytes(numChunks));
    channel.write(toBytes(numValues));
    channel.write(ByteBuffer.wrap(new byte[]{
        logValuesPerChunk,
        decodeStrategy
    }));
    channel.write(toBytes(Ints.checkedCast(offsetsOut.size())));

    offsetsOut.writeTo(channel);
    valuesOut.writeTo(channel);

    for (Map.Entry<String, Integer> item : usage.entrySet()) {
      System.out.println(item.getKey() + ": " + item.getValue());
    }
  }

  private void flushCurrentChunk() throws IOException
  {
    Preconditions.checkState(!wroteFinalOffset, "!wroteFinalOffset");
    Preconditions.checkState(currentChunkPos > 0, "currentChunkPos > 0");
    Preconditions.checkState(offsetsOut.isOpen(), "offsetsOut.isOpen");
    Preconditions.checkState(valuesOut.isOpen(), "valuesOut.isOpen");

    offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));

    int bestSize = Integer.MAX_VALUE;
    IntFormEncoder bestCodec = null;
    for (IntFormEncoder codec : codecs) {
      // todo: configurable to not only prefer smallest, maybe thresholds or uh.. something... if i knew it would be done already.
      int theSize = codec.getEncodedSize(currentChunk, currentChunkPos, chunkMetrics);
      if (theSize < bestSize) {
        double modified = theSize * codec.getSpeedModifier(chunkMetrics);
        if (modified < bestSize) {
          bestCodec = codec;
          bestSize = (int) modified;
        }
      }
    }

    if (codecs.length > 1) {
      if (!usage.containsKey(bestCodec.getName())) {
        usage.put(bestCodec.getName(), 0);
      }
      usage.computeIfPresent(bestCodec.getName(), (k, v) -> v + 1);
    }

    if (bestCodec.hasRandomAccessSupport()) {
      numChunksWithRandomAccess++;
    }

    valuesOut.write(new byte[]{bestCodec.getHeader()});
    bestCodec.encode(valuesOut, currentChunk, currentChunkPos, chunkMetrics);

    chunkMetrics = new IntFormMetrics(optimizationTarget, codecs.length == 1);
    numChunks++;
    currentChunkPos = 0;
  }

  private void writeFinalOffset() throws IOException
  {
    if (!wroteFinalOffset) {
      offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));
      wroteFinalOffset = true;
    }
  }

  private ByteBuffer toBytes(final int n)
  {
    intToBytesHelperBuffer.putInt(0, n);
    intToBytesHelperBuffer.rewind();
    return intToBytesHelperBuffer;
  }

  public class IntFormMetrics
  {
    private int minValue = Integer.MAX_VALUE;
    private int maxValue = Integer.MIN_VALUE;
    private int numRunValues = 0;
    private int numDistinctRuns = 0;
    private int longestRun;
    private int currentRun;
    private int previousValue;
    private boolean isOnlyEncoder;
    private int numValues = 0;
    private boolean isFirstValue = true;
    private IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;

    public IntFormMetrics(IndexSpec.ShapeShiftOptimizationTarget target, boolean isOnlyEncoder)
    {
      this.isOnlyEncoder = isOnlyEncoder;
      this.optimizationTarget = target;
    }

    public void processNextRow(int val)
    {
      if (isFirstValue) {
        isFirstValue = false;
        previousValue = val;
        currentRun = 1;
        longestRun = 1;
      } else {
        if (val == previousValue) {
          currentRun++;
          if (currentRun > 2) {
            numRunValues++;
          }
        } else {
          previousValue = val;
          if (currentRun > 2) {
            numDistinctRuns++;
          }
          currentRun = 1;
        }
      }

      if (currentRun > longestRun) {
        longestRun = currentRun;
      }
      if (val < minValue) {
        minValue = val;
      }
      if (val > maxValue) {
        maxValue = val;
      }
      numValues++;
    }

    public int getNumValues()
    {
      return numValues;
    }

    public int getMinValue()
    {
      return minValue;
    }

    public int getMaxValue()
    {
      return maxValue;
    }

    public int getNumRunValues()
    {
      return numRunValues;
    }

    public int getNumDistinctRuns()
    {
      return numDistinctRuns;
    }

    public int getLongestRun()
    {
      return longestRun;
    }

    public IndexSpec.ShapeShiftOptimizationTarget getOptimizationTarget()
    {
      return optimizationTarget;
    }

    public boolean isConstant()
    {
      return minValue == maxValue;
    }

    public boolean isZero()
    {
      return minValue == 0 && minValue == maxValue;
    }

    public boolean isSingleEncoder()
    {
      return isOnlyEncoder;
    }
  }
}
