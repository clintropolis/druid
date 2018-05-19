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
import io.druid.segment.data.codecs.FormEncoder;
import io.druid.segment.data.codecs.FormMetrics;
import io.druid.segment.serde.Serializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public abstract class ShapeShiftingColumnSerializer<TChunk, TChunkMetrics extends FormMetrics> implements Serializer
{
  /**
   * | version (byte) | numChunks (int) | numValues (int) | logValuesPerChunk (byte) | decodeStrategy (byte) | offsetsOutSize (int) | offsets | values |
   */
  static final int HEADER_BYTES = 1 + (2 * Integer.BYTES) + 1 + 1 + Integer.BYTES;

  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final FormEncoder<TChunk, TChunkMetrics>[] codecs;
  protected final byte logValuesPerChunk;
  protected final int valuesPerChunk;
  protected final ByteBuffer intToBytesHelperBuffer;
  protected final Map<String, Integer> usage = Maps.newHashMap();
  protected final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;
  protected WriteOutBytes offsetsOut;
  protected WriteOutBytes valuesOut;
  protected boolean wroteFinalOffset = false;
  protected TChunkMetrics chunkMetrics;
  protected TChunk currentChunk;
  protected int currentChunkPos = 0;
  protected int numChunks = 0;
  protected int numChunksWithRandomAccess = 0;
  protected int preferRandomAccess = 0;
  protected int numValues = 0;

  public ShapeShiftingColumnSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final FormEncoder<TChunk, TChunkMetrics>[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftAggressionLevel aggroLevel,
      @Nullable final ByteOrder overrideByteOrder
  )
  {
    Preconditions.checkArgument(codecs.length > 0, "must have at least one encoder");
    this.segmentWriteOutMedium = Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    this.logValuesPerChunk = aggroLevel.getBlockSize();
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.codecs = codecs;
    this.optimizationTarget = optimizationTarget;
    ByteOrder byteOrder = overrideByteOrder == null ? ByteOrder.nativeOrder() : overrideByteOrder;
    this.intToBytesHelperBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
  }

  public void open() throws IOException
  {
    offsetsOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    initializeChunk();
    resetChunkCollector();
  }

  public abstract void initializeChunk();

  public abstract void resetChunkCollector();


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

    DecodeStrategy decodeStrategy = DecodeStrategy.MIXED;
    //CHECKSTYLE.OFF: Regexp
    if (preferRandomAccess < numChunks / 3) { //todo: legit? if less than 1/4 are randomly accessible, block optimize?
      decodeStrategy = DecodeStrategy.BLOCK;
      System.out.println(String.format(
          "Using block optimized strategy, %d:%d have random access, %d prefer random access",
          numChunksWithRandomAccess,
          numChunks,
          preferRandomAccess
      ));
      //todo: buffer/unsafe optimized version?
//    } else if (numChunksWithRandomAccess == numChunks) {
//      decodeStrategy = DecodeStrategy.BUFFER;
//      System.out.println(String.format(
//          "Using random access optimized strategy, %d:%d have random access, %d prefer random access",
//          numChunksWithRandomAccess,
//          numChunks,
//          preferDirectAccess
//      ));
    } else {
      System.out.println(String.format(
          "Using mixed access strategy, %d:%d have random access, %d prefer random access",
          numChunksWithRandomAccess,
          numChunks,
          preferRandomAccess
      ));
    }

    channel.write(ByteBuffer.wrap(new byte[]{ShapeShiftingColumnarInts.VERSION}));
    channel.write(toBytes(numChunks));
    channel.write(toBytes(numValues));
    channel.write(ByteBuffer.wrap(new byte[]{
        logValuesPerChunk,
        decodeStrategy.byteValue
    }));
    channel.write(toBytes(Ints.checkedCast(offsetsOut.size())));

    offsetsOut.writeTo(channel);
    valuesOut.writeTo(channel);

    for (Map.Entry<String, Integer> item : usage.entrySet()) {
      System.out.println(item.getKey() + ": " + item.getValue());
    }
    //CHECKSTYLE.ON: Regexp
  }

  protected void flushCurrentChunk() throws IOException
  {
    Preconditions.checkState(!wroteFinalOffset, "!wroteFinalOffset");
    Preconditions.checkState(currentChunkPos > 0, "currentChunkPos > 0");
    Preconditions.checkState(offsetsOut.isOpen(), "offsetsOut.isOpen");
    Preconditions.checkState(valuesOut.isOpen(), "valuesOut.isOpen");

    offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));

    int bestSize = Integer.MAX_VALUE;
    FormEncoder<TChunk, TChunkMetrics> bestCodec = null;
    if (codecs.length > 1) {
      for (FormEncoder codec : codecs) {
        // todo: configurable to not only prefer smallest, maybe thresholds or uh.. something... if i knew it would be done already.
        int theSize = codec.getEncodedSize(currentChunk, currentChunkPos, chunkMetrics);
        if (theSize < bestSize) {
          double modified = (double)theSize * codec.getSpeedModifier(chunkMetrics);
          if (modified < bestSize) {
            bestCodec = codec;
            bestSize = (int) modified;
          }
        }
      }

      if (!usage.containsKey(bestCodec.getName())) {
        usage.put(bestCodec.getName(), 0);
      }
      usage.computeIfPresent(bestCodec.getName(), (k, v) -> v + 1);
    } else {
      bestCodec = codecs[0];
    }

    if (bestCodec.hasDirectAccessSupport()) {
      numChunksWithRandomAccess++;
      if (bestCodec.preferDirectAccess()) {
        preferRandomAccess++;
      }
    }

    valuesOut.write(new byte[]{bestCodec.getHeader()});
    bestCodec.encode(valuesOut, currentChunk, currentChunkPos, chunkMetrics);

    numChunks++;
    resetChunkCollector();
  }

  protected void writeFinalOffset() throws IOException
  {
    if (!wroteFinalOffset) {
      offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));
      wroteFinalOffset = true;
    }
  }

  protected ByteBuffer toBytes(final int n)
  {
    intToBytesHelperBuffer.putInt(0, n);
    intToBytesHelperBuffer.rewind();
    return intToBytesHelperBuffer;
  }

  public enum DecodeStrategy
  {
    MIXED((byte) 0),
    BLOCK((byte) 1);

    byte byteValue;

    DecodeStrategy(byte value)
    {
      this.byteValue = value;
    }
  }
}
