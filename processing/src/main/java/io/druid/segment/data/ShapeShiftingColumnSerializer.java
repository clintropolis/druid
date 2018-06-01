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
import io.druid.java.util.common.logger.Logger;
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
import java.util.function.Function;

/**
 * Base serializer for {@link ShapeShiftingColumn} implementations, providing most common functionality such as headers,
 * value-chunking, encoder selection, and writing out values.
 *
 * Encoding Selection:
 * The intention of this base structure is that implementors of this class will analyze incoming values and aggregate
 * facts about the data which matching {@link FormEncoder} implementations might find interesting, while storing raw,
 * unencoded values in {@link ShapeShiftingColumnSerializer#currentChunk}. When the threshold of
 * {@link ShapeShiftingColumnSerializer#valuesPerChunk} is reached, {@link ShapeShiftingColumnSerializer} will attempt
 * to find the "best" encoding by first computing the encoded size with
 * {@link FormEncoder#getEncodedSize} and then applying a modifier to scale this value in order to influence behavior
 * when sizes are relatively close according to the chosen {@link IndexSpec.ShapeShiftOptimizationTarget}. This
 * effectively sways the decision towards using encodings with faster decoding speed or smaller encoded size as
 * appropriate. Note that very often the best encoding is unambiguous and these settings don't matter, the nuanced
 * differences of behavior of {@link IndexSpec.ShapeShiftOptimizationTarget} mainly come into play when things are
 * close.
 *
 * Implementors need only supply an initialize method to allocate storage for {@code <TChunk>}, an add value method to
 * populate {@code <TChunk>}, a reset method to prepare {@code <TChunkMetrics>} for the next chunk after a flush, and
 * matching {@link FormEncoder} to perform actual value encoding.
 *
 * layout:
 * | version (byte) | numChunks (int) | numValues (int) | logValuesPerChunk (byte) | decodeStrategy (byte) | offsetsOutSize (int) | offsets | values |
 *
 * @param <TChunk>
 * @param <TChunkMetrics>
 */
public abstract class ShapeShiftingColumnSerializer<TChunk, TChunkMetrics extends FormMetrics> implements Serializer
{
  /**
   * | version (byte) | numChunks (int) | numValues (int) | logValuesPerChunk (byte) | decodeStrategy (byte) | offsetsOutSize (int) |
   */
  static final int HEADER_BYTES = 1 + (2 * Integer.BYTES) + 1 + 1 + Integer.BYTES;

  private static Logger log = new Logger(ShapeShiftingColumnSerializer.class);

  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final FormEncoder<TChunk, TChunkMetrics>[] codecs;
  protected final byte version;
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
      final int logBytesPerValue,
      final byte version,
      @Nullable final ByteOrder overrideByteOrder,
      @Nullable final Byte overrideLogValuesPerChunk
  )
  {
    Preconditions.checkArgument(codecs.length > 0, "must have at least one encoder");
    this.segmentWriteOutMedium = Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    this.version = version;
    this.logValuesPerChunk = overrideLogValuesPerChunk != null
                             ? overrideLogValuesPerChunk
                             : (byte) (aggroLevel.getLogBlockSize() - logBytesPerValue);
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

  /**
   * Initialize/allocate {@link ShapeShiftingColumnSerializer#currentChunk} to hold unencoded chunk values until
   * {@link ShapeShiftingColumnSerializer#flushCurrentChunk()} is performed.
   */
  public abstract void initializeChunk();

  /**
   * Reset {@link ShapeShiftingColumnSerializer#chunkMetrics} to prepare for analyzing the next incoming chunk of data
   * after performing {@link ShapeShiftingColumnSerializer#flushCurrentChunk()}
   */
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
    if (preferRandomAccess < numChunks / 10) { //todo: legit? if less than 1/n are randomly accessible, block optimize?
      decodeStrategy = DecodeStrategy.BLOCK;
      log.info(
          "Using block optimized strategy, %d:%d have random access, %d prefer random access",
          numChunksWithRandomAccess,
          numChunks,
          preferRandomAccess
      );
      //todo: buffer/unsafe optimized version?
    } else {
      log.info(
          "Using mixed access strategy, %d:%d have random access, %d prefer random access",
          numChunksWithRandomAccess,
          numChunks,
          preferRandomAccess
      );
    }


    writeShapeShiftHeader(
        channel,
        intToBytesHelperBuffer,
        version,
        numChunks,
        numValues,
        logValuesPerChunk,
        decodeStrategy.byteValue,
        Ints.checkedCast(offsetsOut.size())
    );
    offsetsOut.writeTo(channel);
    valuesOut.writeTo(channel);

    for (Map.Entry<String, Integer> item : usage.entrySet()) {
      log.info(item.getKey() + ": " + item.getValue());
    }
  }

  static void writeShapeShiftHeader(
      WritableByteChannel channel,
      ByteBuffer tmpBuffer,
      byte version,
      int numChunks,
      int numValues,
      byte logValuesPerChunk,
      byte decodeStrategy,
      int offsetsSize
  ) throws IOException
  {
    Function<Integer, ByteBuffer> toBytes = (n) -> {
      tmpBuffer.putInt(0, n);
      tmpBuffer.rewind();
      return tmpBuffer;
    };
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(toBytes.apply(numChunks));
    channel.write(toBytes.apply(numValues));
    channel.write(ByteBuffer.wrap(new byte[]{
        logValuesPerChunk,
        decodeStrategy
    }));
    channel.write(toBytes.apply(offsetsSize));
  }

  /**
   * Encode values of {@link ShapeShiftingColumnSerializer#currentChunk} with the 'best' available {@link FormEncoder}
   * given the information collected in {@link ShapeShiftingColumnSerializer#chunkMetrics}. The best is chosen by
   * computing the smallest 'modified' size, where {@link FormEncoder#getSpeedModifier(FormMetrics)}} is tuned based
   * on decoding speed for each encoding in relation to all other available encodings.
   *
   * @throws IOException
   */
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
          double modified = (double) theSize * codec.getSpeedModifier(chunkMetrics);
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
    resetChunk();
  }

  private void resetChunk()
  {
    currentChunkPos = 0;
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

  /**
   * Metadata about the composition of types of value chunks, to enable decoder optimizations
   */
  public enum DecodeStrategy
  {
    /**
     * Default decoding strategy
     */
    MIXED((byte) 0),
    /**
     * Prefer eagerly decoding all chunks into arrays of primite values
     */
    BLOCK((byte) 1),
    /**
     * Prefer directly decoding values from buffer
     */
    DIRECT((byte) 2);

    final byte byteValue;

    static final Map<Byte, DecodeStrategy> byteMap = Maps.newHashMap();

    static {
      for (DecodeStrategy strategy : DecodeStrategy.values()) {
        byteMap.put(strategy.byteValue, strategy);
      }
    }
    public static DecodeStrategy forByteValue(byte byteValue)
    {
      return byteMap.get(byteValue);
    }

    DecodeStrategy(byte value)
    {
      this.byteValue = value;
    }
  }
}
