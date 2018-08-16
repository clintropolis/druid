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

package io.druid.segment.data.codecs.ints;

import io.druid.java.util.common.IAE;
import io.druid.segment.IndexSpec;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Simple run-length encoding implementation which uses a bytepacking strategy similar to
 * {@link BytePackedIntFormEncoder}, where the maximum run length and maximum row value are analzyed to choose a
 * number of bytes which both the row values and run counts can be encoded, using the high bit to indicate if the
 * bytes represent a run or a single value. A run is encoded with 2 values sized with the chosen number of bytes,
 * the first with the high bit set and the run length encoded, the 2nd with the value that is repeated. A single
 * value is packed into numBytes with the high bit not set.
 *
 * layout:
 * | header: IntCodecs.RLE_BYTEPACK (byte) | numBytes (byte) | encoded values ((2 * numDistinctRuns * numBytes) + (numSingleValues * numBytes)) |
 */
public class RunLengthBytePackedIntFormEncoder extends CompressibleIntFormEncoder
{
  public RunLengthBytePackedIntFormEncoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  private static int applyRunMask(int runLength, int numBytes)
  {
    switch (numBytes) {
      case 1:
        return runLength | RunLengthBytePackedIntFormDecoder.runMask1;
      case 2:
        return runLength | RunLengthBytePackedIntFormDecoder.runMask2;
      case 3:
        return runLength | RunLengthBytePackedIntFormDecoder.runMask3;
      default:
        return runLength | RunLengthBytePackedIntFormDecoder.runMask4;
    }
  }

  private static byte getNumBytesForMax(int maxValue, int maxRun)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }
    int toConsider = maxValue > maxRun ? maxValue : maxRun;
    if (toConsider <= RunLengthBytePackedIntFormDecoder.mask1) {
      return 1;
    } else if (toConsider <= RunLengthBytePackedIntFormDecoder.mask2) {
      return 2;
    } else if (toConsider <= RunLengthBytePackedIntFormDecoder.mask3) {
      return 3;
    }
    return 4;
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    return computeSize(metrics);
  }

  /**
   * Run length speed modifier is dependent on how effective run length encoding is on the data itself if the
   * optimization strategy is not {@link io.druid.segment.IndexSpec.ShapeShiftOptimizationTarget#SMALLER}, since
   * decode performance is not great if run count is small, but approaching 1.0 as the values become constant.
   *
   * @param metrics
   *
   * @return
   */
  @Override
  public double getSpeedModifier(IntFormMetrics metrics)
  {
    // rle is pretty slow when not in a situation where it is appropriate, penalize if big gains are not projected
    final byte numBytesBytepack = BytePackedIntFormEncoder.getNumBytesForMax(metrics.getMaxValue());
    final int bytepackSize = numBytesBytepack * metrics.getNumValues();
    final int size = computeSize(metrics);
    // don't bother if not smaller than bytepacking
    if (size >= bytepackSize) {
      return 10.0;
    }
    double modifier;
    switch (metrics.getOptimizationTarget()) {
      case SMALLER:
        modifier = 1.0;
        break;
      default:
        modifier = (((double) bytepackSize - (double) size)) / (double) bytepackSize;
        break;
    }
    return Math.max(2.0 - modifier, 1.0);
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    final byte numBytes = getNumBytesForMax(metrics.getMaxValue(), metrics.getLongestRun());
    valuesOut.write(new byte[]{numBytes});

    final WriteOutFunction writer = (value) -> writeOutValue(valuesOut, numBytes, value);

    encodeValues(writer, values, numValues, numBytes);

    // pad if odd length
    if (numBytes == 3) {
      valuesOut.write(new byte[]{0});
    }
  }

  @Override
  public void encodeToBuffer(
      ByteBuffer buffer,
      int[] values,
      int numValues,
      IntFormMetrics metadata
  ) throws IOException
  {
    final byte numBytes =
        RunLengthBytePackedIntFormEncoder.getNumBytesForMax(metadata.getMaxValue(), metadata.getLongestRun());

    final WriteOutFunction writer = (value) -> writeOutValue(buffer, numBytes, value);

    encodeValues(writer, values, numValues, numBytes);

    // pad if odd length
    if (numBytes == 3) {
      buffer.put((byte) 0);
    }
    buffer.flip();
  }

  @Override
  public void encodeCompressionMetadata(
      WriteOutBytes valuesOut, int[] values, int numValues, IntFormMetrics metrics
  ) throws IOException
  {
    final byte numBytes = getNumBytesForMax(metrics.getMaxValue(), metrics.getLongestRun());
    valuesOut.write(new byte[]{numBytes});
  }

  @Override
  public int getMetadataSize()
  {
    return 1;
  }

  @Override
  public boolean shouldAttemptCompression(IntFormMetrics hints)
  {
    if (!hints.isEnableEncoderOptOut()) {
      return true;
    }

    // if not very many runs, cheese it out of here since i am expensive-ish
    // todo: this is totally scientific. 100%. If we don't have at least 3/4 runs, then bail on trying compression since expensive
    if ((hints.getOptimizationTarget() != IndexSpec.ShapeShiftOptimizationTarget.SMALLER) &&
        (hints.getNumRunValues() < (3 * (hints.getNumValues() / 4)))) {
      return false;
    }

    return true;
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.RLE_BYTEPACK;
  }

  @Override
  public String getName()
  {
    return "rle-bytepack";
  }

  private void encodeValues(WriteOutFunction writer, int[] values, int numValues, int numBytes) throws IOException
  {
    int runCounter = 1;

    for (int current = 1; current < numValues; current++) {
      final int prev = current - 1;
      final int next = current + 1;
      // if previous value equals current value, we are in a run, continue accumulating
      if (values[prev] == values[current]) {
        runCounter++;
        if (next < numValues) {
          continue;
        }
      }
      // if we get here we are either previously encountered a single value,
      // or we are at the end of a run and the current value is the start of a new run or a single value,
      // so write out the previous value
      if (runCounter > 1) {
        if (runCounter > 2) {
          // if a run, encode with 2 values, the first masked to indicate that it is a run length, followed by the value itself
          int maskedCounter = RunLengthBytePackedIntFormEncoder.applyRunMask(runCounter, numBytes);
          writer.write(maskedCounter);
          writer.write(values[prev]);
          runCounter = 1;
        } else {
          // a run of 2 is lame, and no smaller than encoding directly
          writer.write(values[prev]);
          writer.write(values[prev]);
          runCounter = 1;
        }
      } else {
        // non runs are written directly
        writer.write(values[prev]);
      }
      // write out the last value if not part of a run
      if (next == numValues && values[current] != values[prev]) {
        writer.write(values[current]);
      }
    }
  }

  private int computeSize(IntFormMetrics metadata)
  {
    final byte numBytes = getNumBytesForMax(metadata.getMaxValue(), metadata.getLongestRun());
    int projectedSize = numBytes * metadata.getNumValues();
    projectedSize -= (numBytes * metadata.getNumRunValues());
    projectedSize += (2 * numBytes * metadata.getNumDistinctRuns());

    return projectedSize;
  }
}
