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

package io.druid.segment.data.codecs.ints;

import io.druid.java.util.common.IAE;
import io.druid.segment.data.ShapeShiftingColumnarIntsSerializer.IntFormMetrics;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    final byte numBytesBytepack = BytePackedIntFormEncoder.getNumBytesForMax(metrics.getMaxValue());
    final int bytepackSize = numBytesBytepack * numValues;
    final int projectedSize = projectSize(metrics);
    if (!metrics.isSingleEncoder() && projectedSize > bytepackSize) {
      return Integer.MAX_VALUE;
    }
    return projectedSize;
  }

  @Override
  public double getSpeedModifier(IntFormMetrics metrics)
  {
    // rle is pretty slow when not in a situation where it is appropriate, penalize if big gains are not projected
    final byte numBytesBytepack = BytePackedIntFormEncoder.getNumBytesForMax(metrics.getMaxValue());
    final int bytepackSize = numBytesBytepack * metrics.getNumValues();
    final int projectedSize = projectSize(metrics);
    if (projectedSize > bytepackSize) {
      return 10.0;
    }
    double modifier;
    switch (metrics.getOptimizationTarget()) {
      case SMALLER:
        modifier = 1.0;
        break;
      default:
        modifier = 1.0 - (((double) bytepackSize - (double) projectedSize)) / (double) bytepackSize;
        break;
    }
    return 2.0 - modifier;
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
    buffer.put(numBytes);

    final WriteOutFunction writer = (value) -> writeOutValue(buffer, numBytes, value);

    encodeValues(writer, values, numValues, numBytes);

    // pad if odd length
    if (numBytes == 3) {
      buffer.put((byte) 0);
    }
    buffer.flip();
  }

  @Override
  public boolean shouldAttemptCompression(IntFormMetrics hints)
  {
    if (hints.isSingleEncoder()) {
      return true;
    }
    // if not very many runs, cheese it out of here since i am expensive-ish
    // todo: this is totally scientific. 100%. If we don't have at least 3/4 runs, then bail on trying compression since expensive
    if (hints.getNumRunValues() < (3 * (hints.getNumValues() / 4))) {
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

  private int projectSize(IntFormMetrics metadata)
  {
    final byte numBytes = getNumBytesForMax(metadata.getMaxValue(), metadata.getLongestRun());
    int projectedSize = numBytes * metadata.getNumValues();
    projectedSize -= numBytes * metadata.getNumRunValues();
    projectedSize += 2 * numBytes * metadata.getNumDistinctRuns();

    return projectedSize;
  }
}
