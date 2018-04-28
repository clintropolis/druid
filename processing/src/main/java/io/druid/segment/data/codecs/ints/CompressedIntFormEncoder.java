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

import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ShapeShiftingColumnarIntsSerializer.IntFormMetrics;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedIntFormEncoder extends IntFormEncoder
{
  private final CompressibleIntFormEncoder intEncoder;
  private final CompressionStrategy compressionStrategy;
  private final CompressionStrategy.Compressor compressor;
  private final ByteBuffer uncompressedDataBuffer;
  private final ByteBuffer compressedDataBuffer;

  public CompressedIntFormEncoder(
      byte logValuesPerChunk,
      ByteOrder byteOrder,
      CompressionStrategy strategy,
      CompressibleIntFormEncoder encoder,
      ByteBuffer compressedDataBuffer,
      ByteBuffer uncompressedDataBuffer
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.intEncoder = encoder;
    this.compressionStrategy = strategy;
    this.compressor = compressionStrategy.getCompressor();
    this.compressedDataBuffer = compressedDataBuffer;
    this.uncompressedDataBuffer = uncompressedDataBuffer;
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    if (!intEncoder.shouldAttemptCompression(metrics)) {
      return Integer.MAX_VALUE;
    }

    uncompressedDataBuffer.clear();
    uncompressedDataBuffer.put(intEncoder.getHeader());
    compressedDataBuffer.clear();
    intEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
    ByteBuffer buff = compressor.compress(uncompressedDataBuffer, compressedDataBuffer);
    return 1 + buff.remaining();
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    uncompressedDataBuffer.clear();
    uncompressedDataBuffer.put(intEncoder.getHeader());
    compressedDataBuffer.clear();
    intEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
    valuesOut.write(new byte[]{compressionStrategy.getId()});
    valuesOut.write(compressor.compress(uncompressedDataBuffer, compressedDataBuffer));
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.COMPRESSED;
  }

  @Override
  public String getName()
  {
    return compressionStrategy.toString() + "-" + intEncoder.getName();
  }

  @Override
  public boolean hasRandomAccessSupport()
  {
    return intEncoder.hasRandomAccessSupport();
  }

  @Override
  public double getSpeedModifier(IntFormMetrics metrics)
  {
    switch (metrics.getOptimizationTarget()) {
      case FASTER:
        return intEncoder.getSpeedModifier(metrics) + 0.30;
      case SMALLER:
        return intEncoder.getSpeedModifier(metrics);
      case FASTBUTSMALLISH:
      default:
        return intEncoder.getSpeedModifier(metrics) + 0.05;
    }
  }
}
