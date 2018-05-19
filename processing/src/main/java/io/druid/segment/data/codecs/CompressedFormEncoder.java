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

package io.druid.segment.data.codecs;

import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.codecs.ints.IntCodecs;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedFormEncoder<TChunk, TChunkMetrics extends FormMetrics>
    extends BaseFormEncoder<TChunk, TChunkMetrics>
{
  private final CompressibleFormEncoder<TChunk, TChunkMetrics> formEncoder;
  private final CompressionStrategy compressionStrategy;
  private final CompressionStrategy.Compressor compressor;
  private final ByteBuffer uncompressedDataBuffer;
  private final ByteBuffer compressedDataBuffer;

  public CompressedFormEncoder(
      byte logValuesPerChunk,
      ByteOrder byteOrder,
      CompressionStrategy strategy,
      CompressibleFormEncoder<TChunk, TChunkMetrics> encoder,
      ByteBuffer compressedDataBuffer,
      ByteBuffer uncompressedDataBuffer
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.formEncoder = encoder;
    this.compressionStrategy = strategy;
    this.compressor = compressionStrategy.getCompressor();
    this.compressedDataBuffer = compressedDataBuffer;
    this.uncompressedDataBuffer = uncompressedDataBuffer;
  }

  @Override
  public int getEncodedSize(
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException
  {
    if (!formEncoder.shouldAttemptCompression(metrics)) {
      return Integer.MAX_VALUE;
    }

    uncompressedDataBuffer.clear();
    uncompressedDataBuffer.put(formEncoder.getHeader());
    compressedDataBuffer.clear();
    formEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
    ByteBuffer buff = compressor.compress(uncompressedDataBuffer, compressedDataBuffer);
    return 1 + buff.remaining();
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException
  {
    uncompressedDataBuffer.clear();
    uncompressedDataBuffer.put(formEncoder.getHeader());
    compressedDataBuffer.clear();
    formEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
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
    return compressionStrategy.toString() + "-" + formEncoder.getName();
  }

  @Override
  public boolean hasDirectAccessSupport()
  {
    return formEncoder.hasDirectAccessSupport();
  }

  @Override
  public boolean preferDirectAccess()
  {
    return formEncoder.preferDirectAccess();
  }

  @Override
  public double getSpeedModifier(TChunkMetrics metrics)
  {
    switch (metrics.getOptimizationTarget()) {
      case FASTER:
        return formEncoder.getSpeedModifier(metrics) + 0.30;
      case SMALLER:
        return formEncoder.getSpeedModifier(metrics);
      case FASTBUTSMALLISH:
      default:
        return formEncoder.getSpeedModifier(metrics) + 0.05;
    }
  }
}
