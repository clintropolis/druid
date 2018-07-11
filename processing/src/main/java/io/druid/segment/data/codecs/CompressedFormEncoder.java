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

package io.druid.segment.data.codecs;

import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Generic compression encoder that can wrap {@link CompressibleFormEncoder} to provide any of the compression
 * algorithms available in {@link CompressionStrategy}
 *
 * @param <TChunk>
 * @param <TChunkMetrics>
 */
public abstract class CompressedFormEncoder<TChunk, TChunkMetrics extends FormMetrics>
    extends BaseFormEncoder<TChunk, TChunkMetrics>
{
  private final CompressibleFormEncoder<TChunk, TChunkMetrics> formEncoder;
  private final CompressionStrategy compressionStrategy;
  private final CompressionStrategy.Compressor compressor;
  private final ByteBuffer uncompressedDataBuffer;
  private final ByteBuffer compressedDataBuffer;
  private ByteBuffer compressed;

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

    metrics.setCompressionBufferHolder(formEncoder.getHeader());
    uncompressedDataBuffer.clear();
    compressedDataBuffer.clear();
    formEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
    compressed = compressor.compress(uncompressedDataBuffer, compressedDataBuffer);
    // header | compressionId | inner encoding header | inner encoding metadata | compressed values
    return 1 + 1 + 1 + formEncoder.getMetadataSize() + compressed.remaining();
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException
  {
    if (metrics.getCompressionBufferHolder() == formEncoder.getHeader()) {
      valuesOut.write(new byte[]{compressionStrategy.getId(), formEncoder.getHeader()});
      formEncoder.encodeCompressionMetadata(valuesOut, values, numValues, metrics);
      valuesOut.write(compressedDataBuffer);
    } else {
      uncompressedDataBuffer.clear();
      compressedDataBuffer.clear();
      formEncoder.encodeToBuffer(uncompressedDataBuffer, values, numValues, metrics);
      valuesOut.write(new byte[]{compressionStrategy.getId(), formEncoder.getHeader()});
      formEncoder.encodeCompressionMetadata(valuesOut, values, numValues, metrics);
      valuesOut.write(compressor.compress(uncompressedDataBuffer, compressedDataBuffer));
    }
  }

  @Override
  public String getName()
  {
    return compressionStrategy.toString() + "-" + formEncoder.getName();
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

  public FormEncoder<TChunk, TChunkMetrics> getInnerEncoder()
  {
    return formEncoder;
  }
}
