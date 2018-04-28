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

package org.apache.druid.segment.data.codecs.ints;

import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.BaseFormDecoder;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Integer form decoder using {@link <a href="https://github.com/lemire/FastPFOR">FastPFOR</a>} provided by
 * druid-native-processing module.
 *
 * layout:
 * | header (byte) | encoded values  (numOutputInts * Integer.BYTES) |
 */
public final class LemireIntFormDecoder extends BaseFormDecoder<ShapeShiftingColumnarInts>
{
  private final NonBlockingPool<NativeFastPForCodec> codecPool;
  private final byte header;


  public LemireIntFormDecoder(
      byte logValuesPerChunk,
      byte header,
      ByteOrder byteOrder
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
    this.codecPool = CompressedPools.getShapeshiftNativeLemirePool(header, logValuesPerChunk);

  }

  @Override
  public void transform(ShapeShiftingColumnarInts columnarInts)
  {
    final int numValues = columnarInts.getCurrentChunkNumValues();
    final int startOffset = columnarInts.getCurrentValuesStartOffset();
    final ByteBuffer buffer = columnarInts.getCurrentValueBuffer();
    final int chunkSizeBytes = columnarInts.getCurrentChunkSize();

    final ByteBuffer decodedValuesBuffer = columnarInts.getDecodedDataBuffer();

    try (ResourceHolder<NativeFastPForCodec> nfpfHolder = codecPool.take()) {
      NativeFastPForCodec nfpf = nfpfHolder.get();
      final int chunkSizeInts = (chunkSizeBytes - this.getMetadataSize()) >> 2;
      decodedValuesBuffer.clear();
      int decodedSize = nfpf.decode(buffer, startOffset, chunkSizeInts, decodedValuesBuffer, numValues);
      assert (numValues == decodedSize);
    }
    decodedValuesBuffer.position(0);
    decodedValuesBuffer.limit(numValues * Integer.BYTES);

    columnarInts.setCurrentValueBuffer(decodedValuesBuffer);
    columnarInts.setCurrentValuesStartOffset(0);
    columnarInts.setCurrentValuesAddress(((DirectBuffer) decodedValuesBuffer).address());
    columnarInts.setCurrentBytesPerValue(Integer.BYTES);
  }

  @Override
  public byte getHeader()
  {
    return header;
  }

//  @Override
//  public int getMetadataSize()
//  {
//    return 15;
//  }
}
