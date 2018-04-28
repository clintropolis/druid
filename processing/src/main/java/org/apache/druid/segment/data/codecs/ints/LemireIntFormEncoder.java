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
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Integer form encoder using {@link <a href="https://github.com/lemire/FastPFOR">FastPFOR</a>} provided by
 * druid-native-processing module.
 *
 * layout:
 * | header (byte) | encoded values  (numOutputInts * Integer.BYTES) |
 */
public final class LemireIntFormEncoder extends BaseIntFormEncoder
{
  private final byte header;
  private final String name;
  private int numOutputInts;
  private final ByteBuffer valuesBuffer;
  private final ByteBuffer encodedValuesBuffer;

  private final NonBlockingPool<NativeFastPForCodec> codecPool;


  public LemireIntFormEncoder(
      byte logValuesPerChunk,
      byte header,
      String name,
      ByteBuffer valuesBuffer,
      ByteBuffer encodedValuesBuffer,
      ByteOrder byteOrder
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
    this.name = name;
    this.valuesBuffer = valuesBuffer;
    this.encodedValuesBuffer = encodedValuesBuffer;
    this.codecPool = CompressedPools.getShapeshiftNativeLemirePool(header, logValuesPerChunk);

  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    numOutputInts = doEncode(values, numValues);
    metrics.setCompressionBufferHolder(getHeader());
    return numOutputInts * Integer.BYTES + Long.BYTES;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    numOutputInts = doEncode(values, numValues);
//    valuesOut.write(new byte[15]);
    int numWrittenBytes = valuesOut.write(encodedValuesBuffer);
    assert (numWrittenBytes == numOutputInts << 2);
  }

  @Override
  public byte getHeader()
  {
    return header;
  }

  @Override
  public String getName()
  {
    return name;
  }

  private int doEncode(int[] values, final int numValues)
  {
    valuesBuffer.clear();
    for (int i = 0; i < numValues; i++) {
      valuesBuffer.putInt(values[i]);
    }
    try (ResourceHolder<NativeFastPForCodec> nfpfHolder = codecPool.take()) {
      NativeFastPForCodec npf = nfpfHolder.get();
      int size = npf.encode(
          valuesBuffer,
          numValues,
          encodedValuesBuffer,
          (1 << logValuesPerChunk)
      );
      encodedValuesBuffer.position(0);
      encodedValuesBuffer.limit(size * Integer.BYTES);
      return size;
    }
  }
}
