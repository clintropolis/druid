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

import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.ISE;
import io.druid.segment.CompressedPools;
import io.druid.segment.writeout.WriteOutBytes;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableIntegerCODEC;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Integer form encoder using {@link <a href="https://github.com/lemire/JavaFastPFOR">JavaFastPFOR</a>}. Any
 * {@link SkippableIntegerCODEC} should work, but currently only {@link me.lemire.integercompression.FastPFOR} is
 * setup as a known encoding in {@link IntCodecs} as {@link IntCodecs#FASTPFOR}.
 *
 * layout:
 * | header (byte) | encoded values  (numOutputInts * Integer.BYTES) |
 */
public final class LemireIntFormEncoder extends BaseIntFormEncoder
{
  // Straight from the horse's mouth (https://github.com/lemire/JavaFastPFOR/blob/master/example.java).
  private static final int SHOULD_BE_ENOUGH = 1024;
  private final NonBlockingPool<SkippableIntegerCODEC> codecPool;
  private final int[] encodedValuesTmp;
  private final byte header;
  private final String name;
  private int numOutputInts;

  public LemireIntFormEncoder(
      byte logValuesPerChunk,
      byte header,
      String name,
      ByteOrder byteOrder
  )
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
    this.name = name;
    this.codecPool = CompressedPools.getShapeshiftLemirePool(header, logValuesPerChunk);
    // todo: pass this in so we can share?
    this.encodedValuesTmp = new int[valuesPerChunk + SHOULD_BE_ENOUGH];
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    metrics.setTmpEncodedValuesHolder(header);
    numOutputInts = doEncode(values, numValues);
    return numOutputInts * Integer.BYTES;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    if (metrics.getTmpEncodedValuesHolder() != header) {
      numOutputInts = doEncode(values, numValues);
    }

    for (int i = 0; i < numOutputInts; i++) {
      valuesOut.write(toBytes(encodedValuesTmp[i]));
    }
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
    final IntWrapper inPos = new IntWrapper(0);
    final IntWrapper outPos = new IntWrapper(0);

    try (ResourceHolder<SkippableIntegerCODEC> hodl = codecPool.take()) {
      SkippableIntegerCODEC codec = hodl.get();
      codec.headlessCompress(values, inPos, numValues, encodedValuesTmp, outPos);
    }

    if (inPos.get() != numValues) {
      throw new ISE("Expected to compress[%d] ints, but read[%d]", numValues, inPos.get());
    }

    return outPos.get();
  }
}
