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

import io.druid.java.util.common.ISE;
import io.druid.segment.writeout.WriteOutBytes;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableIntegerCODEC;

import java.io.IOException;
import java.nio.ByteOrder;

public final class LemireIntFormEncoder extends BaseIntFormEncoder
{
  // Straight from the horse's mouth (https://github.com/lemire/JavaFastPFOR/blob/master/example.java).
  private static final int SHOULD_BE_ENOUGH = 1024;
  private final SkippableIntegerCODEC codec;
  private final int[] encodedValuesTmp;

  public LemireIntFormEncoder(
      SkippableIntegerCODEC codec,
      byte logValuesPerChunk
  )
  {
    super(logValuesPerChunk, ByteOrder.LITTLE_ENDIAN);
    this.codec = codec;
    this.encodedValuesTmp = new int[valuesPerChunk + SHOULD_BE_ENOUGH];
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    return doEncode(values, numValues) * Integer.BYTES;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    int numOutputInts = doEncode(values, numValues);

    for (int i = 0; i < numOutputInts; i++) {
      valuesOut.write(toBytes(encodedValuesTmp[i]));
    }
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.FASTPFOR;
  }

  @Override
  public String getName()
  {
    return "fastpfor";
  }

  private int doEncode(int[] values, final int numValues)
  {
    final IntWrapper inPos = new IntWrapper(0);
    final IntWrapper outPos = new IntWrapper(0);

    codec.headlessCompress(values, inPos, numValues, encodedValuesTmp, outPos);

    if (inPos.get() != numValues) {
      throw new ISE("Expected to compress[%d] ints, but read[%d]", numValues, inPos.get());
    }

    return outPos.get();
  }
}
