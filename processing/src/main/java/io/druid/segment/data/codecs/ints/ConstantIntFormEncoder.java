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

import io.druid.segment.data.ShapeShiftingColumnarIntsSerializer.IntFormMetrics;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteOrder;

public class ConstantIntFormEncoder extends IntFormEncoder
{
  public ConstantIntFormEncoder(final byte logValuesPerChunk, final ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    if (metrics.isConstant()) {
      return 1 + Integer.BYTES;
    }
    return Integer.MAX_VALUE;
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    valuesOut.write(toBytes(metrics.getMaxValue()));
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.CONSTANT;
  }

  @Override
  public String getName()
  {
    return "constant";
  }

  @Override
  public boolean hasRandomAccessSupport()
  {
    return true;
  }

}
