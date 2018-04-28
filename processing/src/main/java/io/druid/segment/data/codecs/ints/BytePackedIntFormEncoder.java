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

public class BytePackedIntFormEncoder extends CompressibleIntFormEncoder
{
  public BytePackedIntFormEncoder(final byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  // todo: oh hey, it's me.. ur copy pasta
  public static byte getNumBytesForMax(int maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }
    if (maxValue <= 0xFF) {
      return 1;
    } else if (maxValue <= 0xFFFF) {
      return 2;
    } else if (maxValue <= 0xFFFFFF) {
      return 3;
    }
    return 4;
  }

  static int getPaddingBytes(int numBytes)
  {
    return numBytes == 3 ? 1 : 0;
  }

  @Override
  public int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  )
  {
    final int numBytes = getNumBytesForMax(metrics.getMaxValue());
    return (numValues * numBytes) + getPaddingBytes(numBytes);
  }

  @Override
  public void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException
  {
    final byte numBytes = getNumBytesForMax(metrics.getMaxValue());
    valuesOut.write(new byte[]{numBytes});
    WriteOutFunction writer = (value) -> writeOutValue(valuesOut, numBytes, value);
    encodeValues(writer, values, numValues);
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
    final byte numBytes = BytePackedIntFormEncoder.getNumBytesForMax(metadata.getMaxValue());
    buffer.put(numBytes);

    WriteOutFunction writer = (value) -> writeOutValue(buffer, numBytes, value);
    encodeValues(writer, values, numValues);
    if (numBytes == 3) {
      buffer.put((byte) 0);
    }
    buffer.flip();
  }

  private void encodeValues(
      WriteOutFunction writer,
      int[] values,
      int numValues
  ) throws IOException
  {
    for (int i = 0; i < numValues; i++) {
      writer.write(values[i]);
    }
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.BYTEPACK;
  }

  @Override
  public String getName()
  {
    return "bytepack";
  }

  @Override
  public boolean hasRandomAccessSupport()
  {
    return true;
  }

}
