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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class IntFormEncoder
{
  final byte logValuesPerChunk;
  final int valuesPerChunk;
  final ByteOrder byteOrder;
  final ByteBuffer intToBytesHelperBuffer;
  final boolean isBigEndian;

  IntFormEncoder(byte logValuesPerChunk, ByteOrder byteOrder)
  {
    this.logValuesPerChunk = logValuesPerChunk;
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.byteOrder = byteOrder;
    this.isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    intToBytesHelperBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
  }

  public abstract int getEncodedSize(
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException;

  public double getSpeedModifier(IntFormMetrics metrics)
  {
    return 1.0;
  }

  public abstract void encode(
      WriteOutBytes valuesOut,
      int[] values,
      int numValues,
      IntFormMetrics metrics
  ) throws IOException;

  public abstract byte getHeader();

  public abstract String getName();

  protected ByteBuffer toBytes(final int n)
  {
    intToBytesHelperBuffer.putInt(0, n);
    intToBytesHelperBuffer.rewind();
    return intToBytesHelperBuffer;
  }

  void writeOutValue(WriteOutBytes valuesOut, int numBytes, int value) throws IOException
  {
    intToBytesHelperBuffer.putInt(0, value);
    intToBytesHelperBuffer.position(0);
    if (isBigEndian) {
      valuesOut.write(intToBytesHelperBuffer.array(), Integer.BYTES - numBytes, numBytes);
    } else {
      valuesOut.write(intToBytesHelperBuffer.array(), 0, numBytes);
    }
  }

  void writeOutValue(ByteBuffer valuesOut, int numBytes, int value)
  {
    intToBytesHelperBuffer.putInt(0, value);
    intToBytesHelperBuffer.position(0);
    if (isBigEndian) {
      valuesOut.put(intToBytesHelperBuffer.array(), Integer.BYTES - numBytes, numBytes);
    } else {
      valuesOut.put(intToBytesHelperBuffer.array(), 0, numBytes);
    }
  }

  public boolean hasRandomAccessSupport()
  {
    return false;
  }


  @FunctionalInterface
  interface WriteOutFunction
  {
    void write(int value) throws IOException;
  }
}
