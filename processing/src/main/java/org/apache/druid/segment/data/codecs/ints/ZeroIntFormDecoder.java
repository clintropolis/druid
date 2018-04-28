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

import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.BaseFormDecoder;

import java.nio.ByteOrder;

/**
 * Decoder used if all values are the same within a chunk are zero.
 *
 * layout:
 * | header: IntCodecs.ZERO (byte) |
 */
public final class ZeroIntFormDecoder extends BaseFormDecoder<ShapeShiftingColumnarInts>
{
  public ZeroIntFormDecoder(byte logValuesPerChunk, ByteOrder byteOrder)
  {
    super(logValuesPerChunk, byteOrder);
  }

  /**
   * Fill shapeshifting int column chunk values array with zeros
   *
   * @param columnarInts
   */
  @Override
  public void transform(ShapeShiftingColumnarInts columnarInts)
  {
    columnarInts.setCurrentBytesPerValue(0);
    columnarInts.setCurrentConstant(0);
  }

  @Override
  public byte getHeader()
  {
    return IntCodecs.ZERO;
  }
}
