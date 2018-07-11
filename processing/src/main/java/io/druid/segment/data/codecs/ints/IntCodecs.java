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

package io.druid.segment.data.codecs.ints;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.data.ShapeShiftingColumnarInts;
import io.druid.segment.data.codecs.CompressedFormDecoder;
import io.druid.segment.data.codecs.FormDecoder;

import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

public class IntCodecs
{
  public static final byte ZERO = 0x00;
  public static final byte CONSTANT = 0x01;
  public static final byte UNENCODED = 0x02;
  public static final byte BYTEPACK = 0x03;
  public static final byte RLE_BYTEPACK = 0x04;
  public static final byte COMPRESSED = 0x05;
  public static final byte FASTPFOR = 0x06;


  public static Map<Byte, FormDecoder<ShapeShiftingColumnarInts>> getDecoders(
      List<Byte> composition,
      byte logValuesPerChunk,
      ByteOrder byteOrder
  )
  {
    ImmutableMap.Builder<Byte, FormDecoder<ShapeShiftingColumnarInts>> decodersBuilder = ImmutableMap.<Byte, FormDecoder<ShapeShiftingColumnarInts>>builder();
    for (Byte b : composition) {
      decodersBuilder.put(b, getDecoder(b, logValuesPerChunk, byteOrder));
    }
    return decodersBuilder.build();
  }

  public static FormDecoder<ShapeShiftingColumnarInts> getDecoder(
      byte header,
      byte logValuesPerChunk,
      ByteOrder byteOrder
  )
  {
    switch (header) {
      case ZERO:
        return new ZeroIntFormDecoder(logValuesPerChunk, byteOrder);
      case CONSTANT:
        return new ConstantIntFormDecoder(logValuesPerChunk, byteOrder);
      case UNENCODED:
        return new UnencodedIntFormDecoder(logValuesPerChunk, byteOrder);
      case BYTEPACK:
        return new BytePackedIntFormDecoder(logValuesPerChunk, byteOrder);
      case RLE_BYTEPACK:
        return new RunLengthBytePackedIntFormDecoder(logValuesPerChunk, byteOrder);
      case FASTPFOR:
        return new LemireIntFormDecoder(logValuesPerChunk, IntCodecs.FASTPFOR, byteOrder);
      case COMPRESSED:
        return new CompressedFormDecoder<>(logValuesPerChunk, byteOrder, IntCodecs.COMPRESSED);
    }

    throw new RuntimeException(StringUtils.format("Unknown decoder[%d]", (int) header));
  }
}
