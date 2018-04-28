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

package org.apache.druid.processing.codecs.FastPFor;

import org.apache.druid.java.util.common.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// todo: not all codecs can encode the entire integer value space, need to map out a value range for each to validate
/**
 * Codecs defined in 'lib/FastPFor/include/codecfactory.h'
 */
public enum NativeFastPForCodecs
{
  FAST_BINARY_PACKING_8("fastbinarypacking8"),
  FAST_BINARY_PACKING_16("fastbinarypacking16"),
  FAST_BINARY_PACKING_32("fastbinarypacking32"),
  BP_32("BP32"),
  VS_ENCODING("vsencoding"),
  FASTPFOR_128("fastpfor128"),
  FASTPFOR_256("fastpfor256"),
  SIMD_FASTPFOR_128("simdfastpfor128"),
  SIMD_FASTPFOR_256("simdfastpfor256"),
  SIMPLE_PFOR("simplepfor"),
  SIMD_SIMPLE_PFOR("simdsimplepfor"),
  PFOR("pfor"),
  SIMD_PFOR("simdpfor"),
  PFOR_2008("pfor2008"),
  SIMD_NEW_PFOR("simdnewpfor"),
  NEW_PFOR("newpfor"),
  OPT_PFOR("optpfor"),
  SIMD_OPT_PFOR("simdoptpfor"),
  VARINT("varint"),
  VBYTE("vbyte"),
  MASKED_VBYTE("maskedvbyte"),
  STREAM_VBYTE("streamvbyte"),
  VARINT_GB("varintgb"),
  VARINT_G8IU("varintg8iu"),
//  SNAPPY("snappy"),
  SIMPLE_16("simple16"),
  SIMPLE_9("simple9"),
  SIMPLE_9_RLE("simple9_rle"),
  SIMPLE_8B("simple8b"),
  SIMPLE_8B_RLE("simple8b_rle"),
  SIMD_BINARY_PACKING("simdbinarypacking"),
  SIMD_GROUP_SIMPLE("simdgroupsimple"),
  SIMD_GROUP_SIMPLE_RINGBUF("simdgroupsimple_ringbuf");

  String codecName;

  NativeFastPForCodecs(String name)
  {
    this.codecName = name;
  }

  public String getCodecName()
  {
    return codecName;
  }

  @Override
  public String toString()
  {
    return StringUtils.toLowerCase(this.name());
  }

  public static NativeFastPForCodecs fromString(String name)
  {
    return valueOf(StringUtils.toUpperCase(name));
  }


  public static List<String> getNames()
  {
    return Arrays.stream(values()).map(v -> v.toString()).collect(Collectors.toList());
  }
}
