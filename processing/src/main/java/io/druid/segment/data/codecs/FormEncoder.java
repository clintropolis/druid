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

package io.druid.segment.data.codecs;

import io.druid.segment.data.ShapeShiftingColumn;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;

/**
 * Interface describing value encoders for use with {@link io.druid.segment.data.ShapeShiftingColumnSerializer}
 *
 * @param <TChunk>        Type of value chunk, i.e. {@code int[]}, {@code long[]}, etc.
 * @param <TChunkMetrics> Type of {@link FormMetrics} that the encoder cosumes
 */
public interface FormEncoder<TChunk, TChunkMetrics extends FormMetrics>
{
  /**
   * Get size in bytes if the values were encoded with this encoder
   *
   * @param values
   * @param numValues
   * @param metrics
   *
   * @return
   *
   * @throws IOException
   */
  int getEncodedSize(
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException;


  /**
   * Encode the values to the supplied {@link WriteOutBytes}
   *
   * @param valuesOut
   * @param values
   * @param numValues
   * @param metrics
   *
   * @throws IOException
   */
  void encode(
      WriteOutBytes valuesOut,
      TChunk values,
      int numValues,
      TChunkMetrics metrics
  ) throws IOException;

  /**
   * Byte value to write as first byte to indicate the type of encoder used for this chunk. This value must be distinct
   * for all encoding/decoding strategies tied to a specific implementation of
   * {@link io.druid.segment.data.ShapeShiftingColumnSerializer} and {@link ShapeShiftingColumn}
   *
   * @return
   */
  byte getHeader();

  /**
   * Get friendly name of this encoder
   *
   * @return
   */
  String getName();

  /**
   * @param metrics
   *
   * @return
   */
  default double getSpeedModifier(TChunkMetrics metrics)
  {
    return 1.0;
  }

  /**
   * Values decoded with this encoding may be accessed directly by {@link ShapeShiftingColumn}
   * from either {@link ShapeShiftingColumn#buffer} or {@link ShapeShiftingColumn#decompressedDataBuffer}, used by
   * {@link io.druid.segment.data.ShapeShiftingColumnSerializer} to set
   * {@link io.druid.segment.data.ShapeShiftingColumnSerializer.DecodeStrategy}.
   *
   * @return
   */
  default boolean hasDirectAccessSupport()
  {
    return false;
  }

  /**
   * Prefer that decoded values are accessed directly from {@link ShapeShiftingColumn#buffer} or
   * {@link ShapeShiftingColumn#decompressedDataBuffer}, used by
   * {@link io.druid.segment.data.ShapeShiftingColumnSerializer} to set
   * {@link io.druid.segment.data.ShapeShiftingColumnSerializer.DecodeStrategy}.
   *
   * @return
   */
  default boolean preferDirectAccess()
  {
    return false;
  }
}
