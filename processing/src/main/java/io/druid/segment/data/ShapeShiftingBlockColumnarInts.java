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

package io.druid.segment.data;

import io.druid.segment.data.codecs.FormDecoder;

import java.util.Map;

/**
 * Variant of {@link ShapeShiftingColumnarInts} that is optimized for eagerly decoding all column values, allowing
 * {@link ShapeShiftingColumnarInts#get(int)} to be implemented directly as a masked array access.
 * This optimization will be produced by {@link ShapeShiftingColumnarIntsSupplier} if
 */
public final class ShapeShiftingBlockColumnarInts extends ShapeShiftingColumnarInts
{
  public ShapeShiftingBlockColumnarInts(
      ShapeShiftingColumnData sourceData,
      Map<Byte, FormDecoder<ShapeShiftingColumnarInts>> decoders
  )
  {
    super(sourceData, decoders);
  }

  @Override
  public int get(final int index)
  {
    final int desiredChunk = index >> logValuesPerChunk;

    if (desiredChunk != currentChunk) {
      loadChunk(desiredChunk);
    }

    return decodedValues[index & chunkIndexMask];
  }

  @Override
  public void transform(FormDecoder<ShapeShiftingColumnarInts> nextForm)
  {
    nextForm.transform(this);
  }
}
