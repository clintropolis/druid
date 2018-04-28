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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.ning.compress.BufferRecycler;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.data.ShapeShiftingColumnarInts;
import org.apache.druid.segment.data.codecs.ints.IntCodecs;

import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec;
import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

public class CompressedPools
{
  private static final Logger log = new Logger(CompressedPools.class);

  public static final int BUFFER_SIZE = 0x10000;
  private static final int INT_ARRAY_SIZE = 1 << 14;

  // todo: i have no idea what these should legitimately be, this is only ~24.7M which cannot be reclaimed by gc...
  // ...but maybe convservative if there is a lot of load, perhaps this is configurable?
  private static final int INT_DECODED_ARRAY_POOL_MAX_CACHE = 256;

  private static final NonBlockingPool<BufferRecycler> BUFFER_RECYCLER_POOL = new StupidPool<>(
      "bufferRecyclerPool",
      new Supplier<BufferRecycler>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public BufferRecycler get()
        {
          log.debug("Allocating new bufferRecycler[%,d]", counter.incrementAndGet());
          return new BufferRecycler();
        }
      }
  );

  public static ResourceHolder<BufferRecycler> getBufferRecycler()
  {
    return BUFFER_RECYCLER_POOL.take();
  }

  private static final NonBlockingPool<byte[]> OUTPUT_BYTES_POOL = new StupidPool<byte[]>(
      "outputBytesPool",
      new Supplier<byte[]>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public byte[] get()
        {
          log.debug("Allocating new outputBytesPool[%,d]", counter.incrementAndGet());
          return new byte[BUFFER_SIZE];
        }
      }
  );

  public static ResourceHolder<byte[]> getOutputBytes()
  {
    return OUTPUT_BYTES_POOL.take();
  }

  private static NonBlockingPool<ByteBuffer> makeBufferPool(String name, int size, ByteOrder order)
  {
    return new StupidPool<>(
        name,
        new Supplier<ByteBuffer>()
        {
          private final AtomicLong counter = new AtomicLong(0);

          @Override
          public ByteBuffer get()
          {
            log.info("Allocating new %s[%,d]", name, counter.incrementAndGet());
            return ByteBuffer.allocateDirect(size).order(order);
          }
        }
    );
  }

  private static NonBlockingPool<int[]> makeIntArrayPool(String name, int size, int maxCache)
  {
    return new StupidPool<>(
        name,
        new Supplier<int[]>()
        {
          private final AtomicLong counter = new AtomicLong(0);

          @Override
          public int[] get()
          {
            log.info("Allocating new %s[%,d]", name, counter.incrementAndGet());
            return new int[size];
          }
        },
        0,
        maxCache
    );
  }

  private static NonBlockingPool<NativeFastPForCodec> makeNativeFastPForCodecPool(String name, NativeFastPForCodecs codec)
  {
    return new StupidPool<>(
        name,
        new Supplier<NativeFastPForCodec>()
        {
          private final AtomicLong counter = new AtomicLong(0);

          @Override
          public NativeFastPForCodec get()
          {
            log.debug("Allocating new nativeFastPForCodecPool[%,d]", counter.incrementAndGet());
            return new NativeFastPForCodec(codec);
          }
        }
    );
  }

  private static final NonBlockingPool<ByteBuffer> BIG_END_BYTE_BUF_POOL =
      makeBufferPool("bigEndByteBufPool", BUFFER_SIZE, ByteOrder.BIG_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> LITTLE_END_BYTE_BUF_POOL =
      makeBufferPool("littleEndByteBufPool", BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN);


  private static final NonBlockingPool<int[]> SHAPESHIFT_INTS_DECODED_VALUES_ARRAY_POOL =
      makeIntArrayPool(
          "shapeshiftIntsDecodedValuesArrayPool",
          INT_ARRAY_SIZE,
          INT_DECODED_ARRAY_POOL_MAX_CACHE
      );

  public static ResourceHolder<ByteBuffer> getByteBuf(ByteOrder order)
  {
    if (order.equals(ByteOrder.LITTLE_ENDIAN)) {
      return LITTLE_END_BYTE_BUF_POOL.take();
    }
    return BIG_END_BYTE_BUF_POOL.take();
  }

  /**
   * Get pooled decoded values buffer for {@link ShapeShiftingColumnarInts}
   * @param logBytesPerChunk
   * @param order
   * @return
   */
  public static ResourceHolder<ByteBuffer> getShapeshiftDecodedValuesBuffer(int logBytesPerChunk, ByteOrder order)
  {
    return getByteBuf(order);
  }


  /**
   * Get pooled decoded values array for {@link ShapeShiftingColumnarInts}
   * @param logValuesPerChunk
   * @return
   */
  public static ResourceHolder<int[]> getShapeshiftIntsDecodedValuesArray(int logValuesPerChunk)
  {
    return SHAPESHIFT_INTS_DECODED_VALUES_ARRAY_POOL.take();
  }


  private static final NonBlockingPool<NativeFastPForCodec> nativeFastpforPool =
      makeNativeFastPForCodecPool(
          "nativeFastPForCodecPool",
          IntCodecs.getNativeLemireCodecName(IntCodecs.FASTPFOR)
      );


  public static NonBlockingPool<NativeFastPForCodec> getShapeshiftNativeLemirePool(byte header, int logValuesPerChunk)
  {
    switch (header) {
      case IntCodecs.FASTPFOR:
        return nativeFastpforPool;
      default:
        throw new RuntimeException("unknown FastPFor codec");
    }
  }
}
