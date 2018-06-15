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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.ning.compress.BufferRecycler;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.data.codecs.ints.IntCodecs;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CompressedPools
{
  private static final Logger log = new Logger(CompressedPools.class);

  private static final int SMALLEST_BUFFER_SIZE = 0x4000;
  private static final int SMALLER_BUFFER_SIZE = 0x8000;
  public static final int BUFFER_SIZE = 0x10000;
  // Straight from the horse's mouth (https://github.com/lemire/JavaFastPFOR/blob/master/example.java).
  private static final int ENCODED_INTS_SHOULD_BE_ENOUGH = 1024;
  private static final int INT_ARRAY_SIZE = 1 << 14;
  private static final int SMALLER_INT_ARRAY_SIZE = 1 << 13;
  private static final int SMALLEST_INT_ARRAY_SIZE = 1 << 12;

  // todo: i have no idea what this should legitimately be, this is only 32.25MiB which cannot be reclaimed by gc...
  // ...but maybe convservative if there is a lot of load, perhaps this is configurable?
  private static final int INT_ARRAY_POOL_MAX_CACHE = 256;


  // todo: see ^ re: sizing.. these are currently ~1mb on heap + ~200kb direct buffer. Heap could be ~1/4 of the size
  // with minor changes to fastpfor lib to allow passing page size (our max is 2^14 but codec allocates for 2^16)
  // current sizing put it in around 32MiB that cannot be reclaimed
  private static final int LEMIRE_FASTPFOR_CODEC_POOL_MAX_CACHE = 28;

  private static final NonBlockingPool<BufferRecycler> bufferRecyclerPool = new StupidPool<>(
      "bufferRecyclerPool",
      new Supplier<BufferRecycler>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public BufferRecycler get()
        {
          log.info("Allocating new bufferRecycler[%,d]", counter.incrementAndGet());
          return new BufferRecycler();
        }
      }
  );

  public static ResourceHolder<BufferRecycler> getBufferRecycler()
  {
    return bufferRecyclerPool.take();
  }

  private static final NonBlockingPool<byte[]> outputBytesPool = new StupidPool<>(
      "outputBytesPool",
      new Supplier<byte[]>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public byte[] get()
        {
          log.info("Allocating new outputBytesPool[%,d]", counter.incrementAndGet());
          return new byte[BUFFER_SIZE];
        }
      }
  );

  public static ResourceHolder<byte[]> getOutputBytes()
  {
    return outputBytesPool.take();
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

  private static NonBlockingPool<int[]> makeIntArrayPool(String name, int size)
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
        INT_ARRAY_POOL_MAX_CACHE
    );
  }

  private static NonBlockingPool<SkippableIntegerCODEC> makeFastpforPool(String name, int size)
  {
    return new StupidPool<>(
        name,
        new Supplier<SkippableIntegerCODEC>()
        {
          private final AtomicLong counter = new AtomicLong(0);

          @Override
          public SkippableIntegerCODEC get()
          {
            log.info("Allocating new %s[%,d]", name, counter.incrementAndGet());

            Supplier<ByteBuffer> compressionBufferSupplier =
                Suppliers.memoize(() -> ByteBuffer.allocateDirect(1 << 14));
            return new SkippableComposition(
                new FastPFOR(),
                new VariableByte() {
                  // VariableByte allocates a buffer in compress method instead of in constructor like fastpfor
                  // so override to re-use instead (and only allocate if indexing)
                  @Override
                  protected ByteBuffer makeBuffer(int sizeInBytes)
                  {
                    return compressionBufferSupplier.get();
                  }
                }
              );
          }
        },
        0,
        LEMIRE_FASTPFOR_CODEC_POOL_MAX_CACHE
    );
  }

  private static final NonBlockingPool<ByteBuffer> bigEndByteBufPool =
      makeBufferPool("bigEndByteBufPool", BUFFER_SIZE, ByteOrder.BIG_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> littleBigEndByteBufPool =
      makeBufferPool("littleBigEndByteBufPool", SMALLER_BUFFER_SIZE, ByteOrder.BIG_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> littlestBigEndByteBufPool =
      makeBufferPool("littlestBigEndByteBufPool", SMALLEST_BUFFER_SIZE, ByteOrder.BIG_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> littleEndByteBufPool =
      makeBufferPool("littleEndByteBufPool", BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> littlerEndByteBufPool =
      makeBufferPool("littlerEndByteBufPool", SMALLER_BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN);

  private static final NonBlockingPool<ByteBuffer> littlestEndByteBufPool =
      makeBufferPool("littlestEndByteBufPool", SMALLEST_BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN);

  private static final NonBlockingPool<int[]> shapeshiftIntsDecodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftIntsDecodedValuesArrayPool",
          INT_ARRAY_SIZE
      );

  private static final NonBlockingPool<int[]> shapeshiftIntsEncodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftIntsEncodedValuesArrayPool",
          INT_ARRAY_SIZE + ENCODED_INTS_SHOULD_BE_ENOUGH
      );

  private static final NonBlockingPool<int[]> shapeshiftSmallerIntsDecodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftSmallerIntsDecodedValuesArrayPool",
          SMALLER_INT_ARRAY_SIZE
      );

  private static final NonBlockingPool<int[]> shapeshiftSmallerIntsEncodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftSmallerIntsEncodedValuesArrayPool",
          SMALLER_INT_ARRAY_SIZE + ENCODED_INTS_SHOULD_BE_ENOUGH
      );

  private static final NonBlockingPool<int[]> shapeshiftSmallestIntsDecodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftSmallestIntsDecodedValuesArrayPool",
          SMALLEST_INT_ARRAY_SIZE
      );

  private static final NonBlockingPool<int[]> shapeshiftSmallestIntsEncodedValuesArrayPool =
      makeIntArrayPool(
          "shapeshiftSmallestIntsEncodedValuesArrayPool",
          SMALLEST_INT_ARRAY_SIZE + ENCODED_INTS_SHOULD_BE_ENOUGH
      );


  private static final NonBlockingPool<SkippableIntegerCODEC> shapeshiftFastPforPool =
      makeFastpforPool(
          "shapeshiftFastPforCodecPool",
          INT_ARRAY_SIZE
      );


  public static ResourceHolder<ByteBuffer> getByteBuf(ByteOrder order)
  {
    if (order.equals(ByteOrder.LITTLE_ENDIAN)) {
      return littleEndByteBufPool.take();
    }
    return bigEndByteBufPool.take();
  }

  private static ResourceHolder<ByteBuffer> getSmallerByteBuf(ByteOrder order)
  {
    if (order.equals(ByteOrder.LITTLE_ENDIAN)) {
      return littlerEndByteBufPool.take();
    }
    return littleBigEndByteBufPool.take();
  }

  private static ResourceHolder<ByteBuffer> getSmallestByteBuf(ByteOrder order)
  {
    if (order.equals(ByteOrder.LITTLE_ENDIAN)) {
      return littlestEndByteBufPool.take();
    }
    return littlestBigEndByteBufPool.take();
  }

  /**
   * Get pooled decoded values buffer for {@link io.druid.segment.data.ShapeShiftingColumnarInts}
   * @param logBytesPerChunk
   * @param order
   * @return
   */
  public static ResourceHolder<ByteBuffer> getShapeshiftDecodedValuesBuffer(int logBytesPerChunk, ByteOrder order)
  {
    switch (logBytesPerChunk) {
      case 14:
        return getSmallestByteBuf(order);
      case 15:
        return getSmallerByteBuf(order);
      case 16:
      default:
        return getByteBuf(order);
    }
  }


  /**
   * Get pooled decoded values array for {@link io.druid.segment.data.ShapeShiftingColumnarInts}
   * @param logValuesPerChunk
   * @return
   */
  public static ResourceHolder<int[]> getShapeshiftIntsDecodedValuesArray(int logValuesPerChunk)
  {
    switch (logValuesPerChunk) {
      case 12:
        return shapeshiftSmallestIntsDecodedValuesArrayPool.take();
      case 13:
        return shapeshiftSmallerIntsDecodedValuesArrayPool.take();
      case 14:
      default:
        return shapeshiftIntsDecodedValuesArrayPool.take();
    }
  }

  /**
   * Get pooled encoded values array for {@link io.druid.segment.data.ShapeShiftingColumnarInts}
   * @param logValuesPerChunk
   * @return
   */
  public static ResourceHolder<int[]> getShapeshiftIntsEncodedValuesArray(int logValuesPerChunk)
  {
    switch (logValuesPerChunk) {
      case 12:
        return shapeshiftSmallestIntsEncodedValuesArrayPool.take();
      case 13:
        return shapeshiftSmallerIntsEncodedValuesArrayPool.take();
      case 14:
      default:
        return shapeshiftIntsEncodedValuesArrayPool.take();
    }
  }

  public static NonBlockingPool<SkippableIntegerCODEC> getShapeshiftFastPforPool(int logValuesPerChunk)
  {
    switch (logValuesPerChunk) {
      case 12:
      case 13:
      case 14:
      default:
        return shapeshiftFastPforPool;
    }
  }

  public static NonBlockingPool<SkippableIntegerCODEC> getShapeshiftLemirePool(byte header, int logValuesPerChunk)
  {
    switch (header) {
      case IntCodecs.FASTPFOR:
      default:
        return getShapeshiftFastPforPool(logValuesPerChunk);
    }
  }
}
