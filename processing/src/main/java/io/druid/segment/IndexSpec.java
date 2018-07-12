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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * IndexSpec defines segment storage format options to be used at indexing time,
 * such as bitmap type, and column compression formats.
 *
 * IndexSpec is specified as part of the TuningConfig for the corresponding index task.
 */
public class IndexSpec
{
  public static final CompressionStrategy DEFAULT_METRIC_COMPRESSION = CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY;
  public static final CompressionStrategy DEFAULT_DIMENSION_COMPRESSION = CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY;
  public static final CompressionFactory.LongEncodingStrategy DEFAULT_LONG_ENCODING = CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY;
  public static final ColumnEncodingStrategy DEFAULT_INT_ENCODING_STRATEGY =
      new ColumnEncodingStrategy(EncodingStrategy.COMPRESSION, null, null);

  private static final Set<CompressionStrategy> METRIC_COMPRESSION = Sets.newHashSet(
      Arrays.asList(CompressionStrategy.values())
  );

  private static final Set<CompressionStrategy> DIMENSION_COMPRESSION = Sets.newHashSet(
      Arrays.asList(CompressionStrategy.noNoneValues())
  );

  private static final Set<CompressionFactory.LongEncodingStrategy> LONG_ENCODING_NAMES = Sets.newHashSet(
      Arrays.asList(CompressionFactory.LongEncodingStrategy.values())
  );

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final CompressionStrategy dimensionCompression;
  private final CompressionStrategy metricCompression;
  private final CompressionFactory.LongEncodingStrategy longEncoding;

  private final ColumnEncodingStrategy intEncodingStrategy;

  /**
   * Creates an IndexSpec with default parameters
   */
  public IndexSpec()
  {
    this(
        null,
        null,
        null,
        null,
        null
    );
  }

  public IndexSpec(
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy dimensionCompression,
      CompressionStrategy metricCompression,
      CompressionFactory.LongEncodingStrategy longEncoding
  )
  {
    this(
        bitmapSerdeFactory,
        dimensionCompression,
        metricCompression,
        longEncoding,
        null
    );
  }

  /**
   * Creates an IndexSpec with the given storage format settings.
   *
   * @param bitmapSerdeFactory   type of bitmap to use (e.g. roaring or concise), null to use the default.
   *                             Defaults to the bitmap type specified by the (deprecated) "druid.processing.bitmap.type"
   *                             setting, or, if none was set, uses the default defined in {@link BitmapSerde}
   * @param dimensionCompression compression format for dimension columns, null to use the default.
   *                             Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY}
   * @param metricCompression    compression format for metric columns, null to use the default.
   *                             Defaults to {@link CompressionStrategy#DEFAULT_COMPRESSION_STRATEGY}
   * @param longEncoding         encoding strategy for metric and dimension columns with type long, null to use the default.
   *                             Defaults to {@link CompressionFactory#DEFAULT_LONG_ENCODING_STRATEGY}
   * @param intEncodingStrategy     encoding strategy for integer columns
   */
  @JsonCreator
  public IndexSpec(
      @JsonProperty("bitmap") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("dimensionCompression") CompressionStrategy dimensionCompression,
      @JsonProperty("metricCompression") CompressionStrategy metricCompression,
      @JsonProperty("longEncoding") CompressionFactory.LongEncodingStrategy longEncoding,
      @JsonProperty("intEncodingStrategy") ColumnEncodingStrategy intEncodingStrategy
  )
  {
    Preconditions.checkArgument(dimensionCompression == null || DIMENSION_COMPRESSION.contains(dimensionCompression),
                                "Unknown compression type[%s]", dimensionCompression
    );

    Preconditions.checkArgument(metricCompression == null || METRIC_COMPRESSION.contains(metricCompression),
                                "Unknown compression type[%s]", metricCompression
    );

    Preconditions.checkArgument(longEncoding == null || LONG_ENCODING_NAMES.contains(longEncoding),
                                "Unknown long encoding type[%s]", longEncoding
    );

    this.bitmapSerdeFactory = bitmapSerdeFactory != null ? bitmapSerdeFactory : new ConciseBitmapSerdeFactory();
    this.dimensionCompression = dimensionCompression == null ? DEFAULT_DIMENSION_COMPRESSION : dimensionCompression;
    this.metricCompression = metricCompression == null ? DEFAULT_METRIC_COMPRESSION : metricCompression;
    this.longEncoding = longEncoding == null ? DEFAULT_LONG_ENCODING : longEncoding;
    this.intEncodingStrategy = intEncodingStrategy == null ? DEFAULT_INT_ENCODING_STRATEGY : intEncodingStrategy;
  }

  @JsonProperty("bitmap")
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public CompressionStrategy getDimensionCompression()
  {
    return dimensionCompression;
  }

  @JsonProperty
  public CompressionStrategy getMetricCompression()
  {
    return metricCompression;
  }

  @JsonProperty
  public CompressionFactory.LongEncodingStrategy getLongEncoding()
  {
    return longEncoding;
  }

  @JsonProperty
  public ColumnEncodingStrategy getIntEncodingStrategy()
  {
    return intEncodingStrategy;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexSpec indexSpec = (IndexSpec) o;
    return Objects.equals(bitmapSerdeFactory, indexSpec.bitmapSerdeFactory) &&
           dimensionCompression == indexSpec.dimensionCompression &&
           metricCompression == indexSpec.metricCompression &&
           longEncoding == indexSpec.longEncoding &&
           intEncodingStrategy.equals(indexSpec.intEncodingStrategy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        bitmapSerdeFactory,
        dimensionCompression,
        metricCompression,
        longEncoding,
        intEncodingStrategy
    );
  }

  @Override
  public String toString()
  {
    return "IndexSpec{" +
           "bitmapSerdeFactory=" + bitmapSerdeFactory +
           ", dimensionCompression=" + dimensionCompression +
           ", metricCompression=" + metricCompression +
           ", longEncoding=" + longEncoding +
           ", intEncodingStrategy=" + intEncodingStrategy +
           '}';
  }

  /**
   * Encapsulate column encoding strategy options
   */
  public static class ColumnEncodingStrategy
  {
    private static final EncodingStrategy DEFAULT_ENCODING_STRATEGY = EncodingStrategy.COMPRESSION;
    private static final ShapeShiftOptimizationTarget DEFAULT_OPTIMIZATION_TARGET = ShapeShiftOptimizationTarget.FASTBUTSMALLISH;
    private static final ShapeShiftingBlockSize DEFAULT_BLOCK_SIZE = ShapeShiftingBlockSize.LARGE;
    private static final Set<EncodingStrategy> ENCODING_STRATEGIES = Sets.newHashSet(
        Arrays.asList(EncodingStrategy.values())
    );
    private static final Set<ShapeShiftOptimizationTarget> OPTIMIZATION_TARGETS = Sets.newHashSet(
        Arrays.asList(ShapeShiftOptimizationTarget.values())
    );
    private static final Set<ShapeShiftingBlockSize> BLOCK_SIZES = Sets.newHashSet(
        Arrays.asList(ShapeShiftingBlockSize.values())
    );

    private final EncodingStrategy strategy;
    private final ShapeShiftOptimizationTarget optimizationTarget;
    private final ShapeShiftingBlockSize blockSize;

    @JsonCreator
    public ColumnEncodingStrategy(
        @JsonProperty("strategy") EncodingStrategy strategy,
        @JsonProperty("optimizationTarget") ShapeShiftOptimizationTarget optimizationTarget,
        @JsonProperty("blockSize") ShapeShiftingBlockSize blockSize
    )
    {
      Preconditions.checkArgument(strategy == null || ENCODING_STRATEGIES.contains(strategy),
                                  "Unknown encoding strategy[%s]", strategy
      );
      Preconditions.checkArgument(optimizationTarget == null || OPTIMIZATION_TARGETS.contains(optimizationTarget),
                                  "Unknown shapeshift optimization target[%s]", optimizationTarget
      );
      Preconditions.checkArgument(blockSize == null || BLOCK_SIZES.contains(blockSize),
                                  "Unknown shapeshift block size[%s]", blockSize
      );
      this.strategy = strategy == null ? DEFAULT_ENCODING_STRATEGY : strategy;
      this.optimizationTarget = optimizationTarget == null ? DEFAULT_OPTIMIZATION_TARGET : optimizationTarget;
      this.blockSize = blockSize == null ? DEFAULT_BLOCK_SIZE : blockSize;
    }

    @JsonProperty
    public EncodingStrategy getStrategy()
    {
      return strategy;
    }

    @JsonProperty
    public ShapeShiftOptimizationTarget getOptimizationTarget()
    {
      return optimizationTarget;
    }

    @JsonProperty
    public ShapeShiftingBlockSize getBlockSize()
    {
      return blockSize;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnEncodingStrategy that = (ColumnEncodingStrategy) o;
      return strategy == that.strategy &&
             optimizationTarget == that.optimizationTarget &&
             blockSize == that.blockSize;
    }

    @Override
    public int hashCode()
    {

      return Objects.hash(strategy, optimizationTarget, blockSize);
    }

    @Override
    public String toString()
    {
      return "ColumnEncodingStrategy{" +
             "strategy=" + strategy +
             ", optimizationTarget=" + optimizationTarget +
             ", blockSize=" + blockSize +
             '}';
    }
  }


  public enum EncodingStrategy
  {
    COMPRESSION,
    SHAPESHIFT;


    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static EncodingStrategy fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  /**
   * Log base 2 values per chunk in shapeshift encoding
   */
  public enum ShapeShiftingBlockSize
  {
    /**
     * Shapeshift will encode blocks of 2^16 bytes. This puts the most memory pressure at indexing and query time in
     * exchange for the potential to reduce encoded size. Approximate footprint is 64k off heap for decompression buffer
     * and 129k on heap for value arrays
     */
    LARGE(16),
    /**
     * Shapeshift will encode blocks of 2^15 bytes. Approximate footprint is 32k off heap for decompression buffer
     * and 65k on heap for value arrays
     */
    MIDDLE(15),
    /**
     * Shapeshift will encode blocks of 2^14 bytes. This approach is very conservative and uses less overall memory
     * than {@link IndexSpec.EncodingStrategy#COMPRESSION} in exchange for increased encoding size overhead and
     * potentially smaller gains in overall encoded size. Approximate footprint is 16k off heap for decompression buffer
     * and 33k on heap for value arrays.
     */
    SMALL(14);

    int logBlockSize;

    ShapeShiftingBlockSize(int blockSize)
    {
      this.logBlockSize = blockSize;
    }

    public byte getLogBlockSize()
    {
      return (byte) this.logBlockSize;
    }


    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static ShapeShiftingBlockSize fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  public enum ShapeShiftOptimizationTarget
  {
    SMALLER,
    FASTBUTSMALLISH,
    FASTER;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static ShapeShiftOptimizationTarget fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }
}
