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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.ArrayElementIndexes;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ArrayContainsFilter extends AbstractOptimizableDimFilter implements Filter
{
  private final String column;
  private final ColumnType elementMatchValueType;
  private final Object elementMatchValue;
  private final ExprEval<?> elementMatchValueEval;

  @Nullable
  private final FilterTuning filterTuning;
  private final DruidPredicateFactory predicateFactory;

  @JsonCreator
  public ArrayContainsFilter(
      @JsonProperty("column") String column,
      @JsonProperty("elementMatchValueType") ColumnType elementMatchValueType,
      @JsonProperty("elementMatchValue") Object elementMatchValue,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    if (column == null) {
      throw InvalidInput.exception("Invalid array_contains filter, column cannot be null");
    }
    this.column = column;
    if (elementMatchValueType == null) {
      throw InvalidInput.exception("Invalid array_contains filter on column [%s], elementMatchValueType cannot be null", column);
    }
    this.elementMatchValueType = elementMatchValueType;
    this.elementMatchValue = elementMatchValue;
    this.elementMatchValueEval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(elementMatchValueType), elementMatchValue);
    this.filterTuning = filterTuning;
    this.predicateFactory = new ArrayContainsPredicateFactory(elementMatchValueEval);
  }

  @Override
  public byte[] getCacheKey()
  {
    final TypeStrategy<Object> typeStrategy = elementMatchValueEval.type().getStrategy();
    final int size = typeStrategy.estimateSizeBytes(elementMatchValueEval.value());
    final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
    typeStrategy.write(valueBuffer, elementMatchValueEval.value(), size);
    return new CacheKeyBuilder(DimFilterUtils.ARRAY_CONTAINS_CACHE_ID)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(column)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(elementMatchValueType.asTypeString())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(valueBuffer.array())
        .build();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return this;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @JsonProperty
  public ColumnType getElementMatchValueType()
  {
    return elementMatchValueType;
  }

  @JsonProperty
  public Object getElementMatchValue()
  {
    return elementMatchValue;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public String toString()
  {
    DimFilter.DimFilterToStringBuilder bob =
        new DimFilter.DimFilterToStringBuilder().append("array_contains(")
                                                .appendDimension(column, null)
                                                .append(", ")
                                                .append(elementMatchValueEval.value())
                                                .append(")");
    if (!ColumnType.STRING.equals(elementMatchValueType)) {
      bob.append(" (" + elementMatchValueType.asTypeString() + ")");
    }
    return bob.appendFilterTuning(filterTuning).build();
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
    ArrayContainsFilter that = (ArrayContainsFilter) o;
    if (!column.equals(that.column)) {
      return false;
    }
    if (!Objects.equals(elementMatchValueType, that.elementMatchValueType)) {
      return false;
    }
    if (!Objects.equals(filterTuning, that.filterTuning)) {
      return false;
    }
    if (elementMatchValueType.isArray()) {
      return Arrays.deepEquals(elementMatchValueEval.asArray(), that.elementMatchValueEval.asArray());
    } else {
      return Objects.equals(elementMatchValueEval.value(), that.elementMatchValueEval.value());
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, elementMatchValueType, elementMatchValueEval.value(), filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
      return null;
    }

    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
    if (indexSupplier == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }
    final ArrayElementIndexes elementIndexes = indexSupplier.as(ArrayElementIndexes.class);
    if (elementIndexes != null) {
      return elementIndexes.containsValue(elementMatchValueEval.value(), elementMatchValueType);
    }

    if (selector.getColumnCapabilities(column) != null && !selector.getColumnCapabilities(column).isArray()) {
      // column is not an array, behave like a normal equality filter
      return EqualityFilter.getEqualityIndex(column, elementMatchValueEval, elementMatchValueType, selector);
    }
    // column exists, but has no indexes we can use
    return null;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeProcessor(
        column,
        new TypedConstantElementValueMatcherFactory(elementMatchValueEval, predicateFactory),
        factory
    );
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    final ColumnCapabilities capabilities = factory.getColumnCapabilities(column);

    if (elementMatchValueType.isPrimitive() && (capabilities == null || capabilities.isPrimitive())) {
      return ColumnProcessors.makeVectorProcessor(
          column,
          VectorValueMatcherColumnProcessorFactory.instance(),
          factory
      ).makeMatcher(elementMatchValueEval.value(), elementMatchValueType);
    }
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(new ArrayContainsPredicateFactory(elementMatchValueEval));
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return Filters.supportsSelectivityEstimation(this, column, columnSelector, indexSelector);
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(column);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(column);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          columnRewrites
      );
    }

    return new ArrayContainsFilter(
        rewriteDimensionTo,
        elementMatchValueType,
        elementMatchValue,
        filterTuning
    );
  }

  private static class ArrayContainsPredicateFactory implements DruidPredicateFactory
  {
    private final ExprEval<?> elementMatchValue;
    private final Supplier<Predicate<String>> stringPredicateSupplier;
    private final Supplier<DruidLongPredicate> longPredicateSupplier;
    private final Supplier<DruidFloatPredicate> floatPredicateSupplier;
    private final Supplier<DruidDoublePredicate> doublePredicateSupplier;
    private final ConcurrentHashMap<TypeSignature<ValueType>, Predicate<Object[]>> arrayPredicates;
    private final Supplier<Predicate<Object[]>> typeDetectingArrayPredicateSupplier;
    private final Supplier<Predicate<Object>> objectPredicateSupplier;

    public ArrayContainsPredicateFactory(ExprEval<?> elementMatchValue)
    {
      this.elementMatchValue = elementMatchValue;
      this.stringPredicateSupplier = makeStringPredicateSupplier();
      this.longPredicateSupplier = makeLongPredicateSupplier();
      this.floatPredicateSupplier = makeFloatPredicateSupplier();
      this.doublePredicateSupplier = makeDoublePredicateSupplier();
      this.objectPredicateSupplier = makeObjectPredicateSupplier();
      this.arrayPredicates = new ConcurrentHashMap<>();
      this.typeDetectingArrayPredicateSupplier = makeTypeDetectingArrayPredicate();
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      return stringPredicateSupplier.get();
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      return longPredicateSupplier.get();
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      return floatPredicateSupplier.get();
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      return doublePredicateSupplier.get();
    }

    @Override
    public Predicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
    {
      if (arrayType == null) {
        // fall back to per row detection if input array type is unknown
        return typeDetectingArrayPredicateSupplier.get();
      }

      return new FallbackPredicate<>(
          arrayPredicates.computeIfAbsent(arrayType, (existing) -> makeArrayPredicateInternal(arrayType)),
          ExpressionType.fromColumnTypeStrict(arrayType)
      );
    }

    @Override
    public Predicate<Object> makeObjectPredicate()
    {
      return objectPredicateSupplier.get();
    }

    private Supplier<Predicate<String>> makeStringPredicateSupplier()
    {
      // if element match value is an array, this will never be true
      if (elementMatchValue.isArray() && elementMatchValue.value() != null && elementMatchValue.asArray().length > 1) {
        return Predicates::alwaysFalse;
      }
      // else it is a simple equality match
      return EqualityFilter.makeStringPredicateSupplier(elementMatchValue);
    }

    private Supplier<DruidLongPredicate> makeLongPredicateSupplier()
    {
      // if element match value is an array, this will never be true
      if (elementMatchValue.isArray() && elementMatchValue.value() != null && elementMatchValue.asArray().length > 1) {
        return () -> DruidLongPredicate.ALWAYS_FALSE;
      }
      // else it is a simple equality match
      return EqualityFilter.makeLongPredicateSupplier(elementMatchValue);
    }

    private Supplier<DruidFloatPredicate> makeFloatPredicateSupplier()
    {
      // if element match value is an array, this will never be true
      if (elementMatchValue.isArray() && elementMatchValue.value() != null && elementMatchValue.asArray().length > 1) {
        return () -> DruidFloatPredicate.ALWAYS_FALSE;
      }
      // else it is a simple equality
      return EqualityFilter.makeFloatPredicateSupplier(elementMatchValue);
    }

    private Supplier<DruidDoublePredicate> makeDoublePredicateSupplier()
    {
      // if element match value is an array, this will never be true
      if (elementMatchValue.isArray() && elementMatchValue.value() != null && elementMatchValue.asArray().length > 1) {
        return () -> DruidDoublePredicate.ALWAYS_FALSE;
      }
      // else it is a simple equality match
      return EqualityFilter.makeDoublePredicateSupplier(elementMatchValue);
    }

    private Supplier<Predicate<Object>> makeObjectPredicateSupplier()
    {
      return Suppliers.memoize(() -> input -> {
        final ExprEval<?> inputEval = ExprEval.bestEffortOf(StructuredData.unwrap(input));
        final Predicate<Object[]> matcher = new FallbackPredicate<>(
            arrayPredicates.get(ExpressionType.toColumnType(inputEval.asArrayType())),
            inputEval.asArrayType()
        );
        return matcher.apply(inputEval.asArray());
      });
    }

    private Supplier<Predicate<Object[]>> makeTypeDetectingArrayPredicate()
    {
      return Suppliers.memoize(() -> input -> {
        // just use object predicate logic
        final Predicate<Object> objectPredicate = objectPredicateSupplier.get();
        return objectPredicate.apply(input);
      });
    }

    private Predicate<Object[]> makeArrayPredicateInternal(TypeSignature<ValueType> arrayType)
    {
      final ExpressionType expressionType = ExpressionType.fromColumnTypeStrict(arrayType);

      final Comparator elementComparator = arrayType.getElementType().getNullableStrategy();
      if (elementMatchValue.isArray()) {
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(elementMatchValue, expressionType);
        if (castForComparison == null) {
          return Predicates.alwaysFalse();
        }
        final Object[] matchArray = castForComparison.asArray();
        return input -> {
          if (input == null) {
            return false;
          }
          boolean allMatch = true;
          for (Object matchElement : matchArray) {
            boolean innerMatch = false;
            for (Object element : input) {
              innerMatch = innerMatch || elementComparator.compare(element, matchElement) == 0;
            }
            allMatch = allMatch && innerMatch;
          }
          return allMatch;
        };
      } else {
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(
            elementMatchValue,
            (ExpressionType) expressionType.getElementType()
        );
        if (castForComparison == null) {
          return Predicates.alwaysFalse();
        }
        final Object matchVal = castForComparison.value();
        return input -> {
          if (input == null) {
            return false;
          }
          boolean anyMatch = false;
          for (Object elem : input) {
            anyMatch = anyMatch || elementComparator.compare(elem, matchVal) == 0;
          }
          return anyMatch;
        };
      }
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
      ArrayContainsPredicateFactory that = (ArrayContainsPredicateFactory) o;
      if (!Objects.equals(elementMatchValue.type(), that.elementMatchValue.type())) {
        return false;
      }
      if (elementMatchValue.isArray()) {
        return Arrays.deepEquals(elementMatchValue.asArray(), that.elementMatchValue.asArray());
      }
      return Objects.equals(elementMatchValue.value(), that.elementMatchValue.value());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(elementMatchValue);
    }
  }

  private static class TypedConstantElementValueMatcherFactory extends EqualityFilter.TypedConstantValueMatcherFactory
  {
    public TypedConstantElementValueMatcherFactory(
        ExprEval<?> matchValue,
        DruidPredicateFactory predicateFactory
    )
    {
      super(matchValue, predicateFactory);
    }

    @Override
    public ValueMatcher makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      if (matchValue.isArray()) {
        return predicateMatcherFactory.makeDimensionProcessor(selector, multiValue);
      }
      return super.makeDimensionProcessor(selector, multiValue);
    }

    @Override
    public ValueMatcher makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      if (matchValue.isArray()) {
        return predicateMatcherFactory.makeFloatProcessor(selector);
      }
      return super.makeFloatProcessor(selector);
    }

    @Override
    public ValueMatcher makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      if (matchValue.isArray()) {
        return predicateMatcherFactory.makeDoubleProcessor(selector);
      }
      return super.makeDoubleProcessor(selector);
    }

    @Override
    public ValueMatcher makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      if (matchValue.isArray()) {
        return predicateMatcherFactory.makeLongProcessor(selector);
      }
      return super.makeLongProcessor(selector);
    }
  }
}
