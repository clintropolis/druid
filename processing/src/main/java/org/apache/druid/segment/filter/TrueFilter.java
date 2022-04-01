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

package org.apache.druid.segment.filter;

import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BooleanVectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TrueFilter implements Filter
{
  private static final TrueFilter INSTANCE = new TrueFilter();

  public static TrueFilter instance()
  {
    return INSTANCE;
  }

  private TrueFilter()
  {
  }

  @Override
  public <T> T getBitmapResult(ColumnIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.wrapAllTrue(Filters.allTrue(selector));
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return BooleanValueMatcher.of(true);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return BooleanVectorValueMatcher.of(factory.getReadableVectorInspector(), true);
  }

  @Nullable
  @Override
  public ColumnIndexCapabilities getIndexCapabilities(ColumnIndexSelector selector)
  {
    return new SimpleColumnIndexCapabilities(true, true);
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return true;
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    return this;
  }

  @Override
  public double estimateSelectivity(ColumnIndexSelector indexSelector)
  {
    return 1;
  }

  @Override
  public String toString()
  {
    return "true";
  }

  @Override
  public final int hashCode()
  {
    return TrueFilter.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof TrueFilter;
  }
}
