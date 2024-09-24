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

package org.apache.druid.segment.incremental;

import com.google.common.collect.Iterators;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.filter.ValueMatchers;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Iterator;
import java.util.List;

public class IncrementalIndexCursorHolder implements CursorHolder
{
  private final IncrementalIndexRowSelector rowSelector;
  private final CursorBuildSpec spec;
  private final List<OrderBy> ordering;
  private final String timeColumnName;
  private final boolean isPreaggregated;

  public IncrementalIndexCursorHolder(
      IncrementalIndexRowSelector rowSelector,
      CursorBuildSpec spec,
      String timeColumnName,
      boolean isPreAggregated
  )
  {
    this.rowSelector = rowSelector;
    this.spec = spec;
    this.timeColumnName = timeColumnName;
    this.isPreaggregated = isPreAggregated;
    List<OrderBy> ordering = rowSelector.getOrdering();
    if (Cursors.getTimeOrdering(ordering) != Order.NONE) {
      if (Cursors.preferDescendingTimeOrdering(spec)) {
        this.ordering = Cursors.descendingTimeOrder();
      } else {
        this.ordering = Cursors.ascendingTimeOrder();
      }
    } else {
      this.ordering = ordering;
    }
  }

  @Override
  public Cursor asCursor()
  {
    if (rowSelector.isEmpty()) {
      return null;
    }

    if (spec.getQueryMetrics() != null) {
      spec.getQueryMetrics().vectorized(false);
    }

    return new IncrementalIndexCursor(
        rowSelector,
        spec,
        timeColumnName,
        getTimeOrder(ordering, timeColumnName)
    );
  }

  @Override
  public boolean isPreAggregated()
  {
    return isPreaggregated;
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  static class IncrementalIndexCursor implements Cursor
  {
    private final Iterable<IncrementalIndexRow> cursorIterable;
    private final IncrementalIndexRowHolder currEntry;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final ValueMatcher filterMatcher;
    private final int maxRowIndex;
    @MonotonicNonNull
    private Iterator<IncrementalIndexRow> baseIter = null;
    private int numAdvanced;
    private boolean done;

    IncrementalIndexCursor(
        IncrementalIndexRowSelector rowSelector,
        CursorBuildSpec buildSpec,
        String timeColumn,
        Order timeOrder
    )
    {
      currEntry = new IncrementalIndexRowHolder();
      // Set maxRowIndex before creating the filterMatcher. See https://github.com/apache/druid/pull/6340
      maxRowIndex = rowSelector.getLastRowIndex();
      numAdvanced = -1;

      cursorIterable = rowSelector.getFacts().timeRangeIterable(
          timeOrder == Order.DESCENDING,
          buildSpec.getInterval().getStartMillis(),
          buildSpec.getInterval().getEndMillis()
      );
      columnSelectorFactory = new IncrementalIndexColumnSelectorFactory(
          rowSelector,
          buildSpec.getVirtualColumns(),
          timeColumn,
          timeOrder,
          currEntry
      );
      filterMatcher = buildSpec.getFilter() == null
                      ? ValueMatchers.allTrue()
                      : buildSpec.getFilter().makeMatcher(columnSelectorFactory);
      reset();
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public void advance()
    {
      if (!baseIter.hasNext()) {
        done = true;
        return;
      }

      while (baseIter.hasNext()) {
        BaseQuery.checkInterrupted();

        final IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          continue;
        }

        currEntry.set(entry);

        if (filterMatcher.matches(false)) {
          return;
        }
      }

      done = true;
    }

    @Override
    public void advanceUninterruptibly()
    {
      if (!baseIter.hasNext()) {
        done = true;
        return;
      }

      while (baseIter.hasNext()) {
        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          continue;
        }

        currEntry.set(entry);

        if (filterMatcher.matches(false)) {
          return;
        }
      }

      done = true;
    }

    @Override
    public boolean isDone()
    {
      return done;
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void reset()
    {
      baseIter = cursorIterable.iterator();

      if (numAdvanced == -1) {
        numAdvanced = 0;
      } else {
        Iterators.advance(baseIter, numAdvanced);
      }

      BaseQuery.checkInterrupted();

      boolean foundMatched = false;
      while (baseIter.hasNext()) {
        IncrementalIndexRow entry = baseIter.next();
        if (beyondMaxRowIndex(entry.getRowIndex())) {
          numAdvanced++;
          continue;
        }
        currEntry.set(entry);
        if (filterMatcher.matches(false)) {
          foundMatched = true;
          break;
        }

        numAdvanced++;
      }

      done = !foundMatched;
    }

    private boolean beyondMaxRowIndex(int rowIndex)
    {
      // ignore rows whose rowIndex is beyond the maxRowIndex
      // rows are order by timestamp, not rowIndex,
      // so we still need to go through all rows to skip rows added after cursor created
      return rowIndex > maxRowIndex;
    }
  }

  private static Order getTimeOrder(List<OrderBy> ordering, String timeColumnName)
  {
    if (!ordering.isEmpty() && timeColumnName.equals(ordering.get(0).getColumnName())) {
      return ordering.get(0).getOrder();
    } else {
      return Order.NONE;
    }
  }
}
