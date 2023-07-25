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

package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import java.nio.ByteBuffer;

public class ByteBloomFilterAggregator extends BaseBloomFilterAggregator<BaseObjectColumnValueSelector<Object>>
{
  private final ExpressionType columnType;

  ByteBloomFilterAggregator(
      BaseObjectColumnValueSelector<Object> baseObjectColumnValueSelector,
      TypeSignature<ValueType> columnType,
      int maxNumEntries,
      boolean onHeap
  )
  {
    super(baseObjectColumnValueSelector, maxNumEntries, onHeap);
    this.columnType = ExpressionType.fromColumnTypeStrict(columnType);
  }

  @Override
  void bufferAdd(ByteBuffer buf)
  {
    final Object val = selector.getObject();
    if (val == null) {
      BloomKFilter.addBytes(buf, null, 0, 0);
    } else {
      BloomKFilter.addBytes(buf, ExprEval.toBytes(columnType, val));
    }
  }
}
