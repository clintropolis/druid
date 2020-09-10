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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nullable;
import java.lang.reflect.Array;

public abstract class VectorExprEval<T>
{
  final T values;
  @Nullable
  final boolean [] nulls;

  public VectorExprEval(T values, @Nullable boolean[] nulls)
  {
    this.values = values;
    this.nulls = nulls;
  }

  public T values()
  {
    return values;
  }

  @Nullable
  public Object get(int index)
  {
    if (nulls == null || !nulls[index]) {
      return Array.get(values, index);
    }
    return null;
  }

//  abstract long getLong(int index);
//  abstract double getDouble(int index);
//  abstract String getString(int index);

  @Nullable
  public boolean[] nulls()
  {
    return nulls;
  }

  public abstract ExprType getType();

  public abstract long[] asLongs();

  public abstract double[] asDoubles();

  public Object[] asObjects()
  {
    Object[] output = new Object[Array.getLength(values)];
    for(int i = 0; i < output.length; ++i){
      output[i] = get(i);
    }
    return output;
  }

  public abstract String[] asStrings();

  public abstract long[][] asLongArrays();

  public abstract double[][] asDoubleArrays();

  public abstract String[][] asStringArrays();
}
