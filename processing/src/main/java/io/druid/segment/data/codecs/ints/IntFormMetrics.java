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

package io.druid.segment.data.codecs.ints;

import io.druid.segment.IndexSpec;
import io.druid.segment.data.codecs.FormMetrics;

public class IntFormMetrics extends FormMetrics
{
  private int minValue = Integer.MAX_VALUE;
  private int maxValue = Integer.MIN_VALUE;
  private int numRunValues = 0;
  private int numDistinctRuns = 0;
  private int longestRun;
  private int currentRun;
  private int previousValue;
  private int numValues = 0;
  private boolean isFirstValue = true;

  public IntFormMetrics(IndexSpec.ShapeShiftOptimizationTarget target)
  {
    super(target);
  }

  public void processNextRow(int val)
  {
    if (isFirstValue) {
      isFirstValue = false;
      previousValue = val;
      currentRun = 1;
      longestRun = 1;
    } else {
      if (val == previousValue) {
        currentRun++;
        if (currentRun > 2) {
          numRunValues++;
        }
      } else {
        previousValue = val;
        if (currentRun > 2) {
          numDistinctRuns++;
        }
        currentRun = 1;
      }
    }

    if (currentRun > longestRun) {
      longestRun = currentRun;
    }
    if (val < minValue) {
      minValue = val;
    }
    if (val > maxValue) {
      maxValue = val;
    }
    numValues++;
  }

  public int getNumValues()
  {
    return numValues;
  }

  public int getMinValue()
  {
    return minValue;
  }

  public int getMaxValue()
  {
    return maxValue;
  }

  public int getNumRunValues()
  {
    return numRunValues;
  }

  public int getNumDistinctRuns()
  {
    return numDistinctRuns;
  }

  public int getLongestRun()
  {
    return longestRun;
  }

  public boolean isConstant()
  {
    return minValue == maxValue;
  }

  public boolean isZero()
  {
    return minValue == 0 && minValue == maxValue;
  }
}
