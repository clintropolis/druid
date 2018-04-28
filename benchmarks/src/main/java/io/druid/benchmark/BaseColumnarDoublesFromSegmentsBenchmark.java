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

package io.druid.benchmark;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

@State(Scope.Benchmark)
public class BaseColumnarDoublesFromSegmentsBenchmark extends BaseColumnarDoublesBenchmark
{
  @Param({
      "tpch-lineitem-1g-values-float-l_discount.txt",
      "tpch-lineitem-1g-values-float-l_extendedprice.txt",
      "tpch-lineitem-1g-values-float-l_tax.txt",
      "tpch-lineitem-1g-values-float-max_l_discount.txt",
      "tpch-lineitem-1g-values-float-min_l_discount.txt"
  })
  String fileName;

  int rows;

  void initializeValues() throws IOException
  {
    final String tmpPath = "tmp/";
    final String dirPath = "tmp/segCompress/";
    File tmp = new File(tmpPath);
    tmp.mkdir();
    File dir = new File(dirPath);
    dir.mkdir();
    File dataFile = new File(dir, fileName);

    if (fileName.contains("float")) {
      initializeFloatValues(dataFile);
    } else {
      initializeDoubleValues(dataFile);
    }
  }


  void initializeFloatValues(File dataFile) throws IOException
  {
    ArrayList<Float> values = Lists.newArrayList();
    try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        float value = Float.parseFloat(line);
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
        values.add(value);
        rows++;
      }
    }

    floatVals = ArrayUtils.toPrimitive(values.toArray(new Float[0]), 0.0F);
  }

  void initializeDoubleValues(File dataFile) throws IOException
  {
    ArrayList<Double> values = Lists.newArrayList();
    try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        double value = Double.parseDouble(line);
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
        values.add(value);
        rows++;
      }
    }

    doubleVals = ArrayUtils.toPrimitive(values.toArray(new Double[0]), 0.0D);
  }
}
