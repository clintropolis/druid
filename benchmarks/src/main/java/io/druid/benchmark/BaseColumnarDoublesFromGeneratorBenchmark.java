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

import io.druid.benchmark.datagen.BenchmarkColumnSchema;
import io.druid.benchmark.datagen.BenchmarkColumnValueGenerator;
import io.druid.segment.column.ValueType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

@State(Scope.Benchmark)
public class BaseColumnarDoublesFromGeneratorBenchmark extends BaseColumnarDoublesBenchmark
{
  static BenchmarkColumnValueGenerator makeGenerator(
      String type,
      String distribution,
      int range,
      int rows
  )
  {
    ValueType valueType = type.equals("float") ? ValueType.FLOAT : ValueType.DOUBLE;

    // Zipf to see what happens when you store integers in your doubles
    // PiZipf to get zipf scaled by PI - 2 to get a real numbers (so entire range of bits will be set rather than just part of exponent and part of mantissa)
    switch (distribution) {
      case "lazyZipfLow":
      case "lazyPiZipfLow":
        BenchmarkColumnSchema lzipfLowSchema = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0d,
            0,
            range,
            1d
        );
        return lzipfLowSchema.makeGenerator(1);
      case "lazyZipfHi":
      case "lazyPiZipfHi":
        BenchmarkColumnSchema lzipfHighSchema = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0d,
            0,
            range,
            3d
        );
        return lzipfHighSchema.makeGenerator(1);
      case "nullp50ZipfLow":
      case "nullp50PiZipfLow":
        BenchmarkColumnSchema nullp50ZipfLow = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.5,
            0,
            range,
            3d
        );
        return nullp50ZipfLow.makeGenerator(1);
      case "nullp75ZipfLow":
      case "nullp75PiZipfLow":
        BenchmarkColumnSchema nullp75ZipfLow = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.75,
            0,
            range,
            1d
        );
        return nullp75ZipfLow.makeGenerator(1);
      case "nullp90ZipfLow":
      case "nullp90PiZipfLow":
        BenchmarkColumnSchema nullp90ZipfLow = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.9,
            0,
            range,
            1d
        );
        return nullp90ZipfLow.makeGenerator(1);
      case "nullp95ZipfLow":
      case "nullp95PiZipfLow":
        BenchmarkColumnSchema nullp95ZipfLow = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.95,
            0,
            range,
            1d
        );
        return nullp95ZipfLow.makeGenerator(1);
      case "nullp99ZipfLow":
      case "nullp99PiZipfLow":
        BenchmarkColumnSchema nullp99ZipfLow = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.99,
            0,
            range,
            1d
        );
        return nullp99ZipfLow.makeGenerator(1);
      case "nullp50ZipfHi":
      case "nullp50PiZipfHi":
        BenchmarkColumnSchema nullp50ZipfHi = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.5,
            0,
            range,
            3d
        );
        return nullp50ZipfHi.makeGenerator(1);
      case "nullp75ZipfHi":
      case "nullp75PiZipfHi":
        BenchmarkColumnSchema nullp75ZipfHi = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.75,
            0,
            range,
            3d
        );
        return nullp75ZipfHi.makeGenerator(1);
      case "nullp90ZipfHi":
      case "nullp90PiZipfHi":
        BenchmarkColumnSchema nullp90ZipfHi = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.9,
            0,
            range,
            3d
        );
        return nullp90ZipfHi.makeGenerator(1);
      case "nullp95ZipfHi":
      case "nullp95PiZipfHi":
        BenchmarkColumnSchema nullp95ZipfHi = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.95,
            0,
            range,
            3d
        );
        return nullp95ZipfHi.makeGenerator(1);
      case "nullp99ZipfHi":
      case "nullp99PiZipfHi":
        BenchmarkColumnSchema nullp99ZipfHi = BenchmarkColumnSchema.makeLazyZipf(
            "",
            valueType,
            true,
            1,
            0.99,
            0,
            range,
            3d
        );
        return nullp99ZipfHi.makeGenerator(1);
      case "normal":
        BenchmarkColumnSchema normalSchema = BenchmarkColumnSchema.makeNormal(
            "",
            valueType,
            true,
            1,
            0.0,
            0.0,
            2.0,
            false
        );
        return normalSchema.makeGenerator(1);
      case "uniform":
        BenchmarkColumnSchema lazyUniformSchema = BenchmarkColumnSchema.makeContinuousUniform(
            "",
            valueType,
            true,
            1,
            0d,
            -(double) range / 2,
            (double) range / 2
        );
        return lazyUniformSchema.makeGenerator(1);
    }
    return null;
  }

  @Param({"3000000"})
  int rows;

  @Param({
      "lazyZipfLow",
      "lazyPiZipfLow",
      "lazyPiZipfHi",
      "nullp50PiZipfLow",
      "nullp99PiZipfLow",
      "normal",
      "uniform",
      "random"
  })
  String distribution;

  @Param("150000000")
  int cardinality;

  //  @Param({"float", "double"})
  @Param({"double"})
  String type;

  void initializeFloatValues() throws IOException
  {
    final String filename = "values-double-" + distribution + "-" + cardinality + "-" + rows + ".bin";
    final String tmpPath = "tmp/";
    final String dirPath = "tmp/doubleCompress/";
    File tmp = new File(tmpPath);
    tmp.mkdir();
    File dir = new File(dirPath);
    dir.mkdir();
    File dataFile = new File(dir, filename);

    floatVals = new float[rows];

    if (dataFile.exists()) {
      System.out.println("Data files already exist, re-using");
      try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
        int lineNum = 0;
        String line;
        while ((line = br.readLine()) != null) {
          floatVals[lineNum] = Float.parseFloat(line);
          if (floatVals[lineNum] < minValue) {
            minValue = floatVals[lineNum];
          }
          if (floatVals[lineNum] > maxValue) {
            maxValue = floatVals[lineNum];
          }
          lineNum++;
        }
      }
    } else {
      try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
        float min = 0;
        float max = (float) cardinality / 2;

        if (distribution.equals("random")) {
          for (int i = 0; i < floatVals.length; ++i) {
            floatVals[i] = min + rand.nextFloat() * (max - min);
            writer.write(floatVals[i] + "\n");
          }
        } else {
          BenchmarkColumnValueGenerator valueGenerator = makeGenerator(type, distribution, cardinality, rows);

          float floatPi = (float) Math.PI;
          for (int i = 0; i < floatVals.length; ++i) {
            float value = -1;
            Object rowValue = valueGenerator.generateRowValue();
            value = rowValue != null ? (float) rowValue : 0;
            if (distribution.contains("PiZipf")) {
              value = (floatPi - 2) * value;
            }
            floatVals[i] = value;
            if (floatVals[i] < minValue) {
              minValue = floatVals[i];
            }
            if (floatVals[i] > maxValue) {
              maxValue = floatVals[i];
            }
            writer.write(floatVals[i] + "\n");
          }
        }
      }
    }
  }

  void initializeDoubleValues() throws IOException
  {
    final String filename = "values-" + type + "-" + distribution + "-" + cardinality + "-" + rows + ".bin";
    final String tmpPath = "tmp/";
    final String dirPath = "tmp/doubleCompress/";
    File tmp = new File(tmpPath);
    tmp.mkdir();
    File dir = new File(dirPath);
    dir.mkdir();
    File dataFile = new File(dir, filename);

    doubleVals = new double[rows];

    if (dataFile.exists()) {
      System.out.println("Data files already exist, re-using");
      try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
        int lineNum = 0;
        String line;
        while ((line = br.readLine()) != null) {
          doubleVals[lineNum] = Double.parseDouble(line);
          if (doubleVals[lineNum] < minValue) {
            minValue = doubleVals[lineNum];
          }
          if (doubleVals[lineNum] > maxValue) {
            maxValue = doubleVals[lineNum];
          }
          lineNum++;
        }
      }
    } else {
      try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
        double min = 0;
        double max = (double) cardinality / 2;
        if (distribution.equals("random")) {
          for (int i = 0; i < doubleVals.length; ++i) {
            doubleVals[i] = min + rand.nextDouble() * (max - min);
            writer.write(doubleVals[i] + "\n");
          }
        } else {
          BenchmarkColumnValueGenerator valueGenerator = makeGenerator(type, distribution, cardinality, rows);

          for (int i = 0; i < doubleVals.length; ++i) {
            double value = 0;
            Object rowValue = valueGenerator.generateRowValue();
            value = rowValue != null ? (double) rowValue : 0;
            if (distribution.contains("PiZipf")) {
              value = (Math.PI - 2) * value;
            }
            doubleVals[i] = value;
            if (doubleVals[i] < minValue) {
              minValue = doubleVals[i];
            }
            if (doubleVals[i] > maxValue) {
              maxValue = doubleVals[i];
            }
            writer.write(doubleVals[i] + "\n");
          }
        }
      }
    }
  }
}
