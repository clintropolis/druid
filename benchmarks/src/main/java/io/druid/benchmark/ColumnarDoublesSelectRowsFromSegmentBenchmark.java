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

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.segment.data.ColumnarDoubles;
import io.druid.segment.data.ColumnarFloats;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class ColumnarDoublesSelectRowsFromSegmentBenchmark extends BaseColumnarDoublesFromSegmentsBenchmark
{
  private Map<String, ColumnarFloats> floatData;
  private Map<String, ColumnarDoubles> doubleData;
  private Map<String, Integer> encodedSize;

  // Number of rows to read, the test will read random rows
  @Param({"0.01", "0.1", "0.33", "0.66", "0.95", "1.0"})
  private double filteredRowCountPercentage;

  @Setup
  public void setup() throws Exception
  {
    floatData = Maps.newHashMap();
    doubleData = Maps.newHashMap();
    encodedSize = Maps.newHashMap();

    setupFilters(rows, filteredRowCountPercentage);
    setupFromFile(encoding);

    // uncomment me if worried that encoders are not all equal
    //CHECKSTYLE.OFF: Regexp
//    ImmutableList<String> all = ImmutableList.of("lz-big-endian", "lz-little-endian");
//    for (String _enc : all) {
//      setupFromFile(_enc);
//    }

//    checkDoubleSanity(doubleData, all, rows);
    //CHECKSTYLE.ON: Regexp
  }

  private void setupFromFile(String encoding) throws IOException
  {
    String dirPath = "tmp/segCompress";
    File dir = new File(dirPath);
    File compFile = new File(dir, encoding + "-" + fileName.substring(0, fileName.indexOf('.')) + ".bin");
    ByteBuffer buffer = Files.map(compFile);

    int size = (int) compFile.length();
    encodedSize.put(encoding, size);

    if (fileName.contains("float")) {
      ColumnarFloats data = createColumnarFloats(encoding, buffer, size);
      floatData.put(encoding, data);
    } else {
      ColumnarDoubles data = createColumnarDoubles(encoding, buffer, size);
      doubleData.put(encoding, data);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void selectRows(Blackhole blackhole)
  {
    EncodingSizeProfiler.encodedSize = encodedSize.get(encoding);
    if (fileName.contains("float")) {
      ColumnarFloats encoder = floatData.get(encoding);
      if (filter == null) {
        for (int i = 0; i < rows; i++) {
          blackhole.consume(encoder.get(i));
        }
      } else {
        for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
          blackhole.consume(encoder.get(i));
        }
      }
    } else {
      ColumnarDoubles data = doubleData.get(encoding);
      if (filter == null) {
        for (int i = 0; i < rows; i++) {
          blackhole.consume(data.get(i));
        }
      } else {
        for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
          blackhole.consume(data.get(i));
        }
      }
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    System.out.println("main happened");
    Options opt = new OptionsBuilder()
        .include(ColumnarDoublesSelectRowsFromSegmentBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-doubles-select-speed-segments.csv")
        .build();

    new Runner(opt).run();
  }
}
