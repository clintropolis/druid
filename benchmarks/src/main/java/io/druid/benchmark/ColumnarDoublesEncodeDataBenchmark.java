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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class ColumnarDoublesEncodeDataBenchmark extends BaseColumnarDoublesFromGeneratorBenchmark
{
  @Setup
  public void setup() throws Exception
  {
    if (type.equals("double")) {
      initializeDoubleValues();
    } else {
      initializeFloatValues();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void encodeColumn(Blackhole blackhole) throws IOException
  {
    final String filename = encoding + "-" + type + "-" + distribution + "-" + rows + "-" + cardinality + ".bin";
    final String tmpPath = "tmp/";
    final String dirPath = "tmp/doubleCompress/";
    File tmp = new File(tmpPath);
    tmp.mkdir();
    File dir = new File(dirPath);
    dir.mkdir();
    File columnDataFile = new File(dir, filename);
    columnDataFile.delete();
    FileChannel output =
        FileChannel.open(columnDataFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

    int size = -1;
    if (type.equals("double")) {
      size = encodeDoubleToFile(doubleVals, encoding, output);
    } else {
      size = encodeFloatToFile(floatVals, encoding, output);
    }
    EncodingSizeProfiler.encodedSize = size;
    blackhole.consume(size);
    output.close();
  }

  public static void main(String[] args) throws RunnerException
  {
    System.out.println("main happened");
    Options opt = new OptionsBuilder()
        .include(ColumnarDoublesEncodeDataBenchmark.class.getSimpleName())
        .addProfiler(EncodingSizeProfiler.class)
        .resultFormat(ResultFormatType.CSV)
        .result("column-doubles-encode-speed.csv")
        .build();

    new Runner(opt).run();
  }
}
