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

import com.google.common.collect.ImmutableList;
import io.druid.segment.data.ColumnarDoubles;
import io.druid.segment.data.ColumnarDoublesSerializer;
import io.druid.segment.data.ColumnarFloats;
import io.druid.segment.data.ColumnarFloatsSerializer;
import io.druid.segment.data.CompressedColumnarDoublesSuppliers;
import io.druid.segment.data.CompressedColumnarFloatsSupplier;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Map;
import java.util.Random;

@State(Scope.Benchmark)
public class BaseColumnarDoublesBenchmark
{
  static int encodeFloatToFile(float[] floatVals, String encoding, FileChannel output) throws IOException
  {
    final SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    ColumnarFloatsSerializer serializer;
    switch (encoding) {
      case "lz4-little-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatlzle",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.LZ4
        );
        break;
      case "lzf-little-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatlzle",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.LZF
        );
        break;
      case "lz4-big-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatbele",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.LZ4
        );
        break;
      case "lzf-big-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatbele",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.LZF
        );
        break;
      case "unencoded-little-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatle",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.NONE
        );
        break;
      case "unencoded-big-endian":
        serializer = CompressionFactory.getFloatSerializer(
            writeOutMedium,
            "floatbe",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.NONE
        );
        break;
      default:
        throw new IllegalArgumentException("unknown encoding");
    }

    serializer.open();
    for (float val : floatVals) {
      serializer.add(val);
    }
    serializer.writeTo(output, null);
    return (int) serializer.getSerializedSize();
  }

  static int encodeDoubleToFile(double[] doubleVals, String encoding, FileChannel output) throws IOException
  {
    final SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium();
    ColumnarDoublesSerializer serializer;
    switch (encoding) {
      case "lz4-little-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublelzle",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.LZ4
        );
        break;
      case "lzf-little-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublelzle",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.LZF
        );
        break;
      case "lz4-big-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublelzbe",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.LZ4
        );
        break;
      case "lzf-big-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublelzbe",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.LZF
        );
        break;
      case "unencoded-little-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublele",
            ByteOrder.LITTLE_ENDIAN,
            CompressionStrategy.NONE
        );
        break;
      case "unencoded-big-endian":
        serializer = CompressionFactory.getDoubleSerializer(
            writeOutMedium,
            "doublebe",
            ByteOrder.BIG_ENDIAN,
            CompressionStrategy.NONE
        );
        break;
      default:
        throw new IllegalArgumentException("unknown encoding");
    }

    serializer.open();
    for (double val : doubleVals) {
      serializer.add(val);
    }
    serializer.writeTo(output, null);
    return (int) serializer.getSerializedSize();
  }

  static ColumnarFloats createColumnarFloats(String encoding, ByteBuffer buffer, int size) throws IOException
  {
    switch (encoding) {
      case "lzf-little-endian":
      case "lz4-little-endian":
      case "unencoded-little-endian":
        return CompressedColumnarFloatsSupplier.fromByteBuffer(buffer, ByteOrder.LITTLE_ENDIAN).get();
      case "lzf-big-endian":
      case "lz4-big-endian":
      case "unencoded-big-endian":
        return CompressedColumnarFloatsSupplier.fromByteBuffer(buffer, ByteOrder.BIG_ENDIAN).get();
    }
    throw new IllegalArgumentException("unknown encoding");
  }


  static ColumnarDoubles createColumnarDoubles(String encoding, ByteBuffer buffer, int size) throws IOException
  {
    switch (encoding) {
      case "lzf-little-endian":
      case "lz4-little-endian":
      case "unencoded-little-endian":
        return CompressedColumnarDoublesSuppliers.fromByteBuffer(buffer, ByteOrder.LITTLE_ENDIAN).get();
      case "lzf-big-endian":
      case "lz4-big-endian":
      case "unencoded-big-endian":
        return CompressedColumnarDoublesSuppliers.fromByteBuffer(buffer, ByteOrder.BIG_ENDIAN).get();
    }
    throw new IllegalArgumentException("unknown encoding");
  }

  // for debugging: validate that all encoders read the same values
  static void checkFloatSanity(Map<String, ColumnarFloats> encoders, ImmutableList<String> encodings, int rows)
      throws Exception
  {
    for (int i = 0; i < rows; i++) {
      checkFloatRowSanity(encoders, encodings, i);
    }
  }

  // for debugging: validate that all encoders read the same values
  static void checkDoubleSanity(Map<String, ColumnarDoubles> encoders, ImmutableList<String> encodings, int rows)
      throws Exception
  {
    for (int i = 0; i < rows; i++) {
      checkDoubleRowSanity(encoders, encodings, i);
    }
  }

  static void checkFloatRowSanity(Map<String, ColumnarFloats> encoders, ImmutableList<String> encodings, int row)
      throws Exception
  {
    if (encodings.size() > 1) {
      for (int i = 0; i < encodings.size() - 1; i++) {
        String currentKey = encodings.get(i);
        String nextKey = encodings.get(i + 1);
        ColumnarFloats current = encoders.get(currentKey);
        ColumnarFloats next = encoders.get(nextKey);
        float vCurrent = current.get(row);
        float vNext = next.get(row);
        if (vCurrent != vNext) {
          throw new Exception("values do not match " + currentKey + ":" + vCurrent + " " + nextKey + ":" + vNext);
        }
      }
    }
  }

  static void checkDoubleRowSanity(Map<String, ColumnarDoubles> encoders, ImmutableList<String> encodings, int row)
      throws Exception
  {
    if (encodings.size() > 1) {
      for (int i = 0; i < encodings.size() - 1; i++) {
        String currentKey = encodings.get(i);
        String nextKey = encodings.get(i + 1);
        ColumnarDoubles current = encoders.get(currentKey);
        ColumnarDoubles next = encoders.get(nextKey);
        double vCurrent = current.get(row);
        double vNext = next.get(row);
        if (vCurrent != vNext) {
          throw new Exception("values do not match " + currentKey + ":" + vCurrent + " " + nextKey + ":" + vNext);
        }
      }
    }
  }

  @Param({"lz4-little-endian", "lz4-big-endian", "lzf-little-endian", "lzf-big-endian", "unencoded-big-endian"})
  String encoding;

  Random rand = new Random(0);

  float[] floatVals;
  double[] doubleVals;

  double minValue;
  double maxValue;
  BitSet filter;

  void setupFilters(int rows, double filteredRowCountPercetnage)
  {
    // todo: save and read from file for stable filter set..
    // todo: also maybe filter set distributions to simulate different select patterns?
    // (because benchmarks don't take long enough already..)
    filter = null;
    final int filteredRowCount = (int) Math.floor(rows * filteredRowCountPercetnage);

    if (filteredRowCount < rows) {
      // setup bitset filter
      filter = new BitSet();
      for (int i = 0; i < filteredRowCount; i++) {
        int rowToAccess = rand.nextInt(rows);
        // Skip already selected rows if any
        while (filter.get(rowToAccess)) {
          rowToAccess = (rowToAccess + 1) % rows;
        }
        filter.set(rowToAccess);
      }
    }
  }
}
