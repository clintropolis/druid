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

package io.druid.benchmark;

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.io.Closer;
import io.druid.segment.IndexSpec;
import io.druid.segment.data.ColumnarInts;
import io.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ShapeShiftingColumnarIntsSerializer;
import io.druid.segment.data.ShapeShiftingColumnarIntsSupplier;
import io.druid.segment.data.VSizeColumnarInts;
import io.druid.segment.data.codecs.ints.BytePackedIntFormEncoder;
import io.druid.segment.data.codecs.ints.CompressedIntFormEncoder;
import io.druid.segment.data.codecs.ints.CompressibleIntFormEncoder;
import io.druid.segment.data.codecs.ints.ConstantIntFormEncoder;
import io.druid.segment.data.codecs.ints.IntCodecs;
import io.druid.segment.data.codecs.ints.IntFormEncoder;
import io.druid.segment.data.codecs.ints.LemireIntFormEncoder;
import io.druid.segment.data.codecs.ints.RunLengthBytePackedIntFormEncoder;
import io.druid.segment.data.codecs.ints.UnencodedIntFormEncoder;
import io.druid.segment.data.codecs.ints.ZeroIntFormEncoder;
import io.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import it.unimi.dsi.fastutil.ints.IntArrayList;
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
public class BaseColumnarIntsBenchmark
{
  static int encodeToFile(int[] vals, int minValue, int maxValue, String encoding, FileChannel output)
      throws IOException
  {
    int numBytes = VSizeColumnarInts.getNumBytesForMax(maxValue);

    IndexSpec.ShapeShiftingBlockSize blockSizeEnum = encoding.endsWith("-13")
                                                ? IndexSpec.ShapeShiftingBlockSize.MIDDLE
                                                : encoding.endsWith("-12")
                                                  ? IndexSpec.ShapeShiftingBlockSize.SMALL
                                                  : IndexSpec.ShapeShiftingBlockSize.LARGE;
    byte blockSize = (byte) (blockSizeEnum.getLogBlockSize() - 2);
    IndexSpec.ShapeShiftOptimizationTarget optimizationTarget =
        IndexSpec.ShapeShiftOptimizationTarget.FASTBUTSMALLISH;


    try (SegmentWriteOutMedium writeOutMedium = new OnHeapMemorySegmentWriteOutMedium()) {

      ByteBuffer uncompressedDataBuffer =
          CompressionStrategy.LZ4.getCompressor()
                                 .allocateInBuffer(8 + ((1 << blockSize) * Integer.BYTES), writeOutMedium.getCloser())
                                 .order(ByteOrder.LITTLE_ENDIAN);
      ByteBuffer compressedDataBuffer =
          CompressionStrategy.LZ4.getCompressor()
                                 .allocateOutBuffer(
                                     ((1 << blockSize) * Integer.BYTES) + 1024,
                                     writeOutMedium.getCloser()
                                 );
      switch (encoding) {
        case "vsize-byte":
          final VSizeColumnarInts vsize = VSizeColumnarInts.fromArray(vals);
          vsize.writeTo(output, null);
          return (int) vsize.getSerializedSize();
        case "compressed-vsize-byte":
          final CompressedVSizeColumnarIntsSupplier compressed = CompressedVSizeColumnarIntsSupplier.fromList(
              IntArrayList.wrap(vals),
              Math.max(maxValue - 1, 1),
              CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(numBytes),
              ByteOrder.nativeOrder(),
              CompressionStrategy.LZ4,
              Closer.create()
          );
          compressed.writeTo(output, null);
          return (int) compressed.getSerializedSize();
        case "compressed-vsize-big-endian":
          final CompressedVSizeColumnarIntsSupplier compressedBigEndian = CompressedVSizeColumnarIntsSupplier.fromList(
              IntArrayList.wrap(vals),
              Math.max(maxValue - 1, 1),
              CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForBytes(numBytes),
              ByteOrder.BIG_ENDIAN,
              CompressionStrategy.LZ4,
              Closer.create()
          );
          compressedBigEndian.writeTo(output, null);
          return (int) compressedBigEndian.getSerializedSize();
        case "shapeshift-unencoded":
          final IntFormEncoder[] ssucodecs = new IntFormEncoder[]{
              new UnencodedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN
              )
          };
          final ShapeShiftingColumnarIntsSerializer ssunencodedSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  ssucodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          ssunencodedSerializer.open();
          for (int val : vals) {
            ssunencodedSerializer.addValue(val);
          }
          ssunencodedSerializer.writeTo(output, null);
          return (int) ssunencodedSerializer.getSerializedSize();
        case "shapeshift-bytepack":
          final IntFormEncoder[] ssbytepackcodecs = new IntFormEncoder[]{
              new BytePackedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN
              )
          };
          final ShapeShiftingColumnarIntsSerializer ssbytepackSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  ssbytepackcodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          ssbytepackSerializer.open();
          for (int val : vals) {
            ssbytepackSerializer.addValue(val);
          }
          ssbytepackSerializer.writeTo(output, null);
          return (int) ssbytepackSerializer.getSerializedSize();
        case "shapeshift-rle-bytepack":
          final IntFormEncoder[] ssrbytepackcodecs = new IntFormEncoder[]{
              new RunLengthBytePackedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN
              )
          };
          final ShapeShiftingColumnarIntsSerializer ssrbytepackSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  ssrbytepackcodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          ssrbytepackSerializer.open();
          for (int val : vals) {
            ssrbytepackSerializer.addValue(val);
          }
          ssrbytepackSerializer.writeTo(output, null);
          return (int) ssrbytepackSerializer.getSerializedSize();
        case "shapeshift-lz4-bytepack":
          final IntFormEncoder[] sslzcodecs = new IntFormEncoder[]{
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  new BytePackedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
                  compressedDataBuffer,
                  uncompressedDataBuffer
              )
          };
          final ShapeShiftingColumnarIntsSerializer sslzSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  sslzcodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          sslzSerializer.open();
          for (int val : vals) {
            sslzSerializer.addValue(val);
          }
          sslzSerializer.writeTo(output, null);
          return (int) sslzSerializer.getSerializedSize();
        case "shapeshift-lz4-rle-bytepack":
          final IntFormEncoder[] sslzrlecodecs = new IntFormEncoder[]{
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  new RunLengthBytePackedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
                  compressedDataBuffer,
                  uncompressedDataBuffer
              )
          };
          final ShapeShiftingColumnarIntsSerializer sslzrleSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  sslzrlecodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          sslzrleSerializer.open();
          for (int val : vals) {
            sslzrleSerializer.addValue(val);
          }
          sslzrleSerializer.writeTo(output, null);
          return (int) sslzrleSerializer.getSerializedSize();
        case "shapeshift-fastpfor":
          final IntFormEncoder[] dfastcodecs = new IntFormEncoder[]{
              new LemireIntFormEncoder(
                  blockSize,
                  IntCodecs.FASTPFOR,
                  "fastpfor",
                  ByteOrder.LITTLE_ENDIAN
              )
          };
          final ShapeShiftingColumnarIntsSerializer ssfastPforSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  dfastcodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          ssfastPforSerializer.open();
          for (int val : vals) {
            ssfastPforSerializer.addValue(val);
          }
          ssfastPforSerializer.writeTo(output, null);
          return (int) ssfastPforSerializer.getSerializedSize();
        case "shapeshift":
        case "shapeshift-13":
        case "shapeshift-12":
        case "shapeshift-lazy":
        case "shapeshift-eager":
        case "shapeshift-faster":
        case "shapeshift-faster-13":
        case "shapeshift-faster-12":
        case "shapeshift-smaller":
        case "shapeshift-smaller-13":
        case "shapeshift-smaller-12":
          final CompressibleIntFormEncoder rle = new RunLengthBytePackedIntFormEncoder(
              blockSize,
              ByteOrder.LITTLE_ENDIAN
          );
          final CompressibleIntFormEncoder bytepack = new BytePackedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN);
          final IntFormEncoder[] sscodecs = new IntFormEncoder[]{
              new ZeroIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
              new ConstantIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
              new UnencodedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
              rle,
              bytepack,
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  rle,
                  compressedDataBuffer,
                  uncompressedDataBuffer
              ),
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  bytepack,
                  compressedDataBuffer,
                  uncompressedDataBuffer
              ),
              new LemireIntFormEncoder(
                  blockSize,
                  IntCodecs.FASTPFOR,
                  "fastpfor",
                  ByteOrder.LITTLE_ENDIAN
              )
          };
          final ShapeShiftingColumnarIntsSerializer ssSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  sscodecs,
                  encoding.contains("shapeshift-smaller")
                  ? IndexSpec.ShapeShiftOptimizationTarget.SMALLER
                  : encoding.contains("shapeshift-faster")
                    ? IndexSpec.ShapeShiftOptimizationTarget.FASTER
                    : optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          ssSerializer.open();
          for (int val : vals) {
            ssSerializer.addValue(val);
          }
          ssSerializer.writeTo(output, null);
          return (int) ssSerializer.getSerializedSize();
        case "shapeshift-lz4-only":
          final IntFormEncoder[] sslzNewcodecs = new IntFormEncoder[]{
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  new RunLengthBytePackedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
                  compressedDataBuffer,
                  uncompressedDataBuffer
              ),
              new CompressedIntFormEncoder(
                  blockSize,
                  ByteOrder.LITTLE_ENDIAN,
                  CompressionStrategy.LZ4,
                  new BytePackedIntFormEncoder(blockSize, ByteOrder.LITTLE_ENDIAN),
                  compressedDataBuffer,
                  uncompressedDataBuffer
              ),
              };
          final ShapeShiftingColumnarIntsSerializer sslzNewSerializer =
              new ShapeShiftingColumnarIntsSerializer(
                  writeOutMedium,
                  sslzNewcodecs,
                  optimizationTarget,
                  blockSizeEnum,
                  ByteOrder.LITTLE_ENDIAN
              );
          sslzNewSerializer.open();
          for (int val : vals) {
            sslzNewSerializer.addValue(val);
          }
          sslzNewSerializer.writeTo(output, null);
          return (int) sslzNewSerializer.getSerializedSize();
      }
      throw new IllegalArgumentException("unknown encoding");
    }
  }

  static ColumnarInts createIndexedInts(String encoding, ByteBuffer buffer, int size)
  {
    switch (encoding) {
      case "vsize-byte":
        return VSizeColumnarInts.readFromByteBuffer(buffer);
      case "compressed-vsize-byte":
        return CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder()).get();
      case "compressed-vsize-big-endian":
        return CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, ByteOrder.BIG_ENDIAN).get();
      case "shapeshift":
      case "shapeshift-unencoded":
      case "shapeshift-fastpfor":
      case "shapeshift-bytepack":
      case "shapeshift-lz4-bytepack":
      case "shapeshift-rle-bytepack":
      case "shapeshift-lz4-rle-bytepack":
      case "shapeshift-lz4-only":
      case "shapeshift-smaller":
      case "shapeshift-faster":
      case "shapeshift-13":
      case "shapeshift-smaller-13":
      case "shapeshift-faster-13":
      case "shapeshift-12":
      case "shapeshift-smaller-12":
      case "shapeshift-faster-12":
        return ShapeShiftingColumnarIntsSupplier.fromByteBuffer(buffer, ByteOrder.LITTLE_ENDIAN).get();
      case "shapeshift-lazy":
        return ShapeShiftingColumnarIntsSupplier.fromByteBuffer(
            buffer,
            ByteOrder.LITTLE_ENDIAN,
            ShapeShiftingColumnarIntsSupplier.ShapeShiftingColumnarIntsDecodeOptimization.MIXED
        ).get();
      case "shapeshift-eager":
        return ShapeShiftingColumnarIntsSupplier.fromByteBuffer(
            buffer,
            ByteOrder.LITTLE_ENDIAN,
            ShapeShiftingColumnarIntsSupplier.ShapeShiftingColumnarIntsDecodeOptimization.BLOCK
        ).get();
    }
    throw new IllegalArgumentException("unknown encoding");
  }

  // for debugging: validate that all encoders read the same values
  static void checkSanity(Map<String, ColumnarInts> encoders, ImmutableList<String> encodings, int rows)
      throws Exception
  {
    for (int i = 0; i < rows; i++) {
      checkRowSanity(encoders, encodings, i);
    }
  }

  static void checkRowSanity(Map<String, ColumnarInts> encoders, ImmutableList<String> encodings, int row)
      throws Exception
  {
    if (encodings.size() > 1) {
      for (int i = 0; i < encodings.size() - 1; i++) {
        String currentKey = encodings.get(i);
        String nextKey = encodings.get(i + 1);
        IndexedInts current = encoders.get(currentKey);
        IndexedInts next = encoders.get(nextKey);
        int vCurrent = current.get(row);
        int vNext = next.get(row);
        if (vCurrent != vNext) {
          throw new Exception("values do not match at row "
                              + row
                              + " - "
                              + currentKey
                              + ":"
                              + vCurrent
                              + " "
                              + nextKey
                              + ":"
                              + vNext);
        }
      }
    }
  }

  //@Param({"shapeshift-bytepack", "shapeshift-rle-bytepack", "shapeshift-fastpfor", "shapeshift-lz4-bytepack", "shapeshift-lz4-rle-bytepack", "compressed-vsize-byte"})
  @Param({"shapeshift", "compressed-vsize-byte"})
  String encoding;

  Random rand = new Random(0);

  int[] vals;

  int minValue;
  int maxValue;
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
