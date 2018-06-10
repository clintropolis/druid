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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Set;

@State(Scope.Benchmark)
public class BaseColumnarIntsFromSegmentsBenchmark extends BaseColumnarIntsBenchmark
{
  //CHECKSTYLE.OFF: Regexp
  // wiki columns
  @Param({
      "channel",
      "cityName",
      "comment",
      "commentLength",
      "countryIsoCode",
      "countryName",
      "deltaBucket",
      "diffUrl",
      "flags",
      "isAnonymous",
      "isMinor",
      "isNew",
      "isRobot",
      "isUnpatrolled",
      "metroCode",
      "namespace",
      "page",
      "regionIsoCode",
      "regionName",
      "user"
  })

  // twitter columns
//  @Param({
//    "geo",
//    "lang",
//    "retweet",
//    "screen_name",
//    "source",
//    "text",
//    "utc_offset",
//    "verified"
//  })

  // clarity columns
//  @Param({
//    "bufferpoolName",
//    "clarityTopic",
//    "clarityUser",
//    "context",
//    "dataSource",
//    "description",
//    "dimension",
//    "duration",
//    "feed",
//    "gcGen",
//    "gcGenSpaceName",
//    "gcName",
//    "hasFilters",
//    "host",
//    "id",
//    "identity",
//    "implyCluster",
//    "implyDruidVersion",
//    "implyNodeType",
//    "implyVersion",
//    "memKind",
//    "memcached txt",
//    "metric",
//    "numComplexMetrics",
//    "numDimensions",
//    "numMetrics",
//    "poolKind",
//    "poolName",
//    "priority",
//    "remoteAddr",
//    "remoteAddress",
//    "server",
//    "service",
//    "severity",
//    "success",
//    "taskId",
//    "taskStatus",
//    "taskType",
//    "threshold",
//    "tier",
//    "type",
//    "version"
//  })

  // lineitem columns
//  @Param({
//    "l_comment",
//    "l_commitdate",
//    "l_linenumber",
//    "l_linestatus",
//    "l_orderkey",
//    "l_partkey",
//    "l_receiptdate",
//    "l_returnflag",
//    "l_shipinstruct",
//    "l_shipmode",
//    "l_suppkey"
//  })
  String columnName;

  @Param({"533652"})        // wiki
//  @Param({"3259585"})       // twitter
//  @Param({"3783642"})       // clarity
//  @Param({"6001215"})         // tpch-lineitem-1g
  int rows;


  @Param({"tmp/segments/wiki-1/"})
//  @Param({"tmp/segments/twitter-1/"})
//  @Param({"tmp/segments/clarity-1/"})
//  @Param({"tmp/segments/tpch-lineitem-1/"})
  String segmentPath;

  @Param({"wikiticker"})
//  @Param({"twitter"})
//  @Param({"clarity"})
//  @Param({"tpch-lineitem"})
  String segmentName;


  private static IndexIO INDEX_IO;
  public static ObjectMapper JSON_MAPPER;

  //CHECKSTYLE.ON: Regexp

  /**
   * read column intermediary values into integer array
   * @throws IOException
   */
  void initializeValues() throws IOException
  {
    initializeSegmentValueIntermediaryFile();
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    ArrayList<Integer> values = Lists.newArrayList();
    try (BufferedReader br = Files.newBufferedReader(dataFile.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = br.readLine()) != null) {
        int value = Integer.parseInt(line);
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

    vals = values.stream().mapToInt(i -> i).toArray();
  }

  String getColumnDataFileName(String segmentName, String columnName)
  {
    return StringUtils.format("%s-ints-%s.txt", segmentName, columnName);
  }

  String getColumnEncodedFileName(String encoding, String segmentName, String columnName)
  {
    return StringUtils.format("%s-%s-ints-%s.bin", encoding, segmentName, columnName);
  }

  File getTmpDir()
  {
    final String dirPath = StringUtils.format("tmp/encoding/%s", segmentName);
    File dir = new File(dirPath);
    dir.mkdirs();
    return dir;
  }

  /**
   * writes column values to text file, 1 per line
   * @throws IOException
   */
  void initializeSegmentValueIntermediaryFile() throws IOException
  {
    File dir = getTmpDir();
    File dataFile = new File(dir, getColumnDataFileName(segmentName, columnName));

    if (!dataFile.exists()) {
      JSON_MAPPER = new DefaultObjectMapper();
      INDEX_IO = new IndexIO(
          JSON_MAPPER,
          OffHeapMemorySegmentWriteOutMediumFactory.instance(),
          () -> 0
      );
      try (final QueryableIndex index = INDEX_IO.loadIndex(new File(segmentPath))) {
        final Set<String> columnNames = Sets.newLinkedHashSet();
        columnNames.add(Column.TIME_COLUMN_NAME);
        Iterables.addAll(columnNames, index.getColumnNames());
        final Column column = index.getColumn(columnName);
        final ColumnCapabilities capabilities = column.getCapabilities();
        final ValueType columnType = capabilities.getType();
        try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
          if (columnType != ValueType.STRING) {
            throw new RuntimeException("Invalid column type, expected 'String'");
          }
          DictionaryEncodedColumn<String> theColumn = column.getDictionaryEncoding();

          if (theColumn.hasMultipleValues()) {
            throw new RuntimeException("multi-int benchmarks are not current supported");
          }

          for (int i = 0; i < theColumn.length(); i++) {
            int value = theColumn.getSingleValueRow(i);
            writer.write(value + "\n");
          }
        }
      }
    }
  }
}
