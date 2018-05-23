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
public class BaseColumnarIntsFromSegmentsBenchmark extends BaseColumnarIntsBenchmark
{
  //CHECKSTYLE.OFF: Regexp
//  @Param({
//      "batch-twitter-values-int-geo.txt",
////      "batch-twitter-values-int-hashtags.txt", multivalue?
//      "batch-twitter-values-int-lang.txt",
//      "batch-twitter-values-int-retweet.txt",
//      "batch-twitter-values-int-screen_name.txt",
//      "batch-twitter-values-int-source.txt",
//      "batch-twitter-values-int-text.txt",
//      "batch-twitter-values-int-utc_offset.txt",
//      "batch-twitter-values-int-verified.txt"
//  })

//  @Param({
//      "batch-wiki-values-int-channel.txt",
//      "batch-wiki-values-int-cityName.txt",
//      "batch-wiki-values-int-comment.txt",
//      "batch-wiki-values-int-commentLength.txt",
//      "batch-wiki-values-int-countryIsoCode.txt",
//      "batch-wiki-values-int-countryName.txt",
//      "batch-wiki-values-int-deltaBucket.txt",
//      "batch-wiki-values-int-diffUrl.txt",
//      "batch-wiki-values-int-flags.txt",
//      "batch-wiki-values-int-isAnonymous.txt",
//      "batch-wiki-values-int-isMinor.txt",
//      "batch-wiki-values-int-isNew.txt",
//      "batch-wiki-values-int-isRobot.txt",
//      "batch-wiki-values-int-isUnpatrolled.txt",
//      "batch-wiki-values-int-metroCode.txt",
//      "batch-wiki-values-int-namespace.txt",
//      "batch-wiki-values-int-page.txt",
//      "batch-wiki-values-int-regionIsoCode.txt",
//      "batch-wiki-values-int-regionName.txt",
//      "batch-wiki-values-int-user.txt"
//  })

//  @Param({
//      "compacted-clarity-values-int-bufferpoolName.txt",
//      "compacted-clarity-values-int-clarityTopic.txt",
//      "compacted-clarity-values-int-clarityUser.txt",
//      "compacted-clarity-values-int-context.txt",
//      "compacted-clarity-values-int-dataSource.txt",
//      "compacted-clarity-values-int-description.txt",
//      "compacted-clarity-values-int-dimension.txt",
//      "compacted-clarity-values-int-duration.txt",
//      "compacted-clarity-values-int-feed.txt",
//      "compacted-clarity-values-int-gcGen.txt",
//      "compacted-clarity-values-int-gcGenSpaceName.txt",
//      "compacted-clarity-values-int-gcName.txt",
//      "compacted-clarity-values-int-hasFilters.txt",
//      "compacted-clarity-values-int-host.txt",
//      "compacted-clarity-values-int-id.txt",
//      "compacted-clarity-values-int-identity.txt",
//      "compacted-clarity-values-int-implyCluster.txt",
//      "compacted-clarity-values-int-implyDruidVersion.txt",
//      "compacted-clarity-values-int-implyNodeType.txt",
//      "compacted-clarity-values-int-implyVersion.txt",
//      "compacted-clarity-values-int-memKind.txt",
//      "compacted-clarity-values-int-memcached metric.txt",
//      "compacted-clarity-values-int-metric.txt",
//      "compacted-clarity-values-int-numComplexMetrics.txt",
//      "compacted-clarity-values-int-numDimensions.txt",
//      "compacted-clarity-values-int-numMetrics.txt",
//      "compacted-clarity-values-int-poolKind.txt",
//      "compacted-clarity-values-int-poolName.txt",
//      "compacted-clarity-values-int-priority.txt",
//      "compacted-clarity-values-int-remoteAddr.txt",
//      "compacted-clarity-values-int-remoteAddress.txt",
//      "compacted-clarity-values-int-server.txt",
//      "compacted-clarity-values-int-service.txt",
//      "compacted-clarity-values-int-severity.txt",
//      "compacted-clarity-values-int-success.txt",
//      "compacted-clarity-values-int-taskId.txt",
//      "compacted-clarity-values-int-taskStatus.txt",
//      "compacted-clarity-values-int-taskType.txt",
//      "compacted-clarity-values-int-threshold.txt",
//      "compacted-clarity-values-int-tier.txt",
//      "compacted-clarity-values-int-type.txt",
//      "compacted-clarity-values-int-version.txt"
//  })


  @Param({
      "tpch-lineitem-1g-values-int-l_comment.txt",
      "tpch-lineitem-1g-values-int-l_commitdate.txt",
      "tpch-lineitem-1g-values-int-l_linenumber.txt",
      "tpch-lineitem-1g-values-int-l_linestatus.txt",
      "tpch-lineitem-1g-values-int-l_orderkey.txt",
      "tpch-lineitem-1g-values-int-l_partkey.txt",
      "tpch-lineitem-1g-values-int-l_receiptdate.txt",
      "tpch-lineitem-1g-values-int-l_returnflag.txt",
      "tpch-lineitem-1g-values-int-l_shipinstruct.txt",
      "tpch-lineitem-1g-values-int-l_shipmode.txt",
      "tpch-lineitem-1g-values-int-l_suppkey.txt"
  })

  String fileName;

//  @Param({"3259585"})       // twitter
//  @Param({"533652"})        // wiki
//  @Param({"3783642"})       // clarity
  @Param({"6001215"})         // tpch-lineitem-1g
  int rows;


  //CHECKSTYLE.ON: Regexp
  void initializeValues() throws IOException
  {
    final String tmpPath = "tmp/";
    final String dirPath = "tmp/segCompress/";
    File tmp = new File(tmpPath);
    tmp.mkdir();
    File dir = new File(dirPath);
    dir.mkdir();
    File dataFile = new File(dir, fileName);

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
}
