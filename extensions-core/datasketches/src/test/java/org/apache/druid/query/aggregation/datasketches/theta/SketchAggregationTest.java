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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByTestColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
@RunWith(Parameterized.class)
public class SketchAggregationTest
{
  private final AggregationTestHelper helper;
  private final QueryContexts.Vectorize vectorize;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final Closer closer;

  public SketchAggregationTest(final GroupByQueryConfig config, final String vectorize)
  {
    SketchModule.registerSerde();
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new SketchModule().getJacksonModules(),
        config,
        tempFolder
    );
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.closer = Closer.create();
  }

  @Parameterized.Parameters(name = "config = {0}, vectorize = {1}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "force"}) {
        constructors.add(new Object[]{config, vectorize});
      }
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
    helper.close();
  }

  @Test
  public void testSketchDataIngestAndGpByQuery() throws Exception
  {
    final GroupByQuery groupByQuery =
        readQueryFromClasspath("sketch_test_data_group_by_query.json", helper.getObjectMapper(), vectorize);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        readFileFromClasspathAsString("sketch_test_data_record_parser.json"),
        readFileFromClasspathAsString("sketch_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        groupByQuery
    );

    final String expectedSummary = "\n### HeapCompactSketch SUMMARY: \n"
                                   + "   Estimate                : 50.0\n"
                                   + "   Upper Bound, 95% conf   : 50.0\n"
                                   + "   Lower Bound, 95% conf   : 50.0\n"
                                   + "   Theta (double)          : 1.0\n"
                                   + "   Theta (long)            : 9223372036854775807\n"
                                   + "   Theta (long) hex        : 7fffffffffffffff\n"
                                   + "   EstMode?                : false\n"
                                   + "   Empty?                  : false\n"
                                   + "   Ordered?                : true\n"
                                   + "   Retained Entries        : 50\n"
                                   + "   Seed Hash               : 93cc | 37836\n"
                                   + "### END SKETCH SUMMARY\n";
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("sids_sketch_count", 50.0)
                    .put(
                        "sids_sketch_count_with_err",
                        new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2)
                    )
                    .put("sketchEstimatePostAgg", 50.0)
                    .put(
                        "sketchEstimatePostAggWithErrorBounds",
                        new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2)
                    )
                    .put("sketchUnionPostAggEstimate", 50.0)
                    .put("sketchSummary", expectedSummary)
                    .put("sketchIntersectionPostAggEstimate", 50.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            groupByQuery
        ),
        results.get(0)
    );
  }

  @Test
  public void testEmptySketchAggregateCombine() throws Exception
  {
    final GroupByQuery groupByQuery =
        readQueryFromClasspath("empty_sketch_group_by_query.json", helper.getObjectMapper(), vectorize);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("empty_sketch_data.tsv").getFile()),
        readFileFromClasspathAsString("empty_sketch_data_record_parser.json"),
        readFileFromClasspathAsString("empty_sketch_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        5,
        groupByQuery
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2019-07-14T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_b")
                    .put("sketch_count", 0.0)
                    .build()
            ),
            groupByQuery
        ),
        results.get(0)
    );
  }

  @Test
  public void testThetaCardinalityOnSimpleColumn() throws Exception
  {
    final GroupByQuery groupByQuery =
        readQueryFromClasspath("simple_test_data_group_by_query.json", helper.getObjectMapper(), vectorize);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser2.json"),
        "["
        + "  {"
        + "    \"type\": \"count\","
        + "    \"name\": \"count\""
        + "  }"
        + "]",
        0,
        Granularities.NONE,
        1000,
        groupByQuery
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_3")
                    .put("sketch_count", 38.0)
                    .put("sketchEstimatePostAgg", 38.0)
                    .put("sketchUnionPostAggEstimate", 38.0)
                    .put("sketchIntersectionPostAggEstimate", 38.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_2")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_4")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_5")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
        ).stream().map(row -> ResultRow.fromLegacyRow(row, groupByQuery)).collect(Collectors.toList()),
        results
    );
  }

  @Test
  public void testSketchMergeAggregatorFactorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, null, null, null, false));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, false, true, null, false));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, true, false, null, false));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, true, false, 2, false));
  }

  @Test
  public void testSketchMergeFinalization()
  {
    SketchHolder sketch = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(128).build());

    SketchMergeAggregatorFactory agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, null, null, null, false);
    Assert.assertEquals(0.0, ((Double) agg.finalizeComputation(sketch)).doubleValue(), 0.0001);

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, true, null, null, false);
    Assert.assertEquals(0.0, ((Double) agg.finalizeComputation(sketch)).doubleValue(), 0.0001);

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, false, null, null, false);
    Assert.assertEquals(sketch, agg.finalizeComputation(sketch));

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, true, null, 2, false);
    SketchEstimateWithErrorBounds est = (SketchEstimateWithErrorBounds) agg.finalizeComputation(sketch);
    Assert.assertEquals(0.0, est.getEstimate(), 0.0001);
    Assert.assertEquals(0.0, est.getHighBound(), 0.0001);
    Assert.assertEquals(0.0, est.getLowBound(), 0.0001);
    Assert.assertEquals(2, est.getNumStdDev());

  }

  @Test
  public void testArrays() throws Exception
  {
    AggregatorFactory[] aggs = new AggregatorFactory[]{
        new SketchMergeAggregatorFactory(
            "sketch0",
            "arrayString",
            null,
            null,
            null,
            null,
            true
        ),
        new SketchMergeAggregatorFactory(
            "sketch1",
            "arrayLong",
            null,
            null,
            null,
            null,
            true
        ),
        new SketchMergeAggregatorFactory(
            "sketch2",
            "arrayDouble",
            null,
            null,
            null,
            null,
            true
        ),
        new SketchMergeAggregatorFactory(
            "sketch3",
            "arrayString",
            null,
            null,
            null,
            null,
            false
        ),
        new SketchMergeAggregatorFactory(
            "sketch4",
            "arrayLong",
            null,
            null,
            null,
            null,
            false
        ),
        new SketchMergeAggregatorFactory(
            "sketch5",
            "arrayDouble",
            null,
            null,
            null,
            null,
            false
        )
    };
    IndexBuilder bob = IndexBuilder.create(helper.getObjectMapper())
                                   .tmpDir(tempFolder.newFolder())
                                   .schema(
                                       IncrementalIndexSchema.builder()
                                                             .withTimestampSpec(NestedDataTestUtils.TIMESTAMP_SPEC)
                                                             .withDimensionsSpec(NestedDataTestUtils.AUTO_DISCOVERY)
                                                             .withMetrics(aggs)
                                                             .withQueryGranularity(Granularities.NONE)
                                                             .withRollup(true)
                                                             .withMinTimestamp(0)
                                                             .build()
                                   )
                                   .inputSource(
                                       ResourceInputSource.of(
                                           NestedDataTestUtils.class.getClassLoader(),
                                           NestedDataTestUtils.ARRAY_TYPES_DATA_FILE
                                       )
                                   )
                                   .inputFormat(NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT)
                                   .transform(TransformSpec.NONE)
                                   .inputTmpDir(tempFolder.newFolder());

    List<Segment> realtimeSegs = ImmutableList.of(
        new IncrementalIndexSegment(bob.buildIncrementalIndex(), SegmentId.dummy("test_datasource"))
    );
    List<Segment> segs = ImmutableList.of(
        new QueryableIndexSegment(bob.buildMMappedMergedIndex(), SegmentId.dummy("test_datasource"))
    );

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("test_datasource")
                                     .setGranularity(Granularities.ALL)
                                     .setInterval(Intervals.ETERNITY)
                                     .setAggregatorSpecs(
                                         new SketchMergeAggregatorFactory(
                                             "a0",
                                             "arrayString",
                                             null,
                                             null,
                                             null,
                                             null,
                                             true
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a1",
                                             "arrayLong",
                                             null,
                                             null,
                                             null,
                                             null,
                                             true
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a2",
                                             "arrayDouble",
                                             null,
                                             null,
                                             null,
                                             null,
                                             true
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a3",
                                             "sketch0",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a4",
                                             "sketch1",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a5",
                                             "sketch2",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a6",
                                             "sketch3",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a7",
                                             "sketch4",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new SketchMergeAggregatorFactory(
                                             "a8",
                                             "sketch5",
                                             null,
                                             null,
                                             true,
                                             null,
                                             false
                                         ),
                                         new CountAggregatorFactory("a9")
                                     )
                                     .setPostAggregatorSpecs(
                                         ImmutableList.of(
                                             new SketchEstimatePostAggregator(
                                                 "p0",
                                                 new FieldAccessPostAggregator("f0", "a0"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p1",
                                                 new FieldAccessPostAggregator("f1", "a1"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p2",
                                                 new FieldAccessPostAggregator("f2", "a2"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p3",
                                                 new FieldAccessPostAggregator("f3", "a3"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p4",
                                                 new FieldAccessPostAggregator("f4", "a4"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p5",
                                                 new FieldAccessPostAggregator("f5", "a5"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p6",
                                                 new FieldAccessPostAggregator("f6", "a6"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p7",
                                                 new FieldAccessPostAggregator("f7", "a7"),
                                                 null
                                             ),
                                             new SketchEstimatePostAggregator(
                                                 "p8",
                                                 new FieldAccessPostAggregator("f8", "a8"),
                                                 null
                                             )
                                         )
                                     )
                                     .build();

    Sequence<ResultRow> realtimeSeq = helper.runQueryOnSegmentsObjs(realtimeSegs, query);
    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segs, query);
    List<ResultRow> realtimeList = realtimeSeq.toList();
    List<ResultRow> list = seq.toList();

    // expect 4 distinct arrays for each of these columns from 14 rows
    Assert.assertEquals(1, realtimeList.size());
    Assert.assertEquals(14L, realtimeList.get(0).get(9));
    // array column estimate counts
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(10), 0.01);
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(11), 0.01);
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(12), 0.01);
    // pre-aggregated arrays counts
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(13), 0.01);
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(14), 0.01);
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(15), 0.01);
    // if processAsArray is false, count is done as string mvds so it counts the total number of elements
    Assert.assertEquals(5.0, (Double) realtimeList.get(0).get(16), 0.01);
    Assert.assertEquals(4.0, (Double) realtimeList.get(0).get(17), 0.01);
    Assert.assertEquals(6.0, (Double) realtimeList.get(0).get(18), 0.01);

    Assert.assertEquals(1, list.size());
    Assert.assertEquals(14L, list.get(0).get(9));
    // array column estimate counts
    Assert.assertEquals(4.0, (Double) list.get(0).get(10), 0.01);
    Assert.assertEquals(4.0, (Double) list.get(0).get(11), 0.01);
    Assert.assertEquals(4.0, (Double) list.get(0).get(12), 0.01);
    // pre-aggregated arrays counts
    Assert.assertEquals(4.0, (Double) list.get(0).get(13), 0.01);
    Assert.assertEquals(4.0, (Double) list.get(0).get(14), 0.01);
    Assert.assertEquals(4.0, (Double) list.get(0).get(15), 0.01);
    // if processAsArray is false, count is done as string mvds so it counts the total number of elements
    Assert.assertEquals(5.0, (Double) list.get(0).get(16), 0.01);
    Assert.assertEquals(4.0, (Double) list.get(0).get(17), 0.01);
    Assert.assertEquals(6.0, (Double) list.get(0).get(18), 0.01);
  }

  private void assertAggregatorFactorySerde(AggregatorFactory agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            AggregatorFactory.class
        )
    );
  }

  @Test
  public void testSketchEstimatePostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName"),
            null
        )
    );

    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName"),
            2
        )
    );

    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new SketchConstantPostAggregator("name", "AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI="),
            null
        )
    );
  }

  @Test
  public void testSketchSetPostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new SketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new FieldAccessPostAggregator("name2", "fieldName2")
            )
        )
    );

    assertPostAggregatorSerde(
        new SketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new SketchConstantPostAggregator("name2", "AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI=")
            )
        )
    );
  }

  @Test
  public void testCacheKey()
  {
    final SketchMergeAggregatorFactory factory1 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null,
        null,
        null,
        false
    );
    final SketchMergeAggregatorFactory factory2 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null,
        null,
        null,
        false
    );
    final SketchMergeAggregatorFactory factory3 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        32,
        null,
        null,
        null,
        false
    );

    Assert.assertTrue(Arrays.equals(factory1.getCacheKey(), factory2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(factory1.getCacheKey(), factory3.getCacheKey()));
  }

  @Test
  public void testRetentionDataIngestAndGpByQuery() throws Exception
  {
    final GroupByQuery groupByQuery =
        readQueryFromClasspath("retention_test_data_group_by_query.json", helper.getObjectMapper(), vectorize);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("retention_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        5,
        groupByQuery
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("p1_unique_country_day_1", 20.0)
                    .put("p1_unique_country_day_2", 20.0)
                    .put("p1_unique_country_day_3", 10.0)
                    .put("sketchEstimatePostAgg", 20.0)
                    .put("sketchIntersectionPostAggEstimate1", 10.0)
                    .put("sketchIntersectionPostAggEstimate2", 5.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
        ).stream().map(row -> ResultRow.fromLegacyRow(row, groupByQuery)).collect(Collectors.toList()),
        results
    );
  }

  @Test
  public void testSketchAggregatorFactoryComparator()
  {
    Comparator<Object> comparator = SketchHolder.COMPARATOR;
    //noinspection EqualsWithItself
    Assert.assertEquals(0, comparator.compare(null, null));

    Union union1 = (Union) SetOperation.builder().setNominalEntries(1 << 4).build(Family.UNION);
    union1.update("a");
    union1.update("b");
    Sketch sketch1 = union1.getResult();

    Assert.assertEquals(-1, comparator.compare(null, SketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch1), null));

    Union union2 = (Union) SetOperation.builder().setNominalEntries(1 << 4).build(Family.UNION);
    union2.update("a");
    union2.update("b");
    union2.update("c");
    Sketch sketch2 = union2.getResult();

    Assert.assertEquals(-1, comparator.compare(SketchHolder.of(sketch1), SketchHolder.of(sketch2)));
    Assert.assertEquals(-1, comparator.compare(SketchHolder.of(sketch1), SketchHolder.of(union2)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch2), SketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch2), SketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(union2), SketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(union2), SketchHolder.of(sketch1)));
  }

  @Test
  public void testRelocation()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    SketchHolder sketchHolder = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(16).build());
    UpdateSketch updateSketch = (UpdateSketch) sketchHolder.getSketch();
    updateSketch.update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));
    SketchHolder[] holders = helper.runRelocateVerificationTest(
        new SketchMergeAggregatorFactory("sketch", "sketch", 16, false, true, 2, false),
        columnSelectorFactory,
        SketchHolder.class
    );
    Assert.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
  }

  @Test
  public void testUpdateUnionWithNullInList()
  {
    List<String> value = new ArrayList<>();
    value.add("foo");
    value.add(null);
    value.add("");
    value.add("bar");
    List[] columnValues = new List[]{value};
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(columnValues);
    final Aggregator agg = new SketchAggregator(selector, 4096, false);
    agg.aggregate();
    Assert.assertFalse(agg.isNull());
    Assert.assertNotNull(agg.get());
    Assert.assertTrue(agg.get() instanceof SketchHolder);
    Assert.assertEquals(2, ((SketchHolder) agg.get()).getEstimate(), 0);
    Assert.assertNotNull(((SketchHolder) agg.get()).getSketch());
    Assert.assertEquals(2, ((SketchHolder) agg.get()).getSketch().getEstimate(), 0);
  }

  @Test
  public void testUpdateUnionWithDouble()
  {
    Double[] columnValues = new Double[]{2.0};
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(columnValues);
    final Aggregator agg = new SketchAggregator(selector, 4096, false);
    agg.aggregate();
    Assert.assertFalse(agg.isNull());
    Assert.assertNotNull(agg.get());
    Assert.assertTrue(agg.get() instanceof SketchHolder);
    Assert.assertEquals(1, ((SketchHolder) agg.get()).getEstimate(), 0);
    Assert.assertNotNull(((SketchHolder) agg.get()).getSketch());
    Assert.assertEquals(1, ((SketchHolder) agg.get()).getSketch().getEstimate(), 0);
  }

  @Test
  public void testAggregateWithSize()
  {
    final String[] columnValues = new String[20];
    for (int i = 0; i < columnValues.length; ++i) {
      columnValues[i] = "" + i;
    }

    final TestObjectColumnSelector<String> selector = new TestObjectColumnSelector<>(columnValues);
    final SketchAggregator agg = new SketchAggregator(selector, 128, false);

    // Verify initial size of sketch
    Assert.assertEquals(48L, agg.getInitialSizeBytes());
    Assert.assertEquals(328L, agg.aggregateWithSize());

    // Verify that subsequent size deltas are zero
    for (int i = 1; i < 16; ++i) {
      selector.increment();
      long sizeDelta = agg.aggregateWithSize();
      Assert.assertEquals(0, sizeDelta);
    }

    // Verify that size delta is positive when sketch resizes
    selector.increment();
    long deltaAtResize = agg.aggregateWithSize();
    Assert.assertEquals(1792, deltaAtResize);

    for (int i = 17; i < columnValues.length; ++i) {
      selector.increment();
      long sizeDelta = agg.aggregateWithSize();
      Assert.assertEquals(0, sizeDelta);
    }

    // Verify unique count estimate
    SketchHolder sketchHolder = (SketchHolder) agg.get();
    Assert.assertEquals(columnValues.length, sketchHolder.getEstimate(), 0);
    Assert.assertNotNull(sketchHolder.getSketch());
    Assert.assertEquals(columnValues.length, sketchHolder.getSketch().getEstimate(), 0);
  }

  private void assertPostAggregatorSerde(PostAggregator agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            PostAggregator.class
        )
    );
  }

  public static <T, Q extends Query<T>> Q readQueryFromClasspath(
      final String fileName,
      final ObjectMapper objectMapper,
      final QueryContexts.Vectorize vectorize
  ) throws IOException
  {
    final String queryString = readFileFromClasspathAsString(fileName);

    //noinspection unchecked
    return (Q) objectMapper.readValue(queryString, Query.class)
                           .withOverriddenContext(ImmutableMap.of("vectorize", vectorize.toString()));
  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(SketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        StandardCharsets.UTF_8
    ).read();
  }
}
