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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlBenchmark
{
  static {
    NullHandling.initializeForTests();
    Calcites.setSystemProperties();
  }

  private static final Logger log = new Logger(SqlBenchmark.class);

  private static final List<String> QUERIES = ImmutableList.of(
      // 0, 1, 2, 3: Timeseries, unfiltered
      "SELECT COUNT(*) FROM foo",
      "SELECT COUNT(DISTINCT hyper) FROM foo",
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo",
      "SELECT FLOOR(__time TO MINUTE), SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo GROUP BY 1",

      // 4: Timeseries, low selectivity filter (90% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential NOT LIKE '%3'",

      // 5: Timeseries, high selectivity filter (0.1% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo WHERE dimSequential = '311'",

      // 6: Timeseries, mixing low selectivity index-capable filter (90% of rows match) + cursor filter
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3' AND maxLongUniform > 10",

      // 7: Timeseries, low selectivity toplevel filter (90%), high selectivity filtered aggregator (0.1%)
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal)\n"
      + "FROM foo\n"
      + "WHERE dimSequential NOT LIKE '%3'",

      // 8: Timeseries, no toplevel filter, various filtered aggregators with clauses repeated.
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumLongSequential),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumFloatNormal),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential = '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential <> '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  COUNT(*)\n"
      + "FROM foo",

      // 9: Timeseries, toplevel time filter, time-comparison filtered aggregators
      "SELECT\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-01 12:00:00'),\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 12:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00')\n"
      + "FROM foo\n"
      + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'",

      // 10, 11: GroupBy two strings, unfiltered, unordered
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM foo GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1, 2",

      // 12, 13, 14: GroupBy one string, unfiltered, various aggregator configurations
      "SELECT dimZipf FROM foo GROUP BY 1",
      "SELECT dimZipf, COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",
      "SELECT dimZipf, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1 ORDER BY COUNT(*) DESC",

      // 15, 16: GroupBy long, unfiltered, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo GROUP BY 1",

      // 17, 18: GroupBy long, filter by long, unordered; with and without aggregators
      "SELECT maxLongUniform FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM foo WHERE maxLongUniform > 10 GROUP BY 1",
      // 19: ultra mega union matrix
      "WITH matrix (dimZipf, dimSequential) AS (\n"
      + "  (\n"
      + "    SELECT '100', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '100'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '110', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '110'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '120', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '120'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '130', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '130'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '140', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '140'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '150', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '150'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '160', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '160'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '170', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '170'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '180', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '180'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '190', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '190'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '200', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '200'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '210', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '210'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '220', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '220'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '230', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '230'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '240', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '240'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '250', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '250'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '260', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '260'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '270', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '270'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '280', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '280'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '290', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '290'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '300', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '300'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '310', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '310'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '320', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '320'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '330', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '330'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '340', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '340'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '350', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '350'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '360', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '360'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '370', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '370'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '380', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '380'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT 'other', dimSequential\n"
      + "    FROM (SELECT * FROM foo WHERE dimUniform != 1)\n"
      + "    WHERE\n"
      + "      dimZipf NOT IN (\n"
      + "        '100', '110', '120', '130', '140', '150', '160', '170', '180', '190',\n"
      + "        '200', '210', '220', '230', '240', '250', '260', '270', '280', '290',\n"
      + "        '300', '310', '320', '330', '340', '350', '360', '370', '380'\n"
      + "      )\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + ")\n"
      + "SELECT * FROM matrix",
      // 20: expression timeseries
      "SELECT SUM(longSequential * longUniform) FROM foo",
      // 21: non-expression timeseries reference, 2 columns
      "SELECT SUM(longSequential), SUM(longUniform) FROM foo",
      // 22: long*long/double
      "SELECT SUM((longSequential * longUniform) / doubleZipf) FROM foo",
      // 23: non-expression timeseries reference, 3 columns
      "SELECT SUM(longSequential), SUM(longUniform), SUM(doubleZipf) FROM foo",
      // 24: float + long*long/double
      "SELECT SUM(floatZipf + ((longSequential * longUniform)/doubleZipf)) FROM foo",
      // 25: non-expression timeseries reference, 4 columns
      "SELECT SUM(longSequential), SUM(longUniform), SUM(doubleZipf), SUM(floatZipf) FROM foo",
      // 26: long - float + long*long/double
      "SELECT SUM(longUniformWithNulls - (floatZipf + ((longSequential * longUniform)/doubleZipf))) FROM foo",
      // 27: non-expression timeseries reference, 5 columns
      "SELECT SUM(longSequential), SUM(longUniform), SUM(doubleZipf), SUM(floatZipf), SUM(longUniformWithNulls) FROM foo",
      // 28: all same op
      "SELECT SUM(longUniformWithNulls * floatZipf * longSequential * longUniform * doubleZipf) FROM foo",
      // 29: unary negate
      "SELECT SUM(-longUniform) FROM foo",
      // 30: non-expression timeseries reference, 1 column
      "SELECT SUM(longUniform) FROM foo",
      // 31: string long
      "SELECT SUM(PARSE_LONG(stringSequential)) FROM foo",
      // 32: string longer
      "SELECT SUM(PARSE_LONG(stringUniform)) FROM foo",
      // 33: group by long expr
      "SELECT (longSequential * longUniform), SUM(doubleZipf) FROM foo GROUP BY 1 ORDER BY 2",
      // 34
      "SELECT stringZipf, SUM(longSequential * longUniform) FROM foo GROUP BY 1 ORDER BY 2"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  @Param({"false", "force"})
  private String vectorize;


//  @Param({"31", "32"})
//  @Param({"20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34"})
//  @Param({"22", "23", "24"})
//  @Param({"20"})
//  @Param({"30"})
//  @Param({"33"})
  @Param({"20", "22", "24", "26", "28", "29", "31", "32", "33", "34"})
  private String query;

  @Nullable
  private PlannerFactory plannerFactory;
  private Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("nulls-and-non-nulls");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final PlannerConfig plannerConfig = new PlannerConfig();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);
    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);

    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        dataSegment,
        index
    );
    closer.register(walker);

    final SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );

    checkSanity();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void querySql(Blackhole blackhole) throws Exception
  {
    final Map<String, Object> context = ImmutableMap.of("vectorize", vectorize);
    final AuthenticationResult authenticationResult = NoopEscalator.getInstance()
                                                                   .createEscalatedAuthenticationResult();
    try (final DruidPlanner planner = plannerFactory.createPlanner(context, ImmutableList.of(), authenticationResult)) {
      final PlannerResult plannerResult = planner.plan(QUERIES.get(Integer.parseInt(query)));
      final Sequence<Object[]> resultSequence = plannerResult.run();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  public void planSql(Blackhole blackhole) throws Exception
//  {
//    final Map<String, Object> context = ImmutableMap.of("vectorize", vectorize);
//    final AuthenticationResult authenticationResult = NoopEscalator.getInstance()
//                                                                   .createEscalatedAuthenticationResult();
//    try (final DruidPlanner planner = plannerFactory.createPlanner(context, ImmutableList.of(), authenticationResult)) {
//      final PlannerResult plannerResult = planner.plan(QUERIES.get(Integer.parseInt(query)));
//      blackhole.consume(plannerResult);
//    }
//  }

  public void checkSanity() throws Exception
  {
    final Map<String, Object> vector = ImmutableMap.of("vectorize", true);
    final Map<String, Object> nonvector = ImmutableMap.of("vectorize", false);
    final AuthenticationResult authenticationResult = NoopEscalator.getInstance()
                                                                   .createEscalatedAuthenticationResult();


    try (
        final DruidPlanner vectorPlanner = plannerFactory.createPlanner(vector, ImmutableList.of(), authenticationResult);
        final DruidPlanner nonVectorPlanner = plannerFactory.createPlanner(nonvector, ImmutableList.of(), authenticationResult)
    ) {
      final PlannerResult vectorPlan = vectorPlanner.plan(QUERIES.get(Integer.parseInt(query)));
      final PlannerResult nonVectorPlan = nonVectorPlanner.plan(QUERIES.get(Integer.parseInt(query)));
      final Sequence<Object[]> vectorSequence = vectorPlan.run();
      final Sequence<Object[]> nonVectorSequence = nonVectorPlan.run();
      Yielder<Object[]> vectorizedYielder = Yielders.each(vectorSequence);
      Yielder<Object[]> nonVectorizedYielder = Yielders.each(nonVectorSequence);
      int row = 0;
      int misMatch = 0;
      while (!vectorizedYielder.isDone() || !nonVectorizedYielder.isDone()) {
        Object[] vectorGet = vectorizedYielder.get();
        Object[] nonVectorizedGet = nonVectorizedYielder.get();
        try {
          Assert.assertArrayEquals(
              StringUtils.format(
                  "Results differed at row %s (%s : %s)",
                  row,
                  Arrays.toString(vectorGet),
                  Arrays.toString(nonVectorizedGet)
              ),
              vectorGet,
              nonVectorizedGet
          );
        }
        catch (Throwable t) {
          misMatch++;
        }
        vectorizedYielder = vectorizedYielder.next(vectorGet);
        nonVectorizedYielder = nonVectorizedYielder.next(nonVectorizedGet);
        row++;
      }
      Assert.assertEquals(0, misMatch);
      Assert.assertTrue(vectorizedYielder.isDone());
      Assert.assertTrue(nonVectorizedYielder.isDone());
    }
  }
}
