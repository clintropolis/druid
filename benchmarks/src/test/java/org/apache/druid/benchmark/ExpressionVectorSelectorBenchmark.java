package org.apache.druid.benchmark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ExpressionVectorSelectorBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @Param({"1000000"})
  private int rowsPerSegment;

  @Param({"true", "false"})
  private boolean vectorize;

  private QueryableIndex index;
  private Closer closer;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = new GeneratorSchemaInfo(
        ImmutableList.of(
            GeneratorColumnSchema.makeZipf(
                "n",
                ValueType.LONG,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            ),
            GeneratorColumnSchema.makeZipf(
                "n2",
                ValueType.LONG,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            ),
            GeneratorColumnSchema.makeZipf(
                "s",
                ValueType.STRING,
                false,
                1,
                0d,
                1000,
                10000,
                3d
            )
        ),
        ImmutableList.of(),
        Intervals.of("2000/P1D"),
        false
    );

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    this.index = closer.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, rowsPerSegment)
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
  public void multiply(Blackhole blackhole)
  {
    final String expression = "n * n2";
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v",
                expression,
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );
    if (vectorize) {
      VectorCursor cursor = new QueryableIndexStorageAdapter(index).makeVectorCursor(
          null,
          index.getDataInterval(),
          virtualColumns,
          false,
          512,
          null
      );
      VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
      while (!cursor.isDone()) {
        blackhole.consume(selector.getLongVector());
        blackhole.consume(selector.getNullVector());
        cursor.advance();
      }
      closer.register(cursor);
    } else {
      Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
          null,
          index.getDataInterval(),
          virtualColumns,
          Granularities.ALL,
          false,
          null
      );

      int rowCount = cursors
          .map(cursor -> {
            final ColumnValueSelector selector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");
            int rows = 0;
            while (!cursor.isDone()) {
              blackhole.consume(selector.getLong());
              rows++;
              cursor.advance();
            }
            return rows;
          }).accumulate(0, (acc, in) -> acc + in);

      blackhole.consume(rowCount);
    }
  }

  private void checkSanity()
  {
    final String expression = "n * n2";
    final List<Long> longs = new ArrayList<>(rowsPerSegment);
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v",
                expression,
                ValueType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );
    VectorCursor cursor = new QueryableIndexStorageAdapter(index).makeVectorCursor(
        null,
        index.getDataInterval(),
        virtualColumns,
        false,
        512,
        null
    );
    VectorValueSelector selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
    int rowCount = 0;
    while (!cursor.isDone()) {
      long[] current = selector.getLongVector();
      for (int i = 0; i < selector.getCurrentVectorSize(); i++,rowCount++) {
        longs.add(current[i]);
      }
      cursor.advance();
    }
    closer.register(cursor);

    Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        virtualColumns,
        Granularities.ALL,
        false,
        null
    );

    int rowCountCursor = cursors
        .map(nonVectorized -> {
          final ColumnValueSelector nonSelector = nonVectorized.getColumnSelectorFactory().makeColumnValueSelector("v");
          int rows = 0;
          while (!nonVectorized.isDone()) {
            Preconditions.checkArgument(longs.get(rows) == nonSelector.getLong(), "Failed at row " + rows);
            rows++;
            nonVectorized.advance();
          }
          return rows;
        }).accumulate(0, (acc, in) -> acc + in);

  }
}
