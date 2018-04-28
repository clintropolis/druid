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

package io.druid.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.common.config.NullHandling;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.BaseObjectColumnValueSelector;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.DoublesColumn;
import io.druid.segment.column.FloatsColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ColumnarDoubles;
import io.druid.segment.data.ColumnarFloats;
import io.druid.segment.data.ColumnarInts;
import io.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.VSizeColumnarInts;
import io.druid.segment.filter.Filters;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import io.druid.segment.serde.DoubleGenericColumnPartSerdeV2;
import io.druid.segment.serde.FloatGenericColumnPartSerdeV2;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Command(
    name = "dump-segment",
    description = "Dump segment data"
)
public class DumpSegment extends GuiceRunnable
{
  private static final Logger log = new Logger(DumpSegment.class);

  private enum DumpType
  {
    ROWS,
    METADATA,
    BITMAPS,
    BITFREQUENCY,
    NUMERIC
  }

  public DumpSegment()
  {
    super(log);
  }

  @Option(
      name = {"-d", "--directory"},
      title = "directory",
      description = "Directory containing segment data.",
      required = true)
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "file",
      description = "File to write to, or omit to write to stdout.",
      required = false)
  public String outputFileName;

  @Option(
      name = {"--filter"},
      title = "json",
      description = "Filter, JSON encoded, or omit to include all rows. Only used if dumping rows.",
      required = false)
  public String filterJson = null;

  @Option(
      name = {"-c", "--column"},
      title = "column",
      description = "Column to include, specify multiple times for multiple columns, or omit to include all columns.",
      required = false)
  public List<String> columnNamesFromCli = Lists.newArrayList();

  @Option(
      name = "--time-iso8601",
      title = "Format __time column in ISO8601 format rather than long. Only used if dumping rows.",
      required = false)
  public boolean timeISO8601 = false;

  @Option(
      name = "--dump",
      title = "type",
      description = "Dump either 'rows' (default), 'metadata', 'bitmaps', or 'bitfrequency', 'numeric'",
      required = false)
  public String dumpTypeString = DumpType.ROWS.toString();

  @Option(
      name = "--decompress-bitmaps",
      title = "Dump bitmaps as arrays rather than base64-encoded compressed bitmaps. Only used if dumping bitmaps.",
      required = false)
  public boolean decompressBitmaps = false;

  @Option(
      name = "--anonymize-bitfrequency",
      title = "Dump bitfrequency column names as a generic string instead. Only used if dumping bitfrequency.",
      required = false)
  public boolean anonymizeColumnNames = false;


  @Option(
      name = {"--prefix"},
      title = "output-file suffix for numerical column value dumps (to feed into encoding benchmarks)",
      description = "prefix filename for output column files, which will bein the form of "
                    + "'{prefix}-values-{type}-{columnName}.txt'",
      required = false)
  public String outputFilePrefix;

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    final DumpType dumpType;

    try {
      dumpType = DumpType.valueOf(StringUtils.toUpperCase(dumpTypeString));
    }
    catch (Exception e) {
      throw new IAE("Not a valid dump type: %s", dumpTypeString);
    }

    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      switch (dumpType) {
        case ROWS:
          runDump(injector, index);
          break;
        case METADATA:
          runMetadata(injector, index);
          break;
        case BITMAPS:
          runBitmaps(injector, index);
          break;
        case BITFREQUENCY:
          runBitFrequency(injector, index);
          break;
        case NUMERIC:
          runNumericalColumnDump(injector, index);
          break;
        default:
          throw new ISE("WTF?! dumpType[%s] has no handler?", dumpType);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void runMetadata(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class))
                                              .copy()
                                              .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    final SegmentMetadataQuery query = new SegmentMetadataQuery(
        new TableDataSource("dataSource"),
        new SpecificSegmentSpec(new SegmentDescriptor(index.getDataInterval(), "0", 0)),
        new ListColumnIncluderator(getColumnsToInclude(index)),
        false,
        null,
        EnumSet.allOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );
    withOutputStream(
        out -> {
          evaluateSequenceForSideEffects(
              Sequences.map(
                  executeQuery(injector, index, query),
                  analysis -> {
                    try {
                      objectMapper.writeValue(out, analysis);
                    }
                    catch (IOException e) {
                      throw Throwables.propagate(e);
                    }
                    return null;
                  }
              )
          );

          return null;
        }
    );
  }

  private void runDump(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    final List<String> columnNames = getColumnsToInclude(index);
    final DimFilter filter = filterJson != null ? objectMapper.readValue(filterJson, DimFilter.class) : null;

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(filter),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    withOutputStream(
        out -> {
          final Sequence<Object> sequence = Sequences.map(
              cursors,
              cursor -> {
                final List<BaseObjectColumnValueSelector> selectors = Lists.newArrayList();

                for (String columnName : columnNames) {
                  ColumnValueSelector selector =
                      cursor.getColumnSelectorFactory().makeColumnValueSelector(columnName);
                  selectors.add(new ListObjectSelector(selector));
                }

                while (!cursor.isDone()) {
                  final Map<String, Object> row = Maps.newLinkedHashMap();

                  for (int i = 0; i < columnNames.size(); i++) {
                    final String columnName = columnNames.get(i);
                    final Object value = selectors.get(i).getObject();

                    if (timeISO8601 && columnNames.get(i).equals(Column.TIME_COLUMN_NAME)) {
                      row.put(columnName, new DateTime(value, DateTimeZone.UTC).toString());
                    } else {
                      row.put(columnName, value);
                    }
                  }

                  try {
                    out.write(objectMapper.writeValueAsBytes(row));
                    out.write('\n');
                  }
                  catch (IOException e) {
                    throw Throwables.propagate(e);
                  }

                  cursor.advance();
                }

                return null;
              }
          );

          evaluateSequenceForSideEffects(sequence);

          return null;
        }
    );
  }

  private void runBitmaps(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final BitmapSerdeFactory bitmapSerdeFactory;

    if (bitmapFactory instanceof ConciseBitmapFactory) {
      bitmapSerdeFactory = new ConciseBitmapSerdeFactory();
    } else if (bitmapFactory instanceof RoaringBitmapFactory) {
      bitmapSerdeFactory = new RoaringBitmapSerdeFactory(null);
    } else {
      throw new ISE(
          "Don't know which BitmapSerdeFactory to use for BitmapFactory[%s]!",
          bitmapFactory.getClass().getName()
      );
    }

    final List<String> columnNames = getColumnsToInclude(index);

    withOutputStream(
        out -> {
          try {
            final JsonGenerator jg = objectMapper.getFactory().createGenerator(out);

            jg.writeStartObject();
            jg.writeObjectField("bitmapSerdeFactory", bitmapSerdeFactory);
            jg.writeFieldName("bitmaps");
            jg.writeStartObject();

            for (final String columnName : columnNames) {
              final Column column = index.getColumn(columnName);
              final BitmapIndex bitmapIndex = column.getBitmapIndex();

              if (bitmapIndex == null) {
                jg.writeNullField(columnName);
              } else {
                jg.writeFieldName(columnName);
                jg.writeStartObject();
                for (int i = 0; i < bitmapIndex.getCardinality(); i++) {
                  String val = NullHandling.nullToEmptyIfNeeded(bitmapIndex.getValue(i));
                  if (val != null) {
                    final ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
                    if (decompressBitmaps) {
                      jg.writeStartArray();
                      final IntIterator iterator = bitmap.iterator();
                      while (iterator.hasNext()) {
                        final int rowNum = iterator.next();
                        jg.writeNumber(rowNum);
                      }
                      jg.writeEndArray();
                    } else {
                      jg.writeBinary(bitmapSerdeFactory.getObjectStrategy().toBytes(bitmap));
                    }
                  }
                }
                jg.writeEndObject();
              }
            }

            jg.writeEndObject();
            jg.writeEndObject();
            jg.close();
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }

          return null;
        }
    );
  }

  private void runBitFrequency(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final List<String> columnNames = getColumnsToInclude(index);

    // for now inlining a large chunk of index v9 loader logic rather than poking holes
    try (final SmooshedFileMapper smooshMapper = SmooshedFileMapper.load(new File(directory))) {
      withOutputStream(
          out -> {
            final Map<String, ColumnFrequencyAnalysis> frequencies = Maps.newHashMap();
            final Map<String, Integer> typeCounts = Maps.newHashMap();

            for (String columnName : columnNames) {
              final Column column = index.getColumn(columnName);
              final ColumnCapabilities capabilities = column.getCapabilities();
              final ValueType columnType = capabilities.getType();
              final String typeName = columnType.name().toLowerCase();
              typeCounts.put(typeName, typeCounts.getOrDefault(typeName, 0) + 1);
              final String keyName = anonymizeColumnNames
                                     ? String.format("%s-%d", typeName, typeCounts.get(typeName))
                                     : columnName;
              try {
                ByteBuffer buffer = smooshMapper.mapFile(columnName);

                // read descriptor length
                final int length = buffer.getInt();

                // get descriptor json
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                String descriptorJson = StringUtils.fromUtf8(bytes);

                ColumnDescriptor serde = objectMapper.readValue(descriptorJson, ColumnDescriptor.class);

                int start = buffer.position();
                int smooshSize = 0;
                GenericColumn genericColumn = column.getGenericColumn();
                switch (columnType) {
                  case LONG:

                    byte versionFromBuffer = buffer.get();

                    if (versionFromBuffer == 0x1 || versionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", versionFromBuffer);
                    }

                    long[] longBits = new long[Long.BYTES * 8];
                    Arrays.fill(longBits, 0L);
                    for (int i = 0; i < genericColumn.length(); i++) {
                      long value = genericColumn.getLongSingleValueRow(i);
                      countBits64(value, longBits);
                    }
                    frequencies.put(
                        keyName,
                        new ColumnFrequencyAnalysis(
                            "long",
                            genericColumn.length(),
                            smooshSize,
                            longBits
                        )
                    );
                    break;
                  case DOUBLE:
                    if (serde.getParts().get(0) instanceof DoubleGenericColumnPartSerdeV2) {
                      int doubleOffset = buffer.getInt();
                    }
                    byte doubleVersionFromBuffer = buffer.get();

                    if (doubleVersionFromBuffer == 0x1 || doubleVersionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", doubleVersionFromBuffer);
                    }

                    ColumnarDoubles doubleColumn = ((DoublesColumn) genericColumn).getColumn();

                    long[] doubleBits = new long[Long.BYTES * 8];
                    Arrays.fill(doubleBits, 0L);

                    for (int i = 0; i < genericColumn.length(); i++) {
                      long value = Double.doubleToRawLongBits(doubleColumn.get(i));
                      countBits64(value, doubleBits);
                    }
                    frequencies.put(
                        keyName,
                        new ColumnFrequencyAnalysis(
                            "double",
                            genericColumn.length(),
                            smooshSize,
                            doubleBits
                        )
                    );
                    break;
                  case FLOAT:
                    if (serde.getParts().get(0) instanceof FloatGenericColumnPartSerdeV2) {
                      int floatOffset = buffer.getInt();
                    }
                    byte floatVersionFromBuffer = buffer.get();

                    if (floatVersionFromBuffer == 0x1 || floatVersionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", floatVersionFromBuffer);
                    }

                    long[] floatBits = new long[Float.BYTES * 8];
                    Arrays.fill(floatBits, 0L);
                    ColumnarFloats floatColumn = ((FloatsColumn) genericColumn).getColumn();
                    for (int i = 0; i < genericColumn.length(); i++) {
                      int value = Float.floatToIntBits(floatColumn.get(i));
                      countBits32(value, floatBits);
                    }
                    frequencies.put(
                        keyName,
                        new ColumnFrequencyAnalysis(
                            "float",
                            genericColumn.length(),
                            smooshSize,
                            floatBits
                        )
                    );
                    break;
                  case STRING:
                    if (!(capabilities.hasMultipleValues() || capabilities.hasSpatialIndexes())) {

                      DictionaryEncodedColumn<String> theColumn = column.getDictionaryEncoding();

                      int indexSmooshSize = 0;

                      // we already know dictionary encoded, so start to deserialize as such
                      byte version = buffer.get();
                      int flag = buffer.getInt();
                      final GenericIndexed<String> rDictionary = GenericIndexed.read(
                          buffer,
                          GenericIndexed.STRING_STRATEGY,
                          smooshMapper
                      );

                      ColumnPartSerde partSerde = serde.getParts().get(0);
                      start = buffer.position();
                      ColumnarInts ints = null;
                      switch (version) {
                        case 0x01:
                          ints = VSizeColumnarInts.readFromByteBuffer(buffer);
                          break;
                        case 0x02:
                          ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
                              buffer,
                              ((DictionaryEncodedColumnPartSerde) partSerde).getByteOrder()
                          ).get();
                          break;
                        default:
                          throw new IAE("Unsupported single-value version[%s]", version);
                      }
                      indexSmooshSize += (buffer.position() - start);

                      long[] bitmapBits = new long[Integer.BYTES * 8];
                      Arrays.fill(bitmapBits, 0L);

                      for (int i = 0; i < theColumn.length(); i++) {
                        int value = theColumn.getSingleValueRow(i);
                        countBits32(value, bitmapBits);
                      }
                      frequencies.put(
                          keyName,
                          new ColumnFrequencyAnalysis(
                              "int",
                              theColumn.length(),
                              indexSmooshSize,
                              bitmapBits
                          )
                      );
                    }
                    break;
                }

              }
              catch (IOException ioe) {
                Throwables.propagate(ioe);
              }
            }

            try {
              out.write(objectMapper.writeValueAsBytes(frequencies));
              out.write('\n');
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }

            return null;
          }
      );

    }
  }

  private void runNumericalColumnDump(final Injector injector, final QueryableIndex index) throws IOException
  {
    final ObjectMapper objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final List<String> columnNames = getColumnsToInclude(index);

    // for now inlining a large chunk of index v9 loader logic rather than poking holes
    try (final SmooshedFileMapper smooshMapper = SmooshedFileMapper.load(new File(directory))) {
      withOutputStream(
          out -> {
            final Map<String, Integer> typeCounts = Maps.newHashMap();

            for (String columnName : columnNames) {
              final Column column = index.getColumn(columnName);
              final ColumnCapabilities capabilities = column.getCapabilities();
              final ValueType columnType = capabilities.getType();
              final String typeName = columnType.name().toLowerCase();
              typeCounts.put(typeName, typeCounts.getOrDefault(typeName, 0) + 1);
              final String keyName = anonymizeColumnNames
                                     ? String.format("%s-%d", typeName, typeCounts.get(typeName))
                                     : columnName;

              final String fileName = outputFilePrefix + "-values-" + (typeName.equals("string")
                                                                       ? (capabilities.hasMultipleValues()
                                                                          ? "multi-int"
                                                                          : "int")
                                                                       : typeName) + "-" + keyName + ".txt";
              File dataFile = new File(fileName);

              try (Writer writer = Files.newBufferedWriter(dataFile.toPath(), StandardCharsets.UTF_8)) {
                ByteBuffer buffer = smooshMapper.mapFile(columnName);

                // read descriptor length
                final int length = buffer.getInt();

                // get descriptor json
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                String descriptorJson = StringUtils.fromUtf8(bytes);

                ColumnDescriptor serde = objectMapper.readValue(descriptorJson, ColumnDescriptor.class);

                int start = buffer.position();

                int smooshSize = 0;
                GenericColumn genericColumn = column.getGenericColumn();
                switch (columnType) {
                  case LONG:

                    byte versionFromBuffer = buffer.get();

                    if (versionFromBuffer == 0x1 || versionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", versionFromBuffer);
                    }

                    for (int i = 0; i < genericColumn.length(); i++) {
                      long value = genericColumn.getLongSingleValueRow(i);
                      writer.write(value + "\n");
                    }
                    break;
                  case DOUBLE:
                    if (serde.getParts().get(0) instanceof DoubleGenericColumnPartSerdeV2) {
                      int doubleOffset = buffer.getInt();
                    }
                    byte doubleVersionFromBuffer = buffer.get();

                    if (doubleVersionFromBuffer == 0x1 || doubleVersionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", doubleVersionFromBuffer);
                    }

                    ColumnarDoubles doubleColumn = ((DoublesColumn) genericColumn).getColumn();

                    for (int i = 0; i < genericColumn.length(); i++) {
                      double value = doubleColumn.get(i);
                      writer.write(value + "\n");
                    }
                    break;
                  case FLOAT:
                    if (serde.getParts().get(0) instanceof FloatGenericColumnPartSerdeV2) {
                      int floatOffset = buffer.getInt();
                    }
                    byte floatVersionFromBuffer = buffer.get();

                    if (floatVersionFromBuffer == 0x1 || floatVersionFromBuffer == 0x2) {
                      smooshSize = buffer.getInt();
                    } else {
                      throw new IAE("Unknown version[%s]", floatVersionFromBuffer);
                    }

                    ColumnarFloats floatColumn = ((FloatsColumn) genericColumn).getColumn();
                    for (int i = 0; i < genericColumn.length(); i++) {
                      float value = floatColumn.get(i);
                      writer.write(value + "\n");
                    }
                    break;
                  case STRING:
                    if (!(capabilities.hasMultipleValues())) {

                      DictionaryEncodedColumn<String> theColumn = column.getDictionaryEncoding();

                      int indexSmooshSize = 0;

                      // we already know dictionary encoded, so start to deserialize as such
                      byte version = buffer.get();
                      int flag = buffer.getInt();
                      final GenericIndexed<String> rDictionary = GenericIndexed.read(
                          buffer,
                          GenericIndexed.STRING_STRATEGY,
                          smooshMapper
                      );

                      ColumnPartSerde partSerde = serde.getParts().get(0);
                      start = buffer.position();
                      ColumnarInts ints = null;
                      switch (version) {
                        case 0x01:
                          VSizeColumnarInts.readFromByteBuffer(buffer);
                          break;
                        case 0x02:
                          CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
                              buffer,
                              ((DictionaryEncodedColumnPartSerde) partSerde).getByteOrder()
                          ).get();
                          break;
                        default:
                          throw new IAE("Unsupported single-value version[%s]", version);
                      }
                      for (int i = 0; i < theColumn.length(); i++) {
                        int value = theColumn.getSingleValueRow(i);
                        writer.write(value + "\n");
                      }
                    }
                    break;
                }

              }
              catch (IOException ioe) {
                Throwables.propagate(ioe);
              }
            }

            return null;
          }
      );

    }
  }

  private void countBits32(int value, long[] bits)
  {
    for (int i = 0; i < 32; i++) {
      if ((value & (1L << i)) != 0) {
        bits[i]++;
      }
    }
  }

  private void countBits64(long value, long[] bits)
  {
    for (int i = 0; i < 64; i++) {
      if ((value & (1L << i)) != 0) {
        bits[i]++;
      }
    }
  }

  private List<String> getColumnsToInclude(final QueryableIndex index)
  {
    final Set<String> columnNames = Sets.newLinkedHashSet(columnNamesFromCli);

    // Empty columnNames => include all columns.
    if (columnNames.isEmpty()) {
      columnNames.add(Column.TIME_COLUMN_NAME);
      Iterables.addAll(columnNames, index.getColumnNames());
    } else {
      // Remove any provided columns that do not exist in this segment.
      for (String columnName : ImmutableList.copyOf(columnNames)) {
        if (index.getColumn(columnName) == null) {
          columnNames.remove(columnName);
        }
      }
    }

    return ImmutableList.copyOf(columnNames);
  }

  private <T> T withOutputStream(Function<OutputStream, T> f) throws IOException
  {
    if (outputFileName == null) {
      return f.apply(System.out);
    } else {
      try (final OutputStream out = new FileOutputStream(outputFileName)) {
        return f.apply(out);
      }
    }
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
          binder.bind(DruidProcessingConfig.class).toInstance(
              new DruidProcessingConfig()
              {
                @Override
                public String getFormatString()
                {
                  return "processing-%s";
                }

                @Override
                public int intermediateComputeSizeBytes()
                {
                  return 100 * 1024 * 1024;
                }

                @Override
                public int getNumThreads()
                {
                  return 1;
                }

                @Override
                public int columnCacheSizeBytes()
                {
                  return 25 * 1024 * 1024;
                }
              }
          );
          binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
        }
    );
  }

  private static <T> Sequence<T> executeQuery(final Injector injector, final QueryableIndex index, final Query<T> query)
  {
    final QueryRunnerFactoryConglomerate conglomerate = injector.getInstance(QueryRunnerFactoryConglomerate.class);
    final QueryRunnerFactory factory = conglomerate.findFactory(query);
    final QueryRunner runner = factory.createRunner(new QueryableIndexSegment("segment", index));
    final Sequence results = factory.getToolchest().mergeResults(
        factory.mergeRunners(MoreExecutors.sameThreadExecutor(), ImmutableList.of(runner))
    ).run(QueryPlus.wrap(query), Maps.newHashMap());
    return (Sequence<T>) results;
  }

  private static <T> void evaluateSequenceForSideEffects(final Sequence<T> sequence)
  {
    sequence.accumulate(
        null,
        (accumulated, in) -> null
    );
  }

  private static class ListObjectSelector implements ColumnValueSelector
  {
    private final ColumnValueSelector delegate;

    private ListObjectSelector(ColumnValueSelector delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public double getDouble()
    {
      return delegate.getDouble();
    }

    @Override
    public float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public long getLong()
    {
      return delegate.getLong();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      Object object = delegate.getObject();
      if (object instanceof String[]) {
        return Arrays.asList((String[]) object);
      } else {
        return object;
      }
    }

    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegate", delegate);
    }

    @Override
    public boolean isNull()
    {
      return delegate.isNull();
    }
  }

  private class ColumnFrequencyAnalysis
  {
    private final String type;
    private final int count;
    private final int smooshSize;
    private final long[] bitFrequency;

    @JsonProperty
    public String getType()
    {
      return type;
    }

    @JsonProperty
    public int getCount()
    {
      return count;
    }

    @JsonProperty
    public int getSmooshSize()
    {
      return smooshSize;
    }

    @JsonProperty
    public long[] getBitFrequency()
    {
      return bitFrequency;
    }

    public ColumnFrequencyAnalysis(String type, int count, int smooshSize, long[] bitFrequency)
    {
      this.type = type;
      this.count = count;
      this.smooshSize = smooshSize;
      this.bitFrequency = bitFrequency;
    }
  }
}
