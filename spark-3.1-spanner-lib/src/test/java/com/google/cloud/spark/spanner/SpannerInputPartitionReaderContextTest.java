package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.InputPartitionReaderContext;
import com.google.cloud.spark.spanner.SpannerInputPartitionContext;
import com.google.cloud.spark.spanner.SpannerScanBuilder;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.SpannerUtils;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerInputPartitionReaderContextTest extends SpannerTestBase {

  InternalRow makeInternalRow(int A, String B, double C) {
    GenericInternalRow row = new GenericInternalRow(3);
    row.setLong(0, A);
    row.update(1, UTF8String.fromString(B));
    row.setDouble(2, C);
    return row;
  }

  class InternalRowComparator implements Comparator<InternalRow> {
    @Override
    public int compare(InternalRow r1, InternalRow r2) {
      return r1.toString().compareTo(r2.toString());
    }
  }

  @Test
  public void testCreatePartitionContext() throws Exception {
    String sqlStmt = "SELECT * FROM simpleTable";

    // We expect that each partition will have some elements but
    // at the end we expect that the following will be present:
    List<InternalRow> expectRows =
        Arrays.asList(
            makeInternalRow(1, "1", 2.5),
            makeInternalRow(2, "2", 5.0),
            makeInternalRow(3, "3", Double.POSITIVE_INFINITY),
            makeInternalRow(4, "4", Double.NEGATIVE_INFINITY),
            makeInternalRow(5, "5", Double.NaN),
            makeInternalRow(6, "6", 100000000017.100000000017),
            makeInternalRow(7, "7", -0.0),
            makeInternalRow(8, "8", +0.0),
            makeInternalRow(9, "9", -19999997.9));
    List<InternalRow> gotRows = new ArrayList<>();

    CopyOnWriteArrayList<InternalRow> al = new CopyOnWriteArrayList<>();

    BatchClient batchClient = this.createBatchClient();
    try (final BatchReadOnlyTransaction txn =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      // Not using executor.execute as controlling immediate termination
      // is non-granular and out of scope of these tests.
      Map<String, String> opts = this.connectionProperties();
      String mapAsJSON = SpannerUtils.serializeMap(opts);

      for (final Partition partition : partitions) {
        SpannerInputPartitionContext sCtx =
            new SpannerInputPartitionContext(partition, txn.getBatchTransactionId(), mapAsJSON);
        try {
          InputPartitionReaderContext<InternalRow> ctx = sCtx.createPartitionReaderContext();

          while (ctx.next()) {
            al.add(ctx.get());
          }
          ctx.close();
        } catch (IOException e) {
          System.out.println("\033[33mexception now: " + e + "\033[00m");
        }
        al.forEach(gotRows::add);
      }
    }

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows, gotRows);
  }

  public InternalRow makeCompositeTableRow(
      String id,
      long[] A,
      String[] B,
      String C,
      java.math.BigDecimal D,
      ZonedDateTime E,
      ZonedDateTime F,
      boolean G,
      ZonedDateTime[] H,
      ZonedDateTime[] I) {
    GenericInternalRow row = new GenericInternalRow(10);
    row.update(0, UTF8String.fromString(id));
    row.update(1, new GenericArrayData(A));
    row.update(2, new GenericArrayData(toSparkStrList(B)));
    row.update(3, UTF8String.fromString(C));
    SpannerUtils.asSparkDecimal(row, D, 4);
    row.update(5, SpannerUtils.zonedDateTimeToLong(E));
    row.update(6, SpannerUtils.zonedDateTimeToLong(F));
    row.setBoolean(7, G);
    row.update(8, SpannerUtils.zonedDateTimeIterToSparkInts(Arrays.asList(H)));
    row.update(9, SpannerUtils.zonedDateTimeIterToSparkInts(Arrays.asList(I)));
    return row;
  }

  private UTF8String[] toSparkStrList(String[] strs) {
    List<UTF8String> dest = new ArrayList<>();
    for (String s : strs) {
      dest.add(UTF8String.fromString(s));
    }
    return dest.toArray(new UTF8String[0]);
  }

  public InternalRow makeGamesRow(
      String playerId,
      String[] playerIds,
      String winner,
      ZonedDateTime createdAt,
      ZonedDateTime finishedAt,
      ZonedDateTime maxDate) {
    GenericInternalRow row = new GenericInternalRow(6);
    row.update(0, UTF8String.fromString(playerId));
    List<UTF8String> dest = new ArrayList<UTF8String>(playerIds.length);
    for (String id : playerIds) {
      dest.add(UTF8String.fromString(id));
    }
    row.update(1, new GenericArrayData(dest.toArray(new UTF8String[0])));
    row.update(2, UTF8String.fromString(winner));
    row.update(3, SpannerUtils.zonedDateTimeToLong(createdAt));
    row.update(4, SpannerUtils.zonedDateTimeToLong(finishedAt));
    row.update(5, SpannerUtils.zonedDateTimeToLong(maxDate));
    return row;
  }

  @Test
  public void testMoreDiverseTables() {
    Map<String, String> props = this.connectionProperties();
    props.put("table", "games");
    SpannerTable st = new SpannerTable(props);
    CaseInsensitiveStringMap csm = new CaseInsensitiveStringMap(props);
    ScanBuilder sb = st.newScanBuilder(csm);
    SpannerScanBuilder ssb = ((SpannerScanBuilder) sb);
    InputPartition[] parts = ssb.planInputPartitions();
    PartitionReaderFactory prf = ssb.createReaderFactory();

    List<InternalRow> gotRows = new ArrayList<>();
    for (InputPartition part : parts) {
      PartitionReader<InternalRow> ir = prf.createReader(part);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          gotRows.add(row);
        }
      } catch (IOException e) {
      }
    }

    ZonedDateTime createdAt = ZonedDateTime.parse("2023-08-26T12:22:00Z");
    ZonedDateTime finishedAt = ZonedDateTime.parse("2023-08-26T12:22:00Z");
    ZonedDateTime maxDate = ZonedDateTime.parse("2023-12-31T00:00:00Z");
    List<InternalRow> expectRows =
        Arrays.asList(
            makeGamesRow(
                "g1", new String[] {"p1", "p2", "p3"}, "T1", createdAt, finishedAt, maxDate),
            makeGamesRow(
                "g2", new String[] {"p4", "p5", "p6"}, "T2", createdAt, finishedAt, maxDate));

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertEquals(expectRows, gotRows);
  }

  @Test
  public void testArraysConversions() {
    Map<String, String> props = this.connectionProperties();
    props.put("table", "compositeTable");
    SpannerTable st = new SpannerTable(props);
    CaseInsensitiveStringMap csm = new CaseInsensitiveStringMap(props);
    ScanBuilder sb = st.newScanBuilder(csm);
    SpannerScanBuilder ssb = ((SpannerScanBuilder) sb);
    InputPartition[] parts = ssb.planInputPartitions();
    PartitionReaderFactory prf = ssb.createReaderFactory();

    List<InternalRow> gotRows = new ArrayList<>();
    for (InputPartition part : parts) {
      PartitionReader<InternalRow> ir = prf.createReader(part);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          gotRows.add(row);
        }
      } catch (IOException e) {
      }
    }

    List<InternalRow> expectRows =
        Arrays.asList(
            makeCompositeTableRow(
                "id1",
                new long[] {10, 100, 991, 567282},
                new String[] {"a", "b", "c"},
                "foobar",
                new java.math.BigDecimal(2934),
                ZonedDateTime.parse("2023-01-01T00:00:00Z"),
                ZonedDateTime.parse("2023-08-26T12:22:05Z"),
                true,
                new ZonedDateTime[] {
                  ZonedDateTime.parse("2023-01-02T00:00:00Z"),
                  ZonedDateTime.parse("2023-12-31T00:00:00Z"),
                },
                new ZonedDateTime[] {
                  ZonedDateTime.parse("2023-08-26T12:11:10Z"),
                  ZonedDateTime.parse("2023-08-27T12:11:09Z"),
                }));

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertEquals(expectRows, gotRows);
  }
}
