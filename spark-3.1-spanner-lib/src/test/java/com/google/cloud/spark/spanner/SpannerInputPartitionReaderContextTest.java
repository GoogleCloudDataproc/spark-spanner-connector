package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.InputPartitionReaderContext;
import com.google.cloud.spark.spanner.SpannerInputPartitionContext;
import com.google.cloud.spark.spanner.SpannerScanBuilder;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.SpannerUtils;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerInputPartitionReaderContextTest {

  Spanner spanner = SpannerUtilsTest.createSpanner();
  BatchClient batchClient =
      SpannerUtilsTest.createBatchClient(spanner, SpannerUtilsTest.connectionProperties());

  @Before
  public void setUp() throws Exception {
    // 0. Setup the database.
    SpannerUtilsTest ss = new SpannerUtilsTest();
    ss.initDatabase();
    // 1. Insert some 10 rows.
  }

  @After
  public void teardown() throws Exception {
    // 1. TODO: Delete all the contents.
    // 2. Close the Spanner connection.
    spanner.close();
  }

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
        Arrays.asList(makeInternalRow(1, "1", 2.5), makeInternalRow(2, "2", 5.0));
    List<InternalRow> gotRows = new ArrayList<>();

    CopyOnWriteArrayList<InternalRow> al = new CopyOnWriteArrayList<>();

    try (final BatchReadOnlyTransaction txn =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      // Not using executor.execute as controlling immediate termination
      // is non-granular and out of scope of these tests.
      Map<String, String> opts = SpannerUtilsTest.connectionProperties();
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
        } finally {
          sCtx.close();
        }
        al.forEach(gotRows::add);
      }
    }

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertEquals(expectRows, gotRows);
  }

  public InternalRow makeGamesRow(
      String playerId,
      String[] playerIds,
      String winner,
      Timestamp createdAt,
      Timestamp finishedAt) {
    GenericInternalRow row = new GenericInternalRow(5);
    row.update(0, UTF8String.fromString(playerId));
    List<UTF8String> dest = new ArrayList<UTF8String>(playerIds.length);
    for (String id : playerIds) {
      dest.add(UTF8String.fromString(id));
    }
    row.update(1, dest);
    row.update(2, UTF8String.fromString(winner));
    row.update(3, createdAt);
    row.update(4, finishedAt);
    return row;
  }

  @Test
  public void testMoreDiverseTables() {
    Map<String, String> props = SpannerUtilsTest.connectionProperties();
    props.put("table", "games");
    SpannerTable st = new SpannerTable(null, props);
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

    Timestamp createdAt = Timestamp.valueOf("2023-08-26 15:22:00");
    Timestamp finishedAt = Timestamp.valueOf("2023-08-26 15:22:00");
    List<InternalRow> expectRows =
        Arrays.asList(
            makeGamesRow("g1", new String[] {"p1", "p2", "p3"}, "T1", createdAt, finishedAt),
            makeGamesRow("g2", new String[] {"p4", "p5", "p6"}, "T2", createdAt, finishedAt));

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertEquals(expectRows, gotRows);
  }
}
