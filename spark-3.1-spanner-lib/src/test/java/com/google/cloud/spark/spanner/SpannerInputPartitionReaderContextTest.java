// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
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
            makeInternalRow(7, "7", -0.1),
            makeInternalRow(8, "8", +0.1),
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
    row.update(3, SpannerUtils.zonedDateTimeToSparkTimestamp(createdAt));
    row.update(4, SpannerUtils.zonedDateTimeToSparkTimestamp(finishedAt));
    row.update(5, SpannerUtils.zonedDateTimeToSparkDate(maxDate));
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
    SpannerScanner ss = ((SpannerScanner) ssb.build());
    InputPartition[] parts = ss.planInputPartitions();
    PartitionReaderFactory prf = ss.createReaderFactory();

    List<InternalRow> gotRows = new ArrayList<>();
    for (InputPartition part : parts) {
      PartitionReader<InternalRow> ir = prf.createReader(part);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          gotRows.add(row);
        }
        SpannerPartitionReader sr = ((SpannerPartitionReader) ir);
        sr.close();
      } catch (IOException e) {
      }
    }

    if (prf instanceof SpannerInputPartitionReaderContext) {
      try {
        SpannerInputPartitionReaderContext spc = ((SpannerInputPartitionReaderContext) prf);
        spc.close();
      } catch (IOException e) {
      }
    }

    ZonedDateTime createdAt = ZonedDateTime.parse("2023-08-26T12:22:00Z");
    ZonedDateTime finishedAt = ZonedDateTime.parse("2023-08-26T12:22:00Z");
    ZonedDateTime maxDate = ZonedDateTime.parse("2023-12-30T23:59:59Z");
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
  public void testArraysConversions() throws Exception {
    Map<String, String> props = this.connectionProperties();
    props.put("table", "compositeTable");
    SpannerTable st = new SpannerTable(props);
    CaseInsensitiveStringMap csm = new CaseInsensitiveStringMap(props);
    ScanBuilder sb = st.newScanBuilder(csm);
    SpannerScanBuilder ssb = ((SpannerScanBuilder) sb);
    SpannerScanner ss = ((SpannerScanner) ssb.build());
    InputPartition[] parts = ss.planInputPartitions();
    PartitionReaderFactory prf = ss.createReaderFactory();

    List<InternalRow> gotRows = new ArrayList<>();
    for (InputPartition part : parts) {
      PartitionReader<InternalRow> ir = prf.createReader(part);
      try {
        while (ir.next()) {
          InternalRow row = ir.get();
          gotRows.add(row);
        }
        SpannerPartitionReader sr = ((SpannerPartitionReader) ir);
        sr.close();
      } catch (IOException e) {
      }
    }

    if (prf instanceof SpannerInputPartitionReaderContext) {
      SpannerInputPartitionReaderContext spc = ((SpannerInputPartitionReaderContext) prf);
      spc.close();
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
                },
                "beefdead"),
            makeCompositeTableRow(
                "id2",
                new long[] {20, 200, 2991, 888885},
                new String[] {"A", "B", "C"},
                "this one",
                new java.math.BigDecimal(93411),
                ZonedDateTime.parse("2023-09-23T00:00:00Z"),
                ZonedDateTime.parse("2023-09-22T12:22:05Z"),
                false,
                new ZonedDateTime[] {
                  ZonedDateTime.parse("2023-09-02T00:00:00Z"),
                  ZonedDateTime.parse("2023-12-31T00:00:00Z"),
                },
                new ZonedDateTime[] {
                  ZonedDateTime.parse("2023-09-22T12:11:10Z"),
                  ZonedDateTime.parse("2023-09-23T12:11:09Z"),
                },
                "deadbeef"));

    Comparator<InternalRow> cmp = new InternalRowComparator();
    Collections.sort(expectRows, cmp);
    Collections.sort(gotRows, cmp);

    assertEquals(expectRows.size(), gotRows.size());
    assertInternalRow(gotRows, expectRows);
  }

  private static void assertInternalRow(
      List<InternalRow> actualRows, List<InternalRow> expectedRows) {
    assertEquals(expectedRows.size(), actualRows.size());
    for (int i = 0; i < actualRows.size(); i++) {
      // We cannot use assertEqual for the whole List, since the byte[] will be
      // compared with the object's address.
      GenericInternalRow actualRow = (GenericInternalRow) actualRows.get(i);
      GenericInternalRow expectedRow = (GenericInternalRow) expectedRows.get(i);

      assertThat(actualRow.getUTF8String(0)).isEqualTo(expectedRow.getUTF8String(0));
      assertThat(actualRow.getArray(1)).isEqualTo(expectedRow.getArray(1));
      assertThat(actualRow.getArray(2)).isEqualTo(expectedRow.getArray(2));
      assertThat(actualRow.getUTF8String(3)).isEqualTo(expectedRow.getUTF8String(3));
      assertThat(actualRow.getDecimal(4, 38, 9)).isEqualTo(expectedRow.getDecimal(4, 38, 9));
      assertThat(actualRow.getInt(5)).isEqualTo(expectedRow.getInt(5));
      assertThat(actualRow.getLong(6)).isEqualTo(expectedRow.getLong(6));
      assertThat(actualRow.getBoolean(7)).isEqualTo(expectedRow.getBoolean(7));
      assertThat(actualRow.getArray(8)).isEqualTo(expectedRow.getArray(8));
      assertThat(actualRow.getArray(9)).isEqualTo(expectedRow.getArray(9));
      assertThat(bytesToString(actualRow.getBinary(10)))
          .isEqualTo(bytesToString(expectedRow.getBinary(10)));
    }
  }

  private static String bytesToString(byte[] bytes) {
    return bytes == null ? "" : new String(bytes);
  }
}
