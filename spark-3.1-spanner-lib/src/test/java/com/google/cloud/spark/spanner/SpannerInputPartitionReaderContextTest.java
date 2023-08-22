package com.google.cloud.spark;

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
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
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
    // 1. Insert some 10 rows.
  }

  @After
  public void teardown() throws Exception {
    // 1. Delete all the contents.
    spanner.close();
  }

  @Test
  public void testCreatePartitionContext() throws Exception {
    String sqlStmt = "SELECT * FROM ATable";

    try (final BatchReadOnlyTransaction txn =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<Partition> partitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      SpannerInputPartitionContext sCtx = new SpannerInputPartitionContext(txn, partitions.get(0));
      try (InputPartitionReaderContext<InternalRow> ctx = sCtx.createPartitionReaderContext()) {
        while (ctx.next()) {
          InternalRow row = ctx.get();
          System.out.println("Row " + row.toString());
        }
      }
    }
  }
}
