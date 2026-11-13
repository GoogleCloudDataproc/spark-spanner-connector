// Copyright 2026 Google LLC
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

package com.google.cloud.spark.spanner.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spark.spanner.scan.SpannerTable;
import com.google.cloud.spark.spanner.scan.Spark41SpannerScanBuilder;
import com.google.cloud.spark.spanner.scan.Spark41SpannerTable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias;
import org.apache.spark.sql.functions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark42SpannerScanBuilderIntegrationTest
    extends SpannerScanBuilderIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(Spark42SpannerScanBuilderIntegrationTest.class);

  private SpannerTable getSpannerTable(boolean usePostgreSql) {
    Map<String, String> connectionProperties = connectionProperties(usePostgreSql);
    connectionProperties.put("enablePredicateSql", "true");
    return new Spark41SpannerTable(connectionProperties);
  }

  private SpannerTable getSpannerTable(String tableName, boolean usePostgreSql) {
    Map<String, String> connectionProperties = connectionProperties(usePostgreSql);
    connectionProperties.put("table", tableName);
    connectionProperties.put("enablePredicateSql", "true");
    return new Spark41SpannerTable(connectionProperties);
  }

  @Override
  public Spark41SpannerScanBuilder createSpannerScanBuilder(boolean usePostgreSql) {
    logger.info("Creating Spark41SpannerScanBuilder");
    return new Spark41SpannerScanBuilder(getSpannerTable(usePostgreSql));
  }

  @Override
  public Spark41SpannerScanBuilder createSpannerScanBuilder(
      String tableName, boolean usePostgreSql) {
    logger.info("Creating Spark41SpannerScanBuilder");
    return new Spark41SpannerScanBuilder(getSpannerTable(tableName, usePostgreSql));
  }

  @Test
  public void isOtherSideCompatibleForJoinTest() throws Exception {
    Spark41SpannerScanBuilder ordersSpannerScanBuilder = createSpannerScanBuilder("ORDERS", false);
    Spark41SpannerScanBuilder lineitemSpannerScanBuilder =
        createSpannerScanBuilder("LINEITEM", false);
    Spark41SpannerScanBuilder aTableSpannerScanBuilder = createSpannerScanBuilder("ATable", false);

    assertTrue(ordersSpannerScanBuilder.isOtherSideCompatibleForJoin(lineitemSpannerScanBuilder));
    assertTrue(lineitemSpannerScanBuilder.isOtherSideCompatibleForJoin(ordersSpannerScanBuilder));
    // Tables are not interleaved but exist in same instance and database,
    assertTrue(lineitemSpannerScanBuilder.isOtherSideCompatibleForJoin(aTableSpannerScanBuilder));
  }

  @Test
  public void pushDownJoinTest() throws Exception {
    Spark41SpannerScanBuilder leftSpannerScanBuilder = createSpannerScanBuilder("ORDERS", false);
    Spark41SpannerScanBuilder rightSpannerScanBuilder = createSpannerScanBuilder("LINEITEM", false);

    assertTrue(leftSpannerScanBuilder.isOtherSideCompatibleForJoin(rightSpannerScanBuilder));
    assertTrue(rightSpannerScanBuilder.isOtherSideCompatibleForJoin(leftSpannerScanBuilder));

    ColumnWithAlias[] leftColumns =
        new ColumnWithAlias[] {
          new ColumnWithAlias("O_ORDERKEY", null),
          new ColumnWithAlias("O_CUSTKEY", null),
          new ColumnWithAlias("O_ORDERSTATUS", null),
          new ColumnWithAlias("O_TOTALPRICE", null),
          new ColumnWithAlias("O_ORDERDATE", null),
          new ColumnWithAlias("O_ORDERPRIORITY", null),
          new ColumnWithAlias("O_CLERK", null),
          new ColumnWithAlias("O_SHIPPRIORITY", null),
          new ColumnWithAlias("O_COMMENT", null)
        };
    ColumnWithAlias[] rightColumns =
        new ColumnWithAlias[] {
          new ColumnWithAlias("O_ORDERKEY", "O_ORDERKEY_1"),
          new ColumnWithAlias("L_LINENUMBER", null)
        };
    NamedReference left = FieldReference.column("O_ORDERKEY");

    NamedReference right = FieldReference.column("O_ORDERKEY_1");

    Predicate predicate = new Predicate("=", new Expression[] {left, right});

    assertTrue(
        leftSpannerScanBuilder.pushDownJoin(
            rightSpannerScanBuilder, JoinType.INNER_JOIN, leftColumns, rightColumns, predicate));
  }

  @Test
  public void endToEndDataFrameJoinPushdownTest() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("Spark42JoinPushdownIntegrationTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.optimizer.datasourceV2JoinPushdown", "true")
            .getOrCreate();
    try {
      Map<String, String> ordersProps = connectionProperties(false);
      ordersProps.put("table", "ORDERS");
      ordersProps.put("enablePredicateSql", "true");

      Map<String, String> lineitemProps = connectionProperties(false);
      lineitemProps.put("table", "LINEITEM");
      lineitemProps.put("enablePredicateSql", "true");

      Dataset<Row> orders = spark.read().format("cloud-spanner").options(ordersProps).load();
      Dataset<Row> lineitem = spark.read().format("cloud-spanner").options(lineitemProps).load();

      Dataset<Row> joined =
          orders
              .alias("o")
              .join(
                  lineitem.alias("l"),
                  functions.col("o.O_ORDERKEY").equalTo(functions.col("l.O_ORDERKEY")),
                  "inner")
              .select(
                  functions.col("o.O_ORDERKEY"),
                  functions.col("o.O_ORDERSTATUS"),
                  functions.col("l.L_LINENUMBER"));

      String optimizedPlan = joined.queryExecution().optimizedPlan().toString();
      assertThat(optimizedPlan).contains("RelationV2");
      assertThat(optimizedPlan).doesNotContain("Join ");

      Map<String, String> uuidParentProps = connectionProperties(false);
      uuidParentProps.put("table", "UUID_PARENT");
      uuidParentProps.put("enablePredicateSql", "true");

      Map<String, String> uuidChildProps = connectionProperties(false);
      uuidChildProps.put("table", "UUID_CHILD");
      uuidChildProps.put("enablePredicateSql", "true");

      Dataset<Row> uuidJoined =
          spark
              .read()
              .format("cloud-spanner")
              .options(uuidParentProps)
              .load()
              .alias("p")
              .join(
                  spark.read().format("cloud-spanner").options(uuidChildProps).load().alias("c"),
                  functions.col("p.PARENT_ID").equalTo(functions.col("c.PARENT_ID")),
                  "inner")
              .select(functions.col("p.PARENT_ID"), functions.col("c.CHILD_ID"));

      String uuidOptimizedPlan = uuidJoined.queryExecution().optimizedPlan().toString();
      assertThat(uuidOptimizedPlan).contains("RelationV2");
      assertThat(uuidOptimizedPlan).doesNotContain("Join ");

      // Spanner emulator rejects PartitionQuery on JOIN statements; execute row collection when
      // running against real Spanner.
      if (emulatorHost == null || emulatorHost.isEmpty()) {
        List<Row> rows = joined.collectAsList();
        assertThat(rows).hasSize(13);
        assertThat(uuidJoined.collectAsList()).hasSize(3);
      }
    } finally {
      spark.stop();
    }
  }
}
