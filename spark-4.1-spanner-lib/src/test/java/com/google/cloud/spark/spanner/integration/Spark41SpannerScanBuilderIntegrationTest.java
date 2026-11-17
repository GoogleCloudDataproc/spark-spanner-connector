// Copyright 2025 Google LLC
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

import com.google.cloud.spark.spanner.scan.SpannerScanner;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import com.google.cloud.spark.spanner.scan.Spark41SpannerScanBuilder;
import com.google.cloud.spark.spanner.scan.Spark41SpannerTable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.functions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.javaapi.CollectionConverters;

public class Spark41SpannerScanBuilderIntegrationTest
    extends SpannerScanBuilderIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(Spark41SpannerScanBuilderIntegrationTest.class);

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

  private static boolean containsJoinNode(LogicalPlan plan) {
    if (plan instanceof Join) {
      return true;
    }
    for (LogicalPlan child : CollectionConverters.asJava(plan.children())) {
      if (containsJoinNode(child)) {
        return true;
      }
    }
    return false;
  }

  private static void assertJoinPushedDown(
      LogicalPlan optimizedPlan, String... expectedScanColumns) {
    assertThat(containsJoinNode(optimizedPlan)).isFalse();
    List<LogicalPlan> leaves = CollectionConverters.asJava(optimizedPlan.collectLeaves());
    assertThat(leaves).hasSize(1);
    assertThat(leaves.get(0)).isInstanceOf(DataSourceV2ScanRelation.class);
    DataSourceV2ScanRelation scanRelation = (DataSourceV2ScanRelation) leaves.get(0);
    assertThat(scanRelation.scan()).isInstanceOf(SpannerScanner.class);
    assertThat(Arrays.asList(scanRelation.scan().readSchema().fieldNames()))
        .containsAtLeastElementsIn(expectedScanColumns);
  }

  @Test
  public void endToEndDataFrameJoinPushdownTest() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("JoinPushdownIntegrationTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.optimizer.datasourceV2JoinPushdown", "true")
            .getOrCreate();
    spark.conf().set("spark.sql.optimizer.datasourceV2JoinPushdown", "true");
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

      assertJoinPushedDown(
          joined.queryExecution().optimizedPlan(), "O_ORDERKEY", "O_ORDERSTATUS", "L_LINENUMBER");

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

      assertJoinPushedDown(uuidJoined.queryExecution().optimizedPlan(), "PARENT_ID", "CHILD_ID");

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
