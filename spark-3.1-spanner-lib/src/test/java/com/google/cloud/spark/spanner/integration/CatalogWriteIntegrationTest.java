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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spark.spanner.TestData;
import java.util.Arrays;
import java.util.Collection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CatalogWriteIntegrationTest extends SparkCatalogSpannerIntegrationTestBase {
  private final boolean usePostgresSql;

  public CatalogWriteIntegrationTest(boolean usePostgresSql) {
    super();
    this.usePostgresSql = usePostgresSql;
  }

  @Override
  protected boolean getUsePostgreSql() {
    return usePostgresSql;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> usePostgresSqlValues() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  @Test
  public void testCreateTableWithArrayColumns() {
    String tableName = TestData.WRITE_TABLE_NAME + "_ARR_DDL";
    spark.sql("DROP TABLE IF EXISTS spanner." + tableName);

    // 1. Create the table with array columns.
    spark.sql(
        "CREATE TABLE spanner."
            + tableName
            + " (id BIGINT NOT NULL, long_array ARRAY<BIGINT>, str_array ARRAY<STRING>) USING `cloud-spanner`"
            + " TBLPROPERTIES('primaryKeys' = 'id')");

    // 2. Insert data with array values.
    spark.sql(
        "INSERT INTO spanner."
            + tableName
            + " VALUES (1, ARRAY(CAST(10 AS BIGINT), CAST(20 AS BIGINT), CAST(30 AS BIGINT)), ARRAY('hello', 'world'))");

    // 3. Verify the data.
    Dataset<Row> readBack = spark.sql("SELECT * FROM spanner." + tableName).filter("id = 1");
    assertEquals(1, readBack.count());

    Row row = readBack.first();
    assertThat(row.getLong(0)).isEqualTo(1L);
    Long[] longArr = new Long[] {10L, 20L, 30L};
    Object[] strArr = new Object[] {"hello", "world"};
    assertArrayEquals(longArr, row.getList(1).toArray(new Long[0]));
    assertArrayEquals(strArr, row.getList(2).toArray());
  }

  @Test
  public void testIgnoreSaveMode() {
    String tableName = TestData.WRITE_TABLE_NAME + "_IGNORE";
    spark.sql("DROP TABLE IF EXISTS spanner." + tableName);

    // 1. Create the table.
    spark.sql(
        "CREATE TABLE spanner."
            + tableName
            + " (long_col BIGINT NOT NULL, string_col STRING) USING `cloud-spanner`"
            + " TBLPROPERTIES('primaryKeys' = 'long_col')");

    // 2. Insert initial data.
    spark.sql("INSERT INTO spanner." + tableName + " VALUES (501, 'initial-data')");

    // 3. Verify the initial write.
    Dataset<Row> dfAfterInitialWrite = spark.sql("SELECT * FROM spanner." + tableName);
    assertEquals(1, dfAfterInitialWrite.count());
    assertEquals("initial-data", dfAfterInitialWrite.first().getString(1));

    // 4. CREATE TABLE IF NOT EXISTS is the catalog Ignore mode. This should be a no-op since the
    // table already exists. There is no DataFrameWriterV2 equivalent for Ignore semantics.
    spark.sql(
        "CREATE TABLE IF NOT EXISTS spanner."
            + tableName
            + " (long_col BIGINT NOT NULL, string_col STRING) USING `cloud-spanner`"
            + " TBLPROPERTIES('primaryKeys' = 'long_col')");

    // 5. Verify that the table content is unchanged.
    Dataset<Row> finalDf = spark.sql("SELECT * FROM spanner." + tableName);
    assertEquals(1, finalDf.count());
    Row finalRow = finalDf.first();
    assertEquals(501L, finalRow.getLong(0));
    assertEquals("initial-data", finalRow.getString(1));
  }

  @Test
  public void testErrorIfExists() {
    String tableName = TestData.WRITE_TABLE_NAME + "_EIE";
    spark.sql("DROP TABLE IF EXISTS spanner." + tableName);

    // 1. Create the table.
    spark.sql(
        "CREATE TABLE spanner."
            + tableName
            + " (long_col BIGINT NOT NULL, string_col STRING) USING `cloud-spanner`"
            + " TBLPROPERTIES('primaryKeys' = 'long_col')");

    // 2. Insert initial data.
    spark.sql("INSERT INTO spanner." + tableName + " VALUES (301, 'three-oh-one')");

    // 3. writeTo().create() is the catalog ErrorIfExists: it should fail since the table exists.
    Dataset<Row> newDf =
        spark.sql("SELECT CAST(302 AS BIGINT) AS long_col, 'three-oh-two' AS string_col");
    try {
      newDf.writeTo("spanner." + tableName).create();
      fail("Expected exception was not thrown");
    } catch (Exception e) {
      assertTrue(
          "Expected exception message about table already exists, but got: " + e.getMessage(),
          e.getMessage().contains("already exists"));
    }
  }
}
