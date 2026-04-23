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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Statement;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for SpannerTableSchema.buildSchemaQuery() */
@RunWith(JUnit4.class)
public class SpannerTableSchemaTest {

  @Test
  public void testBuildSchemaQuery_googleSql_usesCaseInsensitiveComparison() {
    Statement stmt = SpannerTableSchema.buildSchemaQuery("MyTable", false);
    String query = stmt.getSql();

    // Verify GoogleSQL uses UPPER() for case-insensitive table name comparison
    assertThat(query).contains("UPPER(TABLE_NAME)=UPPER(@tableName)");
  }

  @Test
  public void testBuildSchemaQuery_googleSql_differentCasing() {
    // Test with different table name casings
    Statement stmt1 = SpannerTableSchema.buildSchemaQuery("mytable", false);
    Statement stmt2 = SpannerTableSchema.buildSchemaQuery("MyTable", false);
    Statement stmt3 = SpannerTableSchema.buildSchemaQuery("MYTABLE", false);

    // All should generate the same query structure with UPPER()
    assertThat(stmt1.getSql()).contains("UPPER(TABLE_NAME)=UPPER(@tableName)");
    assertThat(stmt2.getSql()).contains("UPPER(TABLE_NAME)=UPPER(@tableName)");
    assertThat(stmt3.getSql()).contains("UPPER(TABLE_NAME)=UPPER(@tableName)");
  }

  @Test
  public void testBuildSchemaQuery_postgreSql_usesDirectComparison() {
    Statement stmt = SpannerTableSchema.buildSchemaQuery("myTable", true);
    String query = stmt.getSql();

    // Verify PostgreSQL uses direct comparison without UPPER()
    assertThat(query).contains("columns.table_name=$1");
    assertThat(query).doesNotContain("UPPER");
  }

  @Test
  public void testBuildPrimaryKeyQuery_googleSql() {
    Statement stmt = SpannerTableSchema.buildPrimaryKeyQuery("MyTable", false);
    String query = stmt.getSql();
    assertThat(query).contains("INDEX_NAME = 'PRIMARY_KEY'");
    assertThat(query).contains("UPPER(TABLE_NAME) = UPPER(@tableName)");
  }

  @Test
  public void testBuildPrimaryKeyQuery_postgreSql() {
    Statement stmt = SpannerTableSchema.buildPrimaryKeyQuery("MyTable", true);
    String query = stmt.getSql();
    assertThat(query).contains("CONSTRAINT_TYPE = 'PRIMARY KEY'");
    assertThat(query).contains("TABLE_NAME = $1");
  }

  @Test
  public void testIsJson() {
    assertTrue(SpannerTableSchema.isJson("JSON"));
    assertTrue(SpannerTableSchema.isJson("json"));
    assertTrue(SpannerTableSchema.isJson(" json "));
    assertFalse(SpannerTableSchema.isJson("jsonb"));
    assertFalse(SpannerTableSchema.isJson("STRING"));
  }

  @Test
  public void testIsJsonb() {
    assertTrue(SpannerTableSchema.isJsonb("JSONB"));
    assertTrue(SpannerTableSchema.isJsonb("jsonb"));
    assertTrue(SpannerTableSchema.isJsonb(" jsonb "));
    assertFalse(SpannerTableSchema.isJsonb("json"));
    assertFalse(SpannerTableSchema.isJsonb("STRING"));
  }

  @Test
  public void testGetSparkStructField_basic() {
    StructField field = SpannerTableSchema.getSparkStructField("col", "INT64", true, false);
    assertEquals("col", field.name());
    assertEquals(DataTypes.LongType, field.dataType());
    assertTrue(field.nullable());
  }

  @Test
  public void testGetSparkStructField_primaryKey() {
    StructField field = SpannerTableSchema.getSparkStructField("id", "INT64", false, false, true);
    assertEquals("id", field.name());
    assertFalse(field.nullable());
    assertTrue(field.metadata().contains(SpannerUtils.PRIMARY_KEY_TAG));
    assertTrue(field.metadata().getBoolean(SpannerUtils.PRIMARY_KEY_TAG));
  }

  @Test
  public void testGetSparkStructField_jsonMetadata() {
    StructField field = SpannerTableSchema.getSparkStructField("data", "JSON", true, false);
    assertTrue(field.metadata().contains(SpannerUtils.COLUMN_TYPE));
    assertEquals("json", field.metadata().getString(SpannerUtils.COLUMN_TYPE));
  }

  @Test
  public void testGetSparkStructField_jsonbMetadata() {
    StructField field = SpannerTableSchema.getSparkStructField("data", "jsonb", true, true);
    assertTrue(field.metadata().contains(SpannerUtils.COLUMN_TYPE));
    assertEquals("jsonb", field.metadata().getString(SpannerUtils.COLUMN_TYPE));
  }
}
