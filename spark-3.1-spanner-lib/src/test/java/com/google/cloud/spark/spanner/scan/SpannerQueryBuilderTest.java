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

package com.google.cloud.spark.spanner.scan;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.planning.expression.ColumnExpr;
import com.google.cloud.spark.spanner.planning.expression.EqExpr;
import com.google.cloud.spark.spanner.planning.query.ColumnResolution;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.JoinType;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import java.util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for SpannerQueryBuilder.buildColumnsWithTablePrefix() */
@RunWith(JUnit4.class)
public class SpannerQueryBuilderTest {

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_singleColumn() {
    List<String> columns = Arrays.asList("id");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEqualTo("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_multipleColumns() {
    List<String> columns = Arrays.asList("id", "name");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`id`");
    assertThat(result).contains("`users`.`name`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_columnMatchingTableName() {
    List<String> columns = Arrays.asList("users", "id");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`users`");
    assertThat(result).contains("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_singleColumn() {
    List<String> columns = Arrays.asList("id");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).isEqualTo("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_multipleColumns() {
    List<String> columns = Arrays.asList("id", "name");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"id\"");
    assertThat(result).contains("\"users\".\"name\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_columnMatchingTableName() {
    List<String> columns = Arrays.asList("users", "id");
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"users\"");
    assertThat(result).contains("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_emptyColumns() {
    List<String> columns = new ArrayList<>();
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEmpty();
  }

  @Test
  public void testBuildStatement_indexHint_legacy_builder_googleSql() {
    testBuildStatement_indexHint_legacy_builder(
        Dialect.GOOGLE_STANDARD_SQL, "@{FORCE_INDEX=IndexByA}");
  }

  @Test
  public void testBuildStatement_indexHint_legacy_builder_postgresql() {
    testBuildStatement_indexHint_legacy_builder(Dialect.POSTGRESQL, "/*@ FORCE_INDEX=IndexByA */");
  }

  private void testBuildStatement_indexHint_legacy_builder(
      Dialect dialect, String expectedIndexHint) {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn(" IndexByA");
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(new TableRelation("mockSpannerTable", null, mockSpannerTable))
            .build();
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, dialect, false);
    Statement statement = spannerQueryBuilder.buildStatement();
    String stmt = statement.toString();
    assertThat(stmt).contains(expectedIndexHint);
  }

  @Test
  public void testBuildStatement_indexHintEmpty_legacy_builder_googleSql() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn(" ");
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(new TableRelation("mockSpannerTable", null, mockSpannerTable))
            .build();
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL, false);
    SpannerConnectorException e =
        assertThrows(SpannerConnectorException.class, spannerQueryBuilder::buildStatement);
    assertThat(e.getMessage()).contains("Missing indexHint");
  }

  @Test
  public void testBuildStatement_indexHintJoin_new_builder_googleSql() {
    testBuildStatement_indexHintJoin_new_builder(
        Dialect.GOOGLE_STANDARD_SQL, "@{FORCE_INDEX=IndexByA}");
  }

  @Test
  public void testBuildStatement_indexHintJoin_new_builder_postgresql() {
    testBuildStatement_indexHintJoin_new_builder(Dialect.POSTGRESQL, "/*@ FORCE_INDEX=IndexByA */");
  }

  public void testBuildStatement_indexHintJoin_new_builder(
      Dialect dialect, String expectedIndexHint) {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn("IndexByA");
    Map<String, ColumnResolution> columnResolution = new HashMap<>();
    columnResolution.put(
        "A", new ColumnResolution("A", "A", "mockSpannerTable", DataTypes.StringType, true));
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(
                new JoinRelation(
                    new TableRelation("mockLeftSpannerTable", null, mockSpannerTable),
                    new TableRelation("mockRightSpannerTable", null, mockSpannerTable),
                    JoinType.INNER,
                    new EqExpr(
                        new ColumnExpr("mockLeftSpannerTable", "A", DataTypes.LongType, false),
                        new ColumnExpr("mockRightSpannerTable", "A", DataTypes.LongType, false))))
            .resolutionMap(columnResolution)
            .build();
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, dialect, true);
    Statement statement = spannerQueryBuilder.buildStatement();
    String stmt = statement.toString();
    assertThat(stmt).contains(expectedIndexHint);
  }
}
