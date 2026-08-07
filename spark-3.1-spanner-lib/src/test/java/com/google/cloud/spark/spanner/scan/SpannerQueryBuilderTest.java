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
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import java.util.*;
import org.apache.spark.sql.sources.Filter;
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
    Set<String> columns = new HashSet<>(Arrays.asList("id"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEqualTo("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_multipleColumns() {
    Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`id`");
    assertThat(result).contains("`users`.`name`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_googleSql_columnMatchingTableName() {
    Set<String> columns = new HashSet<>(Arrays.asList("users", "id"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).contains("`users`.`users`");
    assertThat(result).contains("`users`.`id`");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_singleColumn() {
    Set<String> columns = new HashSet<>(Arrays.asList("id"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).isEqualTo("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_multipleColumns() {
    Set<String> columns = new HashSet<>(Arrays.asList("id", "name"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"id\"");
    assertThat(result).contains("\"users\".\"name\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_postgreSql_columnMatchingTableName() {
    Set<String> columns = new HashSet<>(Arrays.asList("users", "id"));
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, true);
    assertThat(result).contains("\"users\".\"users\"");
    assertThat(result).contains("\"users\".\"id\"");
  }

  @Test
  public void testBuildColumnsWithTablePrefix_emptyColumns() {
    Set<String> columns = new HashSet<>();
    String result = SpannerQueryBuilder.buildColumnsWithTablePrefix("users", columns, false);
    assertThat(result).isEmpty();
  }

  @Test
  public void testBuildStatement_indexHint_googleSql() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn(" IndexByA ");
    LogicalQuery logicalQuery =
        new LogicalQuery(
            mockSpannerTable, Collections.emptySet(), new Filter[] {}, new HashMap<>());
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL);
    Statement statement = spannerQueryBuilder.buildStatement();
    String stmt = statement.toString();
    assertThat(stmt).contains("@{FORCE_INDEX=IndexByA}");
  }

  @Test
  public void testBuildStatement_indexHintEmpty() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn(" ");
    LogicalQuery logicalQuery =
        new LogicalQuery(
            mockSpannerTable, Collections.emptySet(), new Filter[] {}, new HashMap<>());
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL);
    SpannerConnectorException e =
        assertThrows(SpannerConnectorException.class, spannerQueryBuilder::buildStatement);
    assertThat(e.getMessage()).contains("Missing indexHint");
  }

  @Test
  public void testBuildStatement_indexHint_postgresql() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
    when(mockProperties.get("indexHint")).thenReturn("IndexByA");
    LogicalQuery logicalQuery =
        new LogicalQuery(
            mockSpannerTable, Collections.emptySet(), new Filter[] {}, new HashMap<>());
    SpannerQueryBuilder spannerQueryBuilder =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.POSTGRESQL);
    Statement statement = spannerQueryBuilder.buildStatement();
    String stmt = statement.toString();
    assertThat(stmt).contains("/*@ FORCE_INDEX=IndexByA */");
  }
}
