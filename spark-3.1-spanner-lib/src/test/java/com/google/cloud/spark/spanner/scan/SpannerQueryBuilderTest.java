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
import com.google.cloud.spanner.Value;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.cloud.spark.spanner.planning.expression.ColumnExpr;
import com.google.cloud.spark.spanner.planning.expression.EqExpr;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.planning.query.ColumnResolution;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.JoinRelation;
import com.google.cloud.spark.spanner.planning.relation.JoinType;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.rendering.SpannerQueryBuilder;
import java.math.BigDecimal;
import java.util.*;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
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
        "A",
        new ColumnResolution("A", "A", "mockSpannerTable", DataTypes.StringType, "STRING", true));
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

  @Test
  public void testBuildColumnsWithTablePrefix_escapesIdentifierDelimiters() {
    String gsqlResult =
        SpannerQueryBuilder.buildColumnsWithTablePrefix(
            "my`table", Arrays.asList("col`1", "col2"), false);
    assertThat(gsqlResult).isEqualTo("`my``table`.`col``1`, `my``table`.`col2`");

    String pgResult =
        SpannerQueryBuilder.buildColumnsWithTablePrefix(
            "my\"table", Arrays.asList("col\"1", "col2"), true);
    assertThat(pgResult).isEqualTo("\"my\"\"table\".\"col\"\"1\", \"my\"\"table\".\"col2\"");
  }

  @Test
  public void testBuildStatement_legacyBuilder_bindsFilterParameters_googleSql() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("users");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    Map<String, StructField> fields = new HashMap<>();
    fields.put("name", new StructField("name", DataTypes.StringType, true, Metadata.empty()));
    Filter[] filters =
        new Filter[] {
          new EqualTo("name", "\\' OR TRUE --"), new StringStartsWith("name", "foo%_\\")
        };
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(new TableRelation("users", null, mockSpannerTable))
            .pushedFilters(filters)
            .fields(fields)
            .build();
    Statement statement =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL, false)
            .buildStatement();
    assertThat(statement.getSql())
        .isEqualTo("SELECT * FROM `users` WHERE (`name` = @p1 AND `name` LIKE @p2)");
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.string("\\' OR TRUE --"));
    assertThat(statement.getParameters().get("p2")).isEqualTo(Value.string("foo\\%\\_\\\\%"));
  }

  @Test
  public void testBuildStatement_legacyBuilder_bindsFilterParameters_postgresql() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("users");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    Map<String, StructField> fields = new HashMap<>();
    fields.put("name", new StructField("name", DataTypes.StringType, true, Metadata.empty()));
    Filter[] filters =
        new Filter[] {new EqualTo("name", "' OR '1'='1"), new StringStartsWith("name", "foo%_\\")};
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(new TableRelation("users", null, mockSpannerTable))
            .pushedFilters(filters)
            .fields(fields)
            .build();
    Statement statement =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.POSTGRESQL, false).buildStatement();
    assertThat(statement.getSql())
        .isEqualTo("SELECT * FROM \"users\" WHERE (\"name\" = $1 AND \"name\" LIKE $2)");
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.string("' OR '1'='1"));
    assertThat(statement.getParameters().get("p2")).isEqualTo(Value.string("foo\\%\\_\\\\%"));
  }

  @Test
  public void testBuildStatement_newBuilder_sharesParameterRegistryBetweenJoinAndFilters() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    Map<String, ColumnResolution> columnResolution = new HashMap<>();
    columnResolution.put(
        "A",
        new ColumnResolution(
            "A", "A", "mockLeftSpannerTable", DataTypes.StringType, "STRING", true));
    columnResolution.put(
        "B",
        new ColumnResolution(
            "B", "B", "mockRightSpannerTable", DataTypes.StringType, "STRING", true));

    Map<String, StructField> fields = new HashMap<>();
    fields.put("A", new StructField("A", DataTypes.StringType, true, Metadata.empty()));
    fields.put("B", new StructField("B", DataTypes.StringType, true, Metadata.empty()));

    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(
                new JoinRelation(
                    new TableRelation("mockLeftSpannerTable", "leftAlias", mockSpannerTable),
                    new TableRelation("mockRightSpannerTable", "rightAlias", mockSpannerTable),
                    JoinType.INNER,
                    new EqExpr(
                        new ColumnExpr("leftAlias", "A", DataTypes.StringType, false),
                        new LiteralExpr("join_val", DataTypes.StringType, "STRING"))))
            .resolutionMap(columnResolution)
            .pushedFilters(new Filter[] {new EqualTo("A", "left_filter_val")})
            .pushedFiltersOther(new Filter[] {new EqualTo("B", "right_filter_val")})
            .fields(fields)
            .build();

    Statement statement =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL, true)
            .buildStatement();
    assertThat(statement.getSql()).contains("ON `leftAlias`.`A` = @p1");
    assertThat(statement.getSql())
        .contains("WHERE (`leftAlias`.`A` = @p2) AND (`rightAlias`.`B` = @p3)");
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.string("join_val"));
    assertThat(statement.getParameters().get("p2")).isEqualTo(Value.string("left_filter_val"));
    assertThat(statement.getParameters().get("p3")).isEqualTo(Value.string("right_filter_val"));
  }

  @Test
  public void testBuildStatement_indexHintSqlInjectionRejected() {
    List<String> maliciousHints =
        Arrays.asList(
            "_BASE_TABLE} WHERE 1=0 UNION ALL SELECT 1 --",
            "idx */ SELECT 1 --",
            "idx; DROP TABLE users",
            "\"quoted\"",
            "`backticked`");

    for (String payload : maliciousHints) {
      for (Dialect dialect : Dialect.values()) {
        if (dialect != Dialect.GOOGLE_STANDARD_SQL && dialect != Dialect.POSTGRESQL) {
          continue;
        }
        CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
        SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
        when(mockSpannerTable.name()).thenReturn("users");
        when(mockSpannerTable.properties()).thenReturn(mockProperties);
        when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);
        when(mockProperties.get("indexHint")).thenReturn(payload);

        LogicalQuery legacyQuery =
            LogicalQuery.builder()
                .source(new TableRelation("users", null, mockSpannerTable))
                .build();
        SpannerConnectorException legacyEx =
            assertThrows(
                SpannerConnectorException.class,
                () -> SpannerQueryBuilder.newBuilder(legacyQuery, dialect, false).buildStatement());
        assertThat(legacyEx.getErrorCode()).isEqualTo(SpannerErrorCode.INVALID_ARGUMENT);

        Map<String, ColumnResolution> resolution = new HashMap<>();
        resolution.put(
            "id", new ColumnResolution("id", "id", "users", DataTypes.LongType, "INT64", false));
        LogicalQuery newQuery =
            LogicalQuery.builder()
                .source(new TableRelation("users", "users", mockSpannerTable))
                .resolutionMap(resolution)
                .build();
        SpannerConnectorException newEx =
            assertThrows(
                SpannerConnectorException.class,
                () -> SpannerQueryBuilder.newBuilder(newQuery, dialect, true).buildStatement());
        assertThat(newEx.getErrorCode()).isEqualTo(SpannerErrorCode.INVALID_ARGUMENT);
      }
    }
  }

  @Test
  public void testBuildStatement_indexHintSupportsBaseTableAndSchemaQualifiedIndex() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("users");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    when(mockProperties.containsKey("indexHint")).thenReturn(Boolean.TRUE);

    when(mockProperties.get("indexHint")).thenReturn("_BASE_TABLE");
    LogicalQuery baseTableQuery =
        LogicalQuery.builder().source(new TableRelation("users", null, mockSpannerTable)).build();
    assertThat(
            SpannerQueryBuilder.newBuilder(baseTableQuery, Dialect.GOOGLE_STANDARD_SQL, false)
                .buildStatement()
                .getSql())
        .contains("@{FORCE_INDEX=_BASE_TABLE}");

    when(mockProperties.get("indexHint")).thenReturn("my_schema.idx_users");
    assertThat(
            SpannerQueryBuilder.newBuilder(baseTableQuery, Dialect.GOOGLE_STANDARD_SQL, false)
                .buildStatement()
                .getSql())
        .contains("@{FORCE_INDEX=\"my_schema.idx_users\"}");
    assertThat(
            SpannerQueryBuilder.newBuilder(baseTableQuery, Dialect.POSTGRESQL, false)
                .buildStatement()
                .getSql())
        .contains("/*@ FORCE_INDEX=my_schema.idx_users */");
  }

  @Test
  public void testBuildStatement_joinWithCollidingColumnNamesUsesSeparateRightFieldsMetadata() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("mockSpannerTable");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);

    MetadataBuilder jsonMeta = new MetadataBuilder();
    jsonMeta.putString(SpannerUtils.COLUMN_TYPE, "json");
    Map<String, StructField> leftFields = new HashMap<>();
    leftFields.put("data", new StructField("data", DataTypes.StringType, true, jsonMeta.build()));

    Map<String, StructField> rightFields = new HashMap<>();
    rightFields.put("data", new StructField("data", DataTypes.StringType, true, Metadata.empty()));

    Map<String, ColumnResolution> columnResolution = new HashMap<>();
    columnResolution.put(
        "id", new ColumnResolution("id", "id", "leftAlias", DataTypes.LongType, "INT64", false));

    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(
                new JoinRelation(
                    new TableRelation("ParentTable", "leftAlias", mockSpannerTable),
                    new TableRelation("ChildTable", "rightAlias", mockSpannerTable),
                    JoinType.INNER,
                    new EqExpr(
                        new ColumnExpr("leftAlias", "id", DataTypes.LongType, false),
                        new ColumnExpr("rightAlias", "id", DataTypes.LongType, false))))
            .resolutionMap(columnResolution)
            .pushedFilters(new Filter[] {new EqualTo("data", "{\"a\":1}")})
            .pushedFiltersOther(new Filter[] {new EqualTo("data", "plain_text")})
            .fields(leftFields)
            .fieldsOther(rightFields)
            .build();

    Statement statement =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.GOOGLE_STANDARD_SQL, true)
            .buildStatement();
    assertThat(statement.getSql())
        .contains(
            "WHERE (TO_JSON_STRING(`leftAlias`.`data`) = @p1) AND (`rightAlias`.`data` = @p2)");
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.string("{\"a\":1}"));
    assertThat(statement.getParameters().get("p2")).isEqualTo(Value.string("plain_text"));
  }

  @Test
  public void testBuildStatement_postgresqlBindsDecimalAsPgNumeric() {
    CaseInsensitiveStringMap mockProperties = Mockito.mock(CaseInsensitiveStringMap.class);
    SpannerTable mockSpannerTable = Mockito.mock(SpannerTable.class);
    when(mockSpannerTable.name()).thenReturn("ledger");
    when(mockSpannerTable.properties()).thenReturn(mockProperties);
    BigDecimal bd = new BigDecimal("1000000000000000000000000000000.50");
    LogicalQuery logicalQuery =
        LogicalQuery.builder()
            .source(new TableRelation("ledger", null, mockSpannerTable))
            .pushedFilters(new Filter[] {new EqualTo("amount", bd)})
            .fields(Collections.emptyMap())
            .build();
    Statement statement =
        SpannerQueryBuilder.newBuilder(logicalQuery, Dialect.POSTGRESQL, false).buildStatement();
    assertThat(statement.getSql()).isEqualTo("SELECT * FROM \"ledger\" WHERE (\"amount\" = $1)");
    assertThat(statement.getParameters().get("p1")).isEqualTo(Value.pgNumeric(bd.toPlainString()));
  }
}
