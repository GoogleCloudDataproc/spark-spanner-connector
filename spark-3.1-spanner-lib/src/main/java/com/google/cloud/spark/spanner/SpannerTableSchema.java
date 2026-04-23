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
package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.connection.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SpannerTableSchema {

  private static final String QUERY_PREFIX =
      "SELECT COLUMN_NAME, IS_NULLABLE='YES' AS ISNULLABLE, SPANNER_TYPE "
          + "FROM INFORMATION_SCHEMA.COLUMNS WHERE ";
  private static final String QUERY_SUFFIX = " ORDER BY ORDINAL_POSITION";
  private static final String GOOGLESQL_SCHEMA =
      QUERY_PREFIX + "UPPER(TABLE_NAME)=UPPER(@tableName)" + QUERY_SUFFIX;
  private static final String POSTGRESQL_SCHEMA =
      QUERY_PREFIX + "columns.table_name=$1" + QUERY_SUFFIX;

  private final Map<String, StructField> columns;

  public final String name;
  public final StructType schema;

  static Statement buildSchemaQuery(String tableName, boolean isPostgreSql) {
    if (isPostgreSql) {
      return Statement.newBuilder(POSTGRESQL_SCHEMA).bind("p1").to(tableName).build();
    } else {
      return Statement.newBuilder(GOOGLESQL_SCHEMA).bind("tableName").to(tableName).build();
    }
  }

  public SpannerTableSchema(Connection conn, String tableName, boolean isPostgreSql) {
    this.name = tableName;
    this.columns = new HashMap<>();
    // 1. Get the primary keys for the table.
    Set<String> primaryKeys = getPrimaryKeys(conn, tableName, isPostgreSql);

    // 2. Get the schema of the table.
    Statement stmt = buildSchemaQuery(tableName, isPostgreSql);
    try (final ResultSet rs = conn.executeQuery(stmt)) {
      // Expecting resultset columns in the ordering:
      //       COLUMN_NAME, IS_NULLABLE, SPANNER_TYPE
      // row1:
      // ...
      // rowN:
      StructType schema = new StructType();
      while (rs.next()) {
        Struct row = rs.getCurrentRowAsStruct();
        String columnName = row.getString(0);
        boolean isPrimaryKey = primaryKeys.contains(columnName);
        StructField structField =
            getSparkStructField(
                columnName, row.getString(2), row.getBoolean(1), isPostgreSql, isPrimaryKey);
        schema = schema.add(structField);
        this.columns.put(columnName, structField);
      }
      this.schema = schema;
    }
  }

  static Statement buildPrimaryKeyQuery(String tableName, boolean isPostgreSql) {
    if (isPostgreSql) {
      String sql =
          "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc "
              + "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu "
              + "ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_NAME = kcu.TABLE_NAME "
              + "WHERE tc.TABLE_NAME = $1 AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_SCHEMA = 'public' "
              + "ORDER BY kcu.ORDINAL_POSITION";
      return Statement.newBuilder(sql).bind("p1").to(tableName.toLowerCase(Locale.ROOT)).build();
    }
    String sql =
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS "
            + "WHERE INDEX_NAME = 'PRIMARY_KEY' AND UPPER(TABLE_NAME) = UPPER(@tableName)";
    return Statement.newBuilder(sql).bind("tableName").to(tableName).build();
  }

  private Set<String> getPrimaryKeys(Connection conn, String tableName, boolean isPostgreSql) {
    Statement stmt = buildPrimaryKeyQuery(tableName, isPostgreSql);
    Set<String> primaryKeys = new HashSet<>();
    try (final ResultSet rs = conn.executeQuery(stmt)) {
      while (rs.next()) {
        primaryKeys.add(rs.getString(0));
      }
    }
    return primaryKeys;
  }

  public static StructField getSparkStructField(
      String name, String spannerType, boolean isNullable, boolean isPostgreSql) {
    return getSparkStructField(name, spannerType, isNullable, isPostgreSql, false);
  }

  public static StructField getSparkStructField(
      String name,
      String spannerType,
      boolean isNullable,
      boolean isPostgreSql,
      boolean isPrimaryKey) {
    DataType catalogType =
        isPostgreSql
            ? SpannerTable.ofSpannerStrTypePg(spannerType, isNullable)
            : SpannerTable.ofSpannerStrType(spannerType, isNullable);
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (isJson(spannerType)) {
      metadataBuilder.putString(SpannerUtils.COLUMN_TYPE, "json");
    } else if (isJsonb(spannerType)) {
      metadataBuilder.putString(SpannerUtils.COLUMN_TYPE, "jsonb");
    }
    if (isPrimaryKey) {
      metadataBuilder.putBoolean(SpannerUtils.PRIMARY_KEY_TAG, true);
    }
    return new StructField(name, catalogType, isNullable, metadataBuilder.build());
  }

  public StructField getStructFieldForColumn(String columnName) {
    return Objects.requireNonNull(columns.get(columnName));
  }

  public static boolean isJson(String spannerStrType) {
    return "json".equalsIgnoreCase(spannerStrType.trim());
  }

  public static boolean isJsonb(String spannerStrType) {
    return "jsonb".equalsIgnoreCase(spannerStrType.trim());
  }
}
