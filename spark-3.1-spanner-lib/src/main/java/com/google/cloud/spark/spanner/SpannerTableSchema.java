package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.connection.Connection;
import java.util.HashMap;
import java.util.Map;
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
      QUERY_PREFIX + "TABLE_NAME=@tableName" + QUERY_SUFFIX;
  private static final String POSTGRESQL_SCHEMA =
      QUERY_PREFIX + "columns.table_name=$1" + QUERY_SUFFIX;

  private final Map<String, StructField> columns;

  public final String name;
  public final StructType schema;

  public SpannerTableSchema(Connection conn, String tableName, boolean isPostgreSql) {
    this.name = tableName;
    this.columns = new HashMap<>();
    Statement stmt;
    if (isPostgreSql) {
      stmt = Statement.newBuilder(POSTGRESQL_SCHEMA).bind("p1").to(tableName).build();
    } else {
      stmt = Statement.newBuilder(GOOGLESQL_SCHEMA).bind("tableName").to(tableName).build();
    }
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
        StructField structField =
            getSparkStructField(columnName, row.getString(2), row.getBoolean(1), isPostgreSql);
        schema = schema.add(structField);
        this.columns.put(columnName, structField);
      }
      this.schema = schema;
    }
  }

  public static StructField getSparkStructField(
      String name, String spannerType, boolean isNullable, boolean isPostgreSql) {
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
    return new StructField(name, catalogType, isNullable, metadataBuilder.build());
  }

  public static boolean isJson(String spannerStrType) {
    return "json".equalsIgnoreCase(spannerStrType.trim());
  }

  public static boolean isJsonb(String spannerStrType) {
    return "jsonb".equalsIgnoreCase(spannerStrType.trim());
  }
}
