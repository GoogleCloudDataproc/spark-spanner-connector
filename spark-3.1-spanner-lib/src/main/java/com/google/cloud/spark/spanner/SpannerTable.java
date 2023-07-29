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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SpannerTable implements Table {
  private String tableName;
  private StructType tableSchema;

  public SpannerTable(Map<String, String> properties) {
    String spannerUri =
        String.format(
            "cloudspanner:/projects/%s/instances/%s/databases/%s",
            properties.get("projectId"),
            properties.get("instanceId"),
            properties.get("databaseId"));

    ConnectionOptions.Builder builder = ConnectionOptions.newBuilder().setUri(spannerUri);
    String gcpCredsUrl = properties.get("credentials");
    if (gcpCredsUrl != null) {
      builder = builder.setCredentialsUrl(gcpCredsUrl);
    }
    ConnectionOptions opts = builder.build();

    try (Connection conn = opts.getConnection()) {
      String tableName = properties.get("table");
      // 3. Run an information schema query to get the type definition of the table.
      Statement stmt =
          Statement.newBuilder(
                  "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, SPANNER_TYPE "
                      + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@tableName "
                      + "ORDER BY ORDINAL_POSITION")
              .bind("tableName")
              .to(tableName)
              .build();
      try (final ResultSet rs = conn.executeQuery(stmt)) {
        this.tableSchema = createSchema(tableName, rs);
      }
    }
  }

  public StructType createSchema(String tableName, ResultSet rs) {
    this.tableName = tableName;

    Integer columnSize = rs.getColumnCount();
    // Expecting resultset columns in the ordering:
    //      COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, SPANNER_TYPE
    StructType schema = new StructType();
    while (rs.next()) {
      Struct row = rs.getCurrentRowAsStruct();

      for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
        String columnName = row.getString(0);
        // Integer ordinalPosition = column.getInt(1);
        boolean isNullable = row.getBoolean(2);
        DataType catalogType = SpannerTable.ofSpannerStrType(row.getString(3));
        schema = schema.add(columnName, catalogType, isNullable);
      }
    }
    this.tableSchema = schema;
    return schema;
  }

  public static DataType ofSpannerStrType(String spannerStrType) {
    switch (spannerStrType) {
      case "BOOL":
        return DataTypes.BooleanType;
      case "BYTES":
        return DataTypes.BinaryType;
      case "DATE":
        return DataTypes.DateType;
      case "FLOAT64":
        return DataTypes.DoubleType;
      case "INT64":
        return DataTypes.LongType;
      case "JSON":
        return DataTypes.BinaryType;
      case "NUMERIC":
        return DataTypes.DoubleType;
      case "STRING":
        return DataTypes.StringType;
      case "TIMESTAMP":
        return DataTypes.TimestampType;
      default: // "ARRAY", "STRUCT"
        int openBracIndex = StringUtils.indexOf(spannerStrType, '(');
        if (openBracIndex >= 0) {
          return SpannerTable.ofSpannerStrType(StringUtils.truncate(spannerStrType, openBracIndex));
        }
        return DataTypes.NullType;
    }
  }

  @Override
  public StructType schema() {
    return this.tableSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return null;
  }

  @Override
  public String name() {
    return this.tableName;
  }
}
