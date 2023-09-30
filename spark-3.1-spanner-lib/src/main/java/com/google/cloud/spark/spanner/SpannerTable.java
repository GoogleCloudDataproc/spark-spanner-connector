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
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * SpannerTable implements Table.
 */
public class SpannerTable implements Table, SupportsRead, SupportsWrite {
  private String tableName;
  private StructType tableSchema;
  private static final ImmutableSet<TableCapability> tableCapabilities =
      ImmutableSet.of(TableCapability.BATCH_READ);

  private static final Logger log = LoggerFactory.getLogger(SpannerTable.class);

  public SpannerTable(Map<String, String> properties) {
    try (Connection conn = SpannerUtils.connectionFromProperties(properties)) {
      String tableName = properties.get("table");
      if (tableName == null) {
        log.error("\"table\" is expecting in properties");
      }

      // 2. Run an information schema query to get the type definition of the table.
      Statement stmt =
          Statement.newBuilder(
                  "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE='YES' AS ISNULLABLE, SPANNER_TYPE "
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
    //       COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, SPANNER_TYPE
    // row1:
    // ...
    // rowN:
    StructType schema = new StructType();
    while (rs.next()) {
      Struct row = rs.getCurrentRowAsStruct();
      String columnName = row.getString(0);
      // Integer ordinalPosition = column.getInt(1);
      boolean isNullable = row.getBoolean(2);
      DataType catalogType = SpannerTable.ofSpannerStrType(row.getString(3), isNullable);
      schema = schema.add(columnName, catalogType, isNullable, "" /* No comments for the text */);
    }
    this.tableSchema = schema;
    return schema;
  }

  public static DataType ofSpannerStrType(String spannerStrType, boolean isNullable) {
    // Trim both ends of the string firstly, it could have come in as:
    // "   STRUCT<a STRING(10), b INT64>  "
    spannerStrType = spannerStrType.trim().toUpperCase();
    switch (spannerStrType) {
      case "BOOL":
        return DataTypes.BooleanType;

      case "BYTES":
        return DataTypes.createArrayType(DataTypes.ByteType);

      case "DATE":
        return DataTypes.DateType;

      case "FLOAT64":
        return DataTypes.DoubleType;

      case "INT64":
        return DataTypes.LongType;

      case "JSON":
        return DataTypes.StringType;

      case "NUMERIC":
        return numericToCatalogDataType;

      case "STRING":
        return DataTypes.StringType;

      case "TIMESTAMP":
        return DataTypes.TimestampType;
    }

    // STRING(MAX), STRING(10) are the correct type
    // definitions for STRING in Cloud Spanner.
    // Non-composite types like "STRING(N)" and "BYTES(N)"
    // can immediately be returned by prefix matching.
    if (spannerStrType.indexOf("STRING") == 0) {
      return DataTypes.StringType;
    }
    if (spannerStrType.indexOf("BYTES") == 0) {
      return DataTypes.createArrayType(DataTypes.ByteType);
    }

    if (spannerStrType.indexOf("ARRAY") == 0) {
      // Sample argument: ARRAY<STRING(MAX)>
      int si = spannerStrType.indexOf("<");
      int se = spannerStrType.lastIndexOf(">");
      String str = spannerStrType.substring(si + 1, se);
      // At this point, str=STRING(MAX) or str=ARRAY<ARRAY<T>>
      // ARRAY<T>
      DataType innerDataType = SpannerTable.ofSpannerStrType(str, isNullable);
      return DataTypes.createArrayType(innerDataType, isNullable);
    }

    // We are left with "STRUCT<TYPE>"
    // TODO: Handle struct field traversal
    return DataTypes.NullType;
  }

  // Please see https://cloud.google.com/spanner/docs/storing-numeric-data#precision
  // We are using (decimalPrecision=38, scale=9)
  private static final DataType numericToCatalogDataType = DataTypes.createDecimalType(38, 9);

  @Override
  public StructType schema() {
    return this.tableSchema;
  }

  /*
   * Cloud Spanner tables support:
   *    BATCH_READ
   * as capabilities
   */
  @Override
  public Set<TableCapability> capabilities() {
    return tableCapabilities;
  }

  @Override
  public String name() {
    return this.tableName;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SpannerScanBuilder(options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    throw new SpannerConnectorException(
        SpannerErrorCode.WRITES_NOT_SUPPORTED,
        "writes are not supported in the Spark Spanner Connector");
  }
}
