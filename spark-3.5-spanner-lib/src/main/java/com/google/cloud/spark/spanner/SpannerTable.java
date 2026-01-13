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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.connection.Connection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  private final String tableName;

  private final String instanceId;

  private final String databaseId;

  private final String projectId;
  private final SpannerTableSchema dbSchema;
  private final StructType sparkSchema;
  private static final ImmutableSet<TableCapability> tableCapabilities =
      ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
  private final Map<String, String> properties;

  private static final Logger log = LoggerFactory.getLogger(SpannerTable.class);

  public SpannerTable(Map<String, String> properties) {
    this.properties = properties;
    log.info("SpannerTable properties: {}", properties);

    this.tableName = SpannerUtils.getRequiredOption(properties, "table");
    this.projectId = SpannerUtils.getRequiredOption(properties, "projectId");
    this.instanceId = SpannerUtils.getRequiredOption(properties, "instanceId");
    this.databaseId = SpannerUtils.getRequiredOption(properties, "databaseId");
    try (Connection conn = SpannerUtils.connectionFromProperties(properties)) {
      boolean isPostgreSql;
      if (conn.getDialect().equals(Dialect.GOOGLE_STANDARD_SQL)) {
        isPostgreSql = false;
      } else if (conn.getDialect().equals(Dialect.POSTGRESQL)) {
        isPostgreSql = true;
      } else {
        throw new SpannerConnectorException(
            SpannerErrorCode.DATABASE_DIALECT_NOT_SUPPORTED,
            "The dialect used "
                + conn.getDialect()
                + " in the Spanner table "
                + tableName
                + " is not supported.");
      }
      this.dbSchema = new SpannerTableSchema(conn, tableName, isPostgreSql);
      this.sparkSchema = this.dbSchema.schema;
    }
  }

  public SpannerTable(Map<String, String> properties, StructType dfSchema) {
    this.properties = properties;
    this.tableName = SpannerUtils.getRequiredOption(properties, "table");
    this.projectId = SpannerUtils.getRequiredOption(properties, "projectId");
    this.instanceId = SpannerUtils.getRequiredOption(properties, "instanceId");
    this.databaseId = SpannerUtils.getRequiredOption(properties, "databaseId");
    try (Connection conn = SpannerUtils.connectionFromProperties(properties)) {
      boolean isPostgreSql;
      if (conn.getDialect().equals(Dialect.GOOGLE_STANDARD_SQL)) {
        isPostgreSql = false;
      } else if (conn.getDialect().equals(Dialect.POSTGRESQL)) {
        isPostgreSql = true;
      } else {
        throw new SpannerConnectorException(
            SpannerErrorCode.DATABASE_DIALECT_NOT_SUPPORTED,
            "The dialect used "
                + conn.getDialect()
                + " in the Spanner table "
                + tableName
                + " is not supported.");
      }
      // Still get the DB schema for validation.
      this.dbSchema = new SpannerTableSchema(conn, tableName, isPostgreSql);
      this.sparkSchema = dfSchema;
    }
  }

  public static DataType ofSpannerStrType(String spannerStrType, boolean isNullable) {
    // Trim both ends of the string firstly, it could have come in as:
    // "   STRUCT<a STRING(10), b INT64>  "
    spannerStrType = spannerStrType.trim().toUpperCase();
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
      return DataTypes.BinaryType;
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

    // Return NullType for non-supported fields.
    return DataTypes.NullType;
  }

  public static DataType ofSpannerStrTypePg(String spannerStrType, boolean isNullable) {
    spannerStrType = spannerStrType.trim().toLowerCase();
    switch (spannerStrType) {
      case "bool":
        return DataTypes.BooleanType;

      case "boolean":
        return DataTypes.BooleanType;

      case "bytea":
        return DataTypes.BinaryType;

      case "date":
        return DataTypes.DateType;

      case "float8":
        return DataTypes.DoubleType;

      case "double precision":
        return DataTypes.DoubleType;

      case "bigint":
        return DataTypes.LongType;

      case "int8":
        return DataTypes.LongType;

      case "jsonb":
        return DataTypes.StringType;

      case "numeric":
        return numericToCatalogDataType;

      case "decimal":
        return numericToCatalogDataType;

      case "character varying":
        return DataTypes.StringType;

      case "varchar":
        return DataTypes.StringType;

      case "text":
        return DataTypes.StringType;

      case "timestamp with time zone":
        return DataTypes.TimestampType;

      case "timestamptz":
        return DataTypes.TimestampType;

      case "int":
        return DataTypes.IntegerType;
    }

    Pattern pattern = Pattern.compile("\\[.*\\]");
    Matcher matcher = pattern.matcher(spannerStrType);
    if (matcher.find()) {
      // Sample argument: character varying[]
      int se = matcher.start();
      String str = spannerStrType.substring(0, se);

      DataType innerDataType = SpannerTable.ofSpannerStrTypePg(str, isNullable);
      return DataTypes.createArrayType(innerDataType, isNullable);
    }

    // character varying(MAX), character varying(10) are the correct type
    // definitions for STRING in Cloud Spanner.
    // Non-composite types like "character varying(N)" and "bytea(N)"
    // can immediately be returned by prefix matching.
    if (spannerStrType.indexOf("character varying") == 0
        || spannerStrType.indexOf("varchar") == 0
        || spannerStrType.indexOf("text") == 0) {
      return DataTypes.StringType;
    }
    if (spannerStrType.indexOf("bytea") == 0) {
      return DataTypes.BinaryType;
    }

    // Return NullType for non-supported fields.
    return DataTypes.NullType;
  }

  // Please see https://cloud.google.com/spanner/docs/storing-numeric-data#precision
  // We are using (decimalPrecision=38, scale=9)
  private static final DataType numericToCatalogDataType = DataTypes.createDecimalType(38, 9);

  @Override
  public StructType schema() {
    return this.sparkSchema;
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
    SpannerUtils.validateSchema(info.schema(), this.dbSchema.schema, this.tableName);
    return new SpannerWriteBuilder(info);
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .putAll(this.properties)
            .put("openlineage.dataset.name", String.format("%s/%s", databaseId, tableName))
            .put(
                "openlineage.dataset.namespace",
                String.format("spanner://%s/%s", projectId, instanceId))
            .put("openlineage.dataset.storageDatasetFacet.storageLayer", "spanner");
    return builder.build();
  }
}
