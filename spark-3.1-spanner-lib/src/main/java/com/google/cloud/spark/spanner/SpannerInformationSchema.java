// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public interface SpannerInformationSchema {
  Identifier[] listTables(ReadContext readContext, String[] namespace);

  Statement tableExistsStatement(String tableName);

  String quoteIdentifier(String identifier);

  String sparkTypeToSpannerType(StructField field);

  default String toDdl(Identifier ident, StructType schema) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE ").append(quoteIdentifier(ident.name())).append(" (");
    for (StructField field : schema.fields()) {
      ddl.append(quoteIdentifier(field.name())).append(" ").append(sparkTypeToSpannerType(field));
      if (!field.nullable()) {
        ddl.append(" NOT NULL");
      }
      ddl.append(", ");
    }

    List<String> primaryKeys =
        Arrays.stream(schema.fields())
            .filter(
                f ->
                    f.metadata().contains(SpannerUtils.PRIMARY_KEY_TAG)
                        && f.metadata().getBoolean(SpannerUtils.PRIMARY_KEY_TAG))
            .map(f -> quoteIdentifier(f.name()))
            .collect(Collectors.toList());

    if (primaryKeys.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "No primary key found for table "
              + ident.name()
              + ". Please specify at least one primary key column.");
    }

    ddl.append("PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
    ddl.append(")");
    return ddl.toString();
  }

  static SpannerInformationSchema create(Dialect dialect) {
    switch (dialect) {
      case POSTGRESQL:
        return new PostgresSpannerInformationSchema();
      case GOOGLE_STANDARD_SQL:
        return new GoogleSqlSpannerInformationSchema();
    }
    throw new IllegalArgumentException("Unsupported dialect: " + dialect);
  }
}

class GoogleSqlSpannerInformationSchema implements SpannerInformationSchema {
  @Override
  public String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  @Override
  public String sparkTypeToSpannerType(StructField field) {
    if (field.dataType().equals(DataTypes.LongType)) {
      return "INT64";
    }
    if (field.dataType().equals(DataTypes.StringType)) {
      return "STRING(MAX)";
    }
    if (field.dataType().equals(DataTypes.BooleanType)) {
      return "BOOL";
    }
    if (field.dataType().equals(DataTypes.DoubleType)) {
      return "FLOAT64";
    }
    if (field.dataType().equals(DataTypes.BinaryType)) {
      return "BYTES(MAX)";
    }
    if (field.dataType().equals(DataTypes.TimestampType)) {
      return "TIMESTAMP";
    }
    if (field.dataType().equals(DataTypes.DateType)) {
      return "DATE";
    }
    if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
      return "NUMERIC";
    }
    if (field.dataType() instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) field.dataType();
      if (arrayType.elementType() instanceof ArrayType) {
        throw new SpannerConnectorException(
            SpannerErrorCode.UNSUPPORTED_DATATYPE,
            "Nested arrays are not supported by Spanner: " + field.dataType());
      }
      StructField elementField =
          DataTypes.createStructField(
              field.name(), arrayType.elementType(), arrayType.containsNull());
      return "ARRAY<" + sparkTypeToSpannerType(elementField) + ">";
    }

    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED_DATATYPE,
        "Unsupported data type in CREATE TABLE: " + field.dataType());
  }

  @Override
  public Identifier[] listTables(ReadContext readContext, String[] namespace) {
    Statement statement =
        Statement.of("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''");
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      List<Identifier> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(Identifier.of(namespace, resultSet.getString("TABLE_NAME")));
      }
      return tables.toArray(new Identifier[0]);
    }
  }

  @Override
  public Statement tableExistsStatement(String tableName) {
    return Statement.newBuilder(
            "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @tableName")
        .bind("tableName")
        .to(tableName)
        .build();
  }
}

class PostgresSpannerInformationSchema implements SpannerInformationSchema {
  @Override
  public String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  public String sparkTypeToSpannerType(StructField field) {
    if (field.dataType().equals(DataTypes.LongType)) {
      return "bigint";
    }
    if (field.dataType().equals(DataTypes.StringType)) {
      return "varchar";
    }
    if (field.dataType().equals(DataTypes.BooleanType)) {
      return "boolean";
    }
    if (field.dataType().equals(DataTypes.DoubleType)) {
      return "float8";
    }
    if (field.dataType().equals(DataTypes.BinaryType)) {
      return "bytea";
    }
    if (field.dataType().equals(DataTypes.TimestampType)) {
      return "timestamptz";
    }
    if (field.dataType().equals(DataTypes.DateType)) {
      return "date";
    }
    if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
      return "numeric";
    }
    if (field.dataType() instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) field.dataType();
      if (arrayType.elementType() instanceof ArrayType) {
        throw new SpannerConnectorException(
            SpannerErrorCode.UNSUPPORTED_DATATYPE,
            "Nested arrays are not supported by Spanner: " + field.dataType());
      }
      StructField elementField =
          DataTypes.createStructField(
              field.name(), arrayType.elementType(), arrayType.containsNull());
      return sparkTypeToSpannerType(elementField) + "[]";
    }

    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED_DATATYPE,
        "Unsupported data type in CREATE TABLE: " + field.dataType());
  }

  @Override
  public Identifier[] listTables(ReadContext readContext, String[] namespace) {
    Statement statement =
        Statement.of(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      List<Identifier> tables = new ArrayList<>();
      while (resultSet.next()) {
        tables.add(Identifier.of(namespace, resultSet.getString("table_name")));
      }
      return tables.toArray(new Identifier[0]);
    }
  }

  @Override
  public Statement tableExistsStatement(String tableName) {
    return Statement.newBuilder(
            "SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1")
        .bind("p1")
        .to(tableName)
        .build();
  }
}
