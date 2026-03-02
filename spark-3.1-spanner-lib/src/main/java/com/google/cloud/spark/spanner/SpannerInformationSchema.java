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
import java.util.List;
import java.util.Locale;
import org.apache.spark.sql.connector.catalog.Identifier;

public interface SpannerInformationSchema {
  Identifier[] listTables(ReadContext readContext, String[] namespace);

  boolean tableExists(ReadContext readContext, String tableName);

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
  public boolean tableExists(ReadContext readContext, String tableName) {
    Statement statement =
        Statement.newBuilder(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @tableName")
            .bind("tableName")
            .to(tableName)
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      return resultSet.next() && resultSet.getLong(0) > 0;
    }
  }
}

class PostgresSpannerInformationSchema implements SpannerInformationSchema {
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
  public boolean tableExists(ReadContext readContext, String tableName) {
    Statement statement =
        Statement.newBuilder(
                "SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1")
            .bind("p1")
            .to(tableName.toLowerCase(Locale.ROOT))
            .build();
    try (ResultSet resultSet = readContext.executeQuery(statement)) {
      return resultSet.next() && resultSet.getLong(0) > 0;
    }
  }
}
