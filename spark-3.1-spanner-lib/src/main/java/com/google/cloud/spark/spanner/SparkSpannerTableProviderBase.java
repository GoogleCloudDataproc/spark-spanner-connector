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

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public abstract class SparkSpannerTableProviderBase
    implements SupportsCatalogOptions, DataSourceRegister, TableProvider {

  /*
   * Infers the schema of the table identified by the given options.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getTable(options).schema();
  }

  /*
   * Returns a Table instance with the specified table schema,
   * partitioning and properties to perform a read or write.
   */
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    final CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
    boolean enablePartialRowUpdates =
        Boolean.parseBoolean(options.getOrDefault("enablePartialRowUpdates", "false"));

    boolean hasTable = options.containsKey("table");
    if (hasTable) {
      if (enablePartialRowUpdates) {
        return new SpannerTable(options, schema);
      } else {
        return new SpannerTable(options);
      }
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "properties must contain one of \"table\" or \"graph\"");
    }
  }

  /*
   * Returns true if the source has the ability of
   * accepting external table metadata when getting tables.
   */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  /*
   * Implements DataSourceRegister.shortName(). This method allows Spark to match
   * the DataSource when spark.read(...).format("spanner") is invoked.
   */
  @Override
  public String shortName() {
    return "cloud-spanner";
  }

  private Table getTable(Map<String, String> properties) {
    boolean hasTable = properties.containsKey("table");
    if (hasTable) {
      return new SpannerTable(properties);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "properties must contain one of \"table\" or \"graph\"");
    }
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    String table = options.get("table");
    if (table != null) {
      return Identifier.of(new String[0], table);
    }
    return null;
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return options.getOrDefault("catalog", "spanner");
  }
}
