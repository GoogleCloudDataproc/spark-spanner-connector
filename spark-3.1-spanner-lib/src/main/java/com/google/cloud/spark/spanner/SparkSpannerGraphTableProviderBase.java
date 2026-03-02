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

import com.google.cloud.spark.spanner.graph.SpannerGraphBuilder;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkSpannerGraphTableProviderBase implements TableProvider, DataSourceRegister {
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getGraph(options).schema();
  }

  private Table getGraph(CaseInsensitiveStringMap options) {
    boolean hasGraph = options.containsKey("graph");
    if (hasGraph) {
      return SpannerGraphBuilder.build(options);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "properties must contain \"graph\"");
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return getGraph(new CaseInsensitiveStringMap(properties));
  }

  @Override
  public String shortName() {
    return "cloud-spanner-graph";
  }
}
