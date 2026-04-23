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

package com.google.cloud.spark.spanner.integration;

import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

public class SparkCatalogSpannerIntegrationTestBase extends SpannerTestBase {

  protected SparkSession spark;

  public SparkCatalogSpannerIntegrationTestBase() {}

  @Before
  public void setUpSpark() {
    Map<String, String> catalogProps = connectionProperties();
    spark =
        SparkSession.builder()
            .master("local")
            .appName("SparkSpannerIntegrationTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.spanner", "com.google.cloud.spark.spanner.SpannerCatalog")
            .config("spark.sql.catalog.spanner.projectId", catalogProps.get("projectId"))
            .config("spark.sql.catalog.spanner.instanceId", catalogProps.get("instanceId"))
            .config("spark.sql.catalog.spanner.databaseId", catalogProps.get("databaseId"))
            .config("spark.default.parallelism", 20)
            .getOrCreate();

    if (catalogProps.get("emulatorHost") != null) {
      spark.conf().set("spark.sql.catalog.spanner.emulatorHost", catalogProps.get("emulatorHost"));
    }
    spark.sparkContext().setLogLevel("WARN");
  }

  @After
  public void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }
}
