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

import org.apache.spark.sql.SparkSession;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

public class SparkSpannerIntegrationTestBase extends SpannerTestBase {

  @ClassRule public static SparkFactory sparkFactory = new SparkFactory();

  protected SparkSession spark;

  public SparkSpannerIntegrationTestBase() {
    this.spark = sparkFactory.spark;
  }

  protected static class SparkFactory extends ExternalResource {
    SparkSession spark;

    @Override
    protected void before() throws Throwable {
      spark =
          SparkSession.builder()
              .master("local")
              .config("spark.ui.enabled", "false")
              .config("spark.default.parallelism", 20)
              .getOrCreate();
      // reducing test's logs
      spark.sparkContext().setLogLevel("WARN");
    }
  }
}