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
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

/*
 * SpannerScanner implements Scan.
 */
public class SpannerScanner implements Scan {
  private SpannerTable spannerTable;

  public SpannerScanner(Map<String, String> opts) {
    this.spannerTable = new SpannerTable(null, opts);
  }

  @Override
  public StructType readSchema() {
    return this.spannerTable.schema();
  }

  @Override
  public Batch toBatch() {
    // TODO: Fill me in.
    return null;
  }
}
