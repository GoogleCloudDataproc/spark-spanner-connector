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

package com.google.cloud.spark.spanner.scan;

import java.util.Map;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Spark41SpannerTable extends SpannerTable {
  private static final Logger logger = LoggerFactory.getLogger(Spark41SpannerTable.class);

  public Spark41SpannerTable(Map<String, String> properties) {
    super(properties);
    logger.info("Spark41SpannerTable constructor - properties: {}", properties);
  }

  public Spark41SpannerTable(CaseInsensitiveStringMap properties, StructType dfSchema) {
    super(properties, dfSchema);
    logger.info(
        "Spark41SpannerTable constructor - properties: {}, dfSchema: {}", properties, dfSchema);
  }

  public Spark41SpannerTable(
      String projectId,
      String instanceId,
      String databaseId,
      String tableNameOption,
      CaseInsensitiveStringMap properties,
      StructType dfSchema) {
    super(projectId, instanceId, databaseId, tableNameOption, properties, dfSchema);
    logger.info(
        "Spark41SpannerTable constructor - projectId: {}, instanceId: {}, databaseId: {}, tableNameOption: {}, properties: {}, dfSchema: {}",
        projectId,
        instanceId,
        databaseId,
        tableNameOption,
        properties,
        dfSchema);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    logger.info("Spark41SpannerTable.newScanBuilder - this: {}, options: {}", this, options);
    return new Spark41SpannerScanBuilder(this);
  }
}
