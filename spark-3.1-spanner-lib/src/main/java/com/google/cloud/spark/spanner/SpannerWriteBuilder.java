// Copyright 2025 Google LLC
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

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SpannerWriteBuilder implements WriteBuilder {
  private final LogicalWriteInfo info;
  private final CaseInsensitiveStringMap properties;

  public SpannerWriteBuilder(LogicalWriteInfo info, CaseInsensitiveStringMap properties) {
    this.info = info;
    this.properties = properties;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info, properties);
  }
}
