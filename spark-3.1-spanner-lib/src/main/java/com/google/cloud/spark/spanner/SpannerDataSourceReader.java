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

import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.DataSourceReaderFactory;

public class SpannerDataSourceReader implements DataSourceReader {
  public SpannerDataSourceReader(DataSourceOptions options) {
    this.scanner = SpannerScanner(options);
  }

  @Override
  public StructType readSchema() {
    return this.scanner.readSchema();
  }

  @Override
  List<DataReaderFactory<Row>> createDataReaderFactories() {
    return null;
  }
}
