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

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class Spark40SpannerDataWriterTest extends SpannerDataWriterTestBase {

  @Override
  protected void localSetup() {}

  @Override
  protected Encoder<Row> getEncoder(StructType schema) {
    // In Spark 4.0, this returns an AgnosticEncoder which implements Encoder
    return Encoders.row(schema);
  }

  @Override
  protected InternalRow CreateInternalRow(long i) {
    Row row = RowFactory.create(i, "row" + i);

    return (InternalRow) CatalystTypeConverters.createToCatalystConverter(schema).apply(row);
  }
}
