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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/*
 * SpannerPartitionReaderFactory is an entry to implement PartitionReaderFactory.
 */
public class SpannerPartitionReaderFactory implements PartitionReaderFactory {

  public SpannerPartitionReaderFactory() {}

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    // Unsupported!
    throw new RuntimeException("columnar reads are not supported the Spark Spanner Connector");
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    InputPartitionContext<InternalRow> ctx = ((SpannerPartition) partition).getContext();
    return new SpannerPartitionReader<>(ctx.createPartitionReaderContext());
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
