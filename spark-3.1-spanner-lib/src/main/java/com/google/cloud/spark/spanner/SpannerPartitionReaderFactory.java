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
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class SpannerPartitionReaderFactory implements PartitionReaderFactory {
  @Override
  public PartititionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    // TODO: Fill me in.
    return null;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    // TODO: Fill me in.
    return null;
  }

  @Override
  public boolean supportsColumnarReads(InputPartition partition) {
    // TODO: Fill me in.
    return false;
  }
}
