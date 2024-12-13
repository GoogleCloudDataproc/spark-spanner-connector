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

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;

public class SpannerInputPartitionContext
    implements InputPartitionContext<InternalRow>, Serializable {

  private final BatchTransactionId batchTransactionId;
  private final Partition partition;
  private final String mapAsJSONStr;
  private final SpannerRowConverter rowConverter;

  public SpannerInputPartitionContext(
      Partition partition,
      BatchTransactionId batchTransactionId,
      String mapAsJSONStr,
      SpannerRowConverter rowConverter) {
    this.mapAsJSONStr = mapAsJSONStr;
    this.partition = partition;
    this.batchTransactionId = batchTransactionId;
    this.rowConverter = rowConverter;
  }

  @Override
  public InputPartitionReaderContext<InternalRow> createPartitionReaderContext() {
    return new SpannerInputPartitionReaderContext(
        partition, batchTransactionId, mapAsJSONStr, rowConverter);
  }

  @Override
  public boolean supportsColumnarReads() {
    return false;
  }
}
