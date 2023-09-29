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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.Serializable;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;

public class SpannerInputPartitionContext
    implements InputPartitionContext<InternalRow>, Serializable {

  private BatchTransactionId batchTransactionId;
  private Partition partition;
  private Map<String, String> opts;

  public SpannerInputPartitionContext(
      Partition partition, BatchTransactionId batchTransactionId, String mapAsJSONStr) {
    try {
      this.opts = SpannerUtils.deserializeMap(mapAsJSONStr);
    } catch (JsonProcessingException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.SPANNER_FAILED_TO_PARSE_OPTIONS, "Error parsing the input options.", e);
    }
    this.partition = partition;
    this.batchTransactionId = batchTransactionId;
  }

  @Override
  public InputPartitionReaderContext<InternalRow> createPartitionReaderContext() {
    String projectId = this.opts.get("projectId");
    SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder();
    if (projectId != null && projectId != "") {
      spannerOptionsBuilder = spannerOptionsBuilder.setProjectId(projectId);
    }
    try {
      SpannerOptions spannerOptions = spannerOptionsBuilder.build();
      Spanner spanner = spannerOptions.getService();
      BatchClient batchClient =
          spanner.getBatchClient(
              DatabaseId.of(projectId, this.opts.get("instanceId"), this.opts.get("databaseId")));
      BatchReadOnlyTransaction txn = batchClient.batchReadOnlyTransaction(this.batchTransactionId);
      return new SpannerInputPartitionReaderContext(txn.execute(this.partition));
    } finally {
      spanner.close();
    }
  }

  @Override
  public boolean supportsColumnarReads() {
    return false;
  }
}
