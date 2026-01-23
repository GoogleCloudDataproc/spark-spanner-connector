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

import java.io.Serializable;
import org.apache.spark.Partition;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;

public class SpannerPartition implements Partition, InputPartition, Serializable {

  private final com.google.cloud.spanner.Partition partition;
  private final int index;
  private final InputPartitionContext<InternalRow> ctx;

  public SpannerPartition(
      com.google.cloud.spanner.Partition partition,
      int index,
      InputPartitionContext<InternalRow> ctx) {
    this.index = index;
    this.partition = partition;
    this.ctx = ctx;
  }

  @Override
  public int index() {
    return this.index;
  }

  @Override
  public String toString() {
    return "SpannerPartition{index=" + this.index + ", stream=" + this.partition + "}";
  }

  public InputPartitionContext<InternalRow> getContext() {
    return this.ctx;
  }
}
