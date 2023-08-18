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

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TimestampBound;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Seq;
import scala.collection.Seq$;

public class SpannerRDD extends RDD<InternalRow> {

  private final BatchClient batchClient;
  private final Partition[] partitions;

  public SpannerRDD(SparkContext sparkContext, BatchClient batchClient, Partition[] partitions) {
    super(
        sparkContext,
        (Seq<Dependency<?>>) Seq$.MODULE$.<Dependency<?>>newBuilder().result(),
        scala.reflect.ClassTag$.MODULE$.apply(InternalRow.class));

    this.batchClient = batchClient;
    this.partitions = partitions;
  }

  @Override
  public scala.collection.Iterator<InternalRow> compute(Partition partition, TaskContext context) {
    com.google.cloud.spanner.Partition part = (com.google.cloud.spanner.Partition) partition;

    // 1. Execute the partition in a transaction.
    // TODO: Once we fully support writes, let's change
    // this transaction's type from ReadOnly to ReadWrite.
    try (final BatchReadOnlyTransaction txn =
        this.batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {

      // Note that rs.close() is automatically invoked after
      // the iterator in InternalRowIterator is exhausted.
      ResultSet rs = txn.execute(part);

      return new InterruptibleIterator<InternalRow>(
          context, new ScalaIterator<InternalRow>(new InternalRowIterator(rs)));
    }
  }

  @Override
  public Partition[] getPartitions() {
    return this.partitions;
  }
}
