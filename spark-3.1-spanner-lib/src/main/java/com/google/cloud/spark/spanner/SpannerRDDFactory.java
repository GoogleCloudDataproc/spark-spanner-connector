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
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.collect.Streams;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerRDDFactory {
  private static final Logger log = LoggerFactory.getLogger(SpannerRDDFactory.class);

  private BatchClient batchClient;
  private SQLContext sqlContext;

  public SpannerRDDFactory(BatchClient batchClient, SQLContext sqlContext) {
    this.batchClient = batchClient;
    this.sqlContext = sqlContext;
  }

  public RDD<InternalRow> buildScanFromSQL(String sqlStmt) {
    log.info("Running SQL {}", sqlStmt);

    try (final BatchReadOnlyTransaction txn =
        this.batchClient.batchReadOnlyTransaction(TimestampBound.strong())) {
      List<com.google.cloud.spanner.Partition> rawPartitions =
          txn.partitionQuery(
              PartitionOptions.getDefaultInstance(),
              Statement.of(sqlStmt),
              Options.dataBoostEnabled(true));

      List<Partition> partitions =
          Streams.mapWithIndex(
                  rawPartitions.stream(),
                  (partition, index) ->
                      new SpannerPartition(
                          partition.getPartitionToken().toString(), Math.toIntExact(index)))
              .collect(Collectors.toList());

      Partition[] parts = partitions.toArray(new Partition[0]);
      return createRDD(sqlContext, parts);
    }
  }

  public RDD<InternalRow> createRDD(SQLContext sqlContext, Partition[] partitions) {
    String className = "com.google.cloud.spark.spanner.SpannerRDD";
    try {
      Class<? extends RDD<InternalRow>> clazz =
          (Class<? extends RDD<InternalRow>>) Class.forName(className);
      Constructor<? extends RDD<InternalRow>> constructor =
          clazz.getConstructor(SparkContext.class, BatchClient.class, Partition[].class);

      RDD<InternalRow> spannerRDD =
          constructor.newInstance(sqlContext.sparkContext(), this.batchClient, partitions);

      return spannerRDD;
    } catch (Exception e) {
      String.format(
          "Could not initialize a SpannerRDD class of type [%s}: %s", className, e.toString());
      return null;
    }
  }
}
