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

import com.google.cloud.spanner.SessionPoolOptions;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class SpannerDataWriterFactory implements DataWriterFactory {
  private final Map<String, String> properties;
  private final StructType schema;

  public SpannerDataWriterFactory(Map<String, String> properties, StructType schema) {
    this.properties = properties;
    this.schema = schema;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    int numThreads = Integer.parseInt(properties.getOrDefault("numWriteThreads", "8"));
    SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder().setMinSessions(1).setMaxSessions(numThreads).build();
    BatchClientWithCloser batchClient =
        SpannerUtils.batchClientFromProperties(properties, sessionPoolOptions);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    return new SpannerDataWriter(
        partitionId, taskId, properties, schema, batchClient, executor, scheduler);
  }
}
