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
