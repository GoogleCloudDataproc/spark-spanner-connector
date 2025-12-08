package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class SpannerDataWriter implements DataWriter<InternalRow> {
  private final int partitionId;
  private final long taskId;
  private final DatabaseClient databaseClient;
  private final List<Mutation> mutations = new ArrayList<>();
  private final String tableName;
  private final StructType schema;
  private final int mutationsPerBatch;
  private final int numWriterThreads;

  public SpannerDataWriter(
      int partitionId, long taskId, Map<String, String> properties, StructType schema) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.tableName = properties.get("table");
    this.schema = schema;
    this.mutationsPerBatch = Integer.parseInt(properties.getOrDefault("mutationsPerBatch", "1000"));
    this.numWriterThreads = Integer.parseInt(properties.getOrDefault("numWriterThreads", "8"));
    BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(properties);
    this.databaseClient = batchClient.databaseClient;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    mutations.add(SpannerWriterUtils.internalRowToMutation(tableName, record, schema));
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    ExecutorService executor = Executors.newFixedThreadPool(numWriterThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < mutations.size(); i += mutationsPerBatch) {
      final List<Mutation> batch =
          mutations.subList(i, Math.min(i + mutationsPerBatch, mutations.size()));
      futures.add(executor.submit(() -> databaseClient.write(batch)));
    }

    try {
      for (Future<?> future : futures) {
        future.get(); // Wait for all batches to complete
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Error during parallel batch write", e);
    } finally {
      executor.shutdown();
    }
    return new SpannerWriterCommitMessage(partitionId, taskId);
  }

  @Override
  public void abort() throws IOException {
    mutations.clear();
  }

  @Override
  public void close() throws IOException {
    mutations.clear();
  }
}
