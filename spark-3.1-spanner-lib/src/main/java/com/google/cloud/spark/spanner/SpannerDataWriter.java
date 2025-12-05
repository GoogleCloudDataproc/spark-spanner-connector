package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class SpannerDataWriter implements DataWriter<InternalRow> {
  private final int partitionId;
  private final long taskId;
  private final BatchClientWithCloser batchClientWithCloser;
  private final List<Mutation> mutations = new ArrayList<>();
  private final String tableName;
  private final StructType schema;
  private final int mutationsPerBatch;

  public SpannerDataWriter(
      int partitionId, long taskId, Map<String, String> properties, StructType schema) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.tableName = properties.get("table");
    this.schema = schema;
    this.mutationsPerBatch = Integer.parseInt(properties.getOrDefault("mutationsPerBatch", "1000"));
    batchClientWithCloser = SpannerUtils.batchClientFromProperties(properties);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    mutations.add(SpannerWriterUtils.internalRowToMutation(tableName, record, schema));
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    for (int i = 0; i < mutations.size(); i += mutationsPerBatch) {
      int end = Math.min(i + mutationsPerBatch, mutations.size());
      batchClientWithCloser.databaseClient.write(mutations.subList(i, end));
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
    batchClientWithCloser.close();
  }
}
