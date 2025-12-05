package com.google.cloud.spark.spanner;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class SpannerWriterCommitMessage implements WriterCommitMessage {
  private final int partitionId;
  private final long taskId;

  public SpannerWriterCommitMessage(int partitionId, long taskId) {
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getTaskId() {
    return taskId;
  }
}
