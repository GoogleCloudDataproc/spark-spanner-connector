package com.google.cloud.spark.spanner;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class SpannerBatchWrite implements BatchWrite {
  private final LogicalWriteInfo info;

  public SpannerBatchWrite(LogicalWriteInfo info) {
    this.info = info;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new SpannerDataWriterFactory(
        this.info.options().asCaseSensitiveMap(), this.info.schema());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    // Commit the write operation.
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // Abort the write operation.
  }
}
