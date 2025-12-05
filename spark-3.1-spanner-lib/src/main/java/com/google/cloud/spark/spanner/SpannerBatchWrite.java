package com.google.cloud.spark.spanner;

import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class SpannerBatchWrite implements BatchWrite {
  private final Map<String, String> properties;
  private final LogicalWriteInfo info;
  private final StructType schema;

  public SpannerBatchWrite(
      LogicalWriteInfo info, Map<String, String> properties, StructType schema) {
    this.info = info;
    this.properties = properties;
    this.schema = schema;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new SpannerDataWriterFactory(this.properties, this.schema);
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
