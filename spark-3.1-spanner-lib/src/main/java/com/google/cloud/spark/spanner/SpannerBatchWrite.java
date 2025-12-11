package com.google.cloud.spark.spanner;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerBatchWrite implements BatchWrite {
  private static final Logger log = LoggerFactory.getLogger(SpannerBatchWrite.class);

  private final LogicalWriteInfo info;

  public SpannerBatchWrite(LogicalWriteInfo info) {
    this.info = info;
    log.info("Creating SpannerBatchWrite for queryId {}", info.queryId());
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new SpannerDataWriterFactory(
        this.info.options().asCaseSensitiveMap(), this.info.schema());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    // This is a no-op. There is no per-batch resources allocated.
    log.info("Committing SpannerBatchWrite for queryId {}", info.queryId());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    log.info("Aborting SpannerBatchWrite for queryId {}", info.queryId());
  }
}
