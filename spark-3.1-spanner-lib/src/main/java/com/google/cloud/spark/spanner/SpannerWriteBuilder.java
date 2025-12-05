package com.google.cloud.spark.spanner;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class SpannerWriteBuilder implements WriteBuilder {
  private final LogicalWriteInfo info;

  public SpannerWriteBuilder(LogicalWriteInfo info) {
    this.info = info;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info);
  }
}
