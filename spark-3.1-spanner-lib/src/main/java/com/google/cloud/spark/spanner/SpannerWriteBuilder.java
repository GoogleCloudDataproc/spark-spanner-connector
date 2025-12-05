package com.google.cloud.spark.spanner;

import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SpannerWriteBuilder implements WriteBuilder {
  private final LogicalWriteInfo info;
  private final Map<String, String> properties;
  private final StructType schema;

  public SpannerWriteBuilder(
      LogicalWriteInfo info, Map<String, String> properties, StructType schema) {
    this.info = info;
    this.properties = properties;
    this.schema = schema;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info, properties, schema);
  }
}
