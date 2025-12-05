package com.google.cloud.spark.spanner;

import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SpannerDataWriterFactory implements DataWriterFactory {
  private final Map<String, String> properties;
  private final StructType schema;

  public SpannerDataWriterFactory(Map<String, String> properties, StructType schema) {
    this.properties = new CaseInsensitiveStringMap(properties);
    this.schema = schema;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new SpannerDataWriter(partitionId, taskId, properties, schema);
  }
}
