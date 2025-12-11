package com.google.cloud.spark.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.spanner.*;
import com.google.spanner.v1.BatchWriteResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SpannerDataWriterTest {

  @Mock private Spanner mockSpanner;
  @Mock private DatabaseClient mockDatabaseClient;
  @Mock private BatchClient mockBatchClient;
  @Mock private ServerStream<BatchWriteResponse> mockSuccessStream;

  private StructType schema;
  private Map<String, String> properties;
  private BatchClientWithCloser batchClientWithCloser;
  private ExpressionEncoder.Serializer<Row> serializer;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Instantiate the thin wrapper with our mocks
    batchClientWithCloser =
        new BatchClientWithCloser(mockSpanner, mockBatchClient, mockDatabaseClient);

    // Mock a successful stream response for batchWriteAtLeastOnce
    when(mockSuccessStream.iterator()).thenReturn(Collections.emptyIterator());

    schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("long_col", DataTypes.LongType, false),
              DataTypes.createStructField("string_col", DataTypes.StringType, true),
            });

    ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
    serializer = encoder.createSerializer();
    properties = new HashMap<>();
    properties.put("table", "testTable");
    properties.put("mutationsPerBatch", "10");
    properties.put("bytesPerBatch", "10000");
  }

  // Helper to create a SpannerDataWriter instance and inject the BatchClientWithCloser
  private SpannerDataWriter createWriter(boolean assumeIdempotentRows) {
    properties.put("assumeIdempotentRows", String.valueOf(assumeIdempotentRows));
    return new SpannerDataWriter(0, 0, properties, schema, batchClientWithCloser);
  }

  private List<Row> createMockRows(int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(RowFactory.create((long) i, "row" + i));
    }
    return rows;
  }

  @Test
  public void testIdempotentWriteRecoversFromRetriableError() throws IOException {
    try (SpannerDataWriter writer = createWriter(true)) {

      SpannerException transientError =
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.UNAVAILABLE, "Simulated transient error");

      when(mockDatabaseClient.batchWriteAtLeastOnce(anyList()))
          .thenThrow(transientError)
          .thenReturn(mockSuccessStream);

      List<Row> rowsToWrite = createMockRows(15); // > mutationsPerBatch (10)
      for (Row row : rowsToWrite) {
        writer.write(serializer.apply(row)); // Direct cast to InternalRow
      }
      writer.commit();

      verify(mockDatabaseClient, times(2)).batchWriteAtLeastOnce(anyList());
    }
    verify(mockSpanner, times(1)).close();
  }

  @Test
  public void testTransactionalWriteFailsImmediately() throws IOException {
    try (SpannerDataWriter writer = createWriter(false)) {

      SpannerException immediateError =
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.UNAVAILABLE, "Simulated immediate failure");
      doThrow(immediateError).when(mockDatabaseClient).write(anyList());

      List<Row> rowsToWrite = createMockRows(15); // > mutationsPerBatch (10)
      for (Row row : rowsToWrite) {
        writer.write(serializer.apply(row)); // Direct cast to InternalRow
      }

      IOException thrown = assertThrows(IOException.class, writer::commit);

      assertEquals("Failed to commit Spanner partition 0", thrown.getMessage());
      assertEquals(SpannerException.class, thrown.getCause().getClass());

      verify(mockDatabaseClient, times(1)).write(anyList());
    }
    verify(mockSpanner, times(1)).close();
  }
}
