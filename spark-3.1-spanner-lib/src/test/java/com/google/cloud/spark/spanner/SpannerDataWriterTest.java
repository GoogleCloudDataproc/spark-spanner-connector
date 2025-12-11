package com.google.cloud.spark.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.spanner.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.testing.TestingExecutors;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.google.spanner.v1.BatchWriteResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
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
  private final ExecutorService executor = MoreExecutors.newDirectExecutorService();

  private final ScheduledExecutorService scheduledExecutor =
      TestingExecutors.sameThreadScheduledExecutor();
  @Mock private ServerStream<BatchWriteResponse> mockSuccessStream;
  @Mock private ServerStream<BatchWriteResponse> mockTransientFailureStream;
  private StructType schema;
  private Map<String, String> properties;
  private BatchClientWithCloser batchClientWithCloser;
  private ExpressionEncoder.Serializer<Row> serializer;
  @Mock private ExecutorService mockExecutor;
  @Mock private ScheduledExecutorService mockScheduledExecutor;
  private final BatchWriteResponse transientError =
      BatchWriteResponse.newBuilder()
          .addIndexes(0)
          .setStatus(Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED_VALUE).build())
          .build();

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    batchClientWithCloser =
        new BatchClientWithCloser(mockSpanner, mockBatchClient, mockDatabaseClient);
    when(mockSuccessStream.iterator()).thenReturn(Collections.emptyIterator());

    when(mockTransientFailureStream.iterator())
        .thenReturn(Collections.singletonList(transientError).iterator());
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
    properties.put("mutationsPerBatch", "2"); // Use a small batch size for tests
  }

  private SpannerDataWriter createWriter(Map<String, String> props) {
    return new SpannerDataWriter(
        0, 0, props, schema, batchClientWithCloser, executor, scheduledExecutor);
  }

  private SpannerDataWriter createWriterMockExecutors(Map<String, String> props) {
    return new SpannerDataWriter(
        0, 0, props, schema, batchClientWithCloser, mockExecutor, mockScheduledExecutor);
  }

  private Row createMockRow(long i) {
    return RowFactory.create(i, "row" + i);
  }

  @Test
  public void testIdempotentWriteRecoversFromRetriableError() throws IOException {
    properties.put("assumeIdempotentRows", "true");
    try (SpannerDataWriter writer = createWriter(properties)) {

      // On the first call, the executor submits a task that throws an error.
      // On the second call, the executor submits a task that succeeds.
      when(mockDatabaseClient.batchWriteAtLeastOnce(any()))
          .thenReturn(mockTransientFailureStream)
          .thenReturn(mockSuccessStream);
      writer.write(serializer.apply(createMockRow(1L)));
      writer.commit();
    }

    // We expect the executor to have been called twice (1 initial + 1 retry)
    verify(mockDatabaseClient, times(2)).batchWriteAtLeastOnce(any());
  }

  @Test
  public void testTransactionalWriteFailsImmediately() throws IOException {
    properties.put("assumeIdempotentRows", "false");
    try (SpannerDataWriter writer = createWriter(properties)) {

      SpannerException immediateError =
          SpannerExceptionFactory.newSpannerException(
              ErrorCode.UNAVAILABLE, "Simulated immediate failure");

      when(mockDatabaseClient.write(any())).thenThrow(immediateError);
      IOException thrown =
          assertThrows(
              IOException.class,
              () -> {
                writer.write(serializer.apply(createMockRow(1L)));
                writer.commit();
              });

      assertEquals("Failed to commit Spanner partition 0", thrown.getMessage());
      assertEquals(SpannerException.class, thrown.getCause().getClass());
    }
  }

  @Test
  public void testBackpressureWaitsWhenQueueIsFull() throws Exception {
    properties.put("maxPendingTransactions", "1");
    // Use a real executor here to test the blocking behavior
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    SpannerDataWriter writer =
        new SpannerDataWriter(
            0, 0, properties, schema, batchClientWithCloser, realExecutor, scheduledExecutor);

    CompletableFuture<Void> blockingFuture = new CompletableFuture<>();
    writer.pendingWrites.add(blockingFuture); // Manually fill the queue

    ExecutorService testExecutor = Executors.newSingleThreadExecutor();
    Future<?> futureWrite =
        testExecutor.submit(
            () -> {
              try {
                // This should trigger flushBufferAsync and block on waitForOneWrite
                writer.write(serializer.apply(createMockRow(1L)));
                writer.commit();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    // Give the writer thread a moment to block on the future.get()
    Thread.sleep(200);
    assertThat(futureWrite.isDone()).isFalse();

    // Now, unblock the writer by completing the future
    blockingFuture.complete(null);

    // The futureWrite should now complete quickly
    futureWrite.get(2, TimeUnit.SECONDS);
    assertThat(futureWrite.isDone()).isTrue();

    realExecutor.shutdown();
    testExecutor.shutdown();
  }

  @Test
  public void testFailureIsPropagatedFromFullQueue() throws IOException {
    properties.put("maxPendingTransactions", "1");
    properties.put("mutationsPerTransaction", "1");
    // Use a real executor as we are not mocking the execution itself,
    // but the result of the execution.
    ExecutorService realExecutor = Executors.newSingleThreadExecutor();
    SpannerDataWriter writer =
        new SpannerDataWriter(
            0, 0, properties, schema, batchClientWithCloser, realExecutor, scheduledExecutor);

    SpannerException permanentError =
        SpannerExceptionFactory.newSpannerException(ErrorCode.INVALID_ARGUMENT, "Permanent error");

    // Manually add a failed future to the queue to make it "full".
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(permanentError);
    writer.pendingWrites.add(failedFuture);
    // Fill mutation queue
    writer.write(serializer.apply(createMockRow(1L)));

    // This next write should trigger flushBufferAsync.
    // Inside flushBufferAsync, cleanUpFinishedWrites is called.
    // The failed future remains on the list
    // Then, the backpressure `while` loop will run, calling waitForOneWrite.
    // waitForOneWrite will call .get() on the failed future, throwing an exception.
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> writer.write(serializer.apply(createMockRow(1L))));

    // Verify the cause is the original exception from the failed future.
    assertEquals(permanentError, thrown.getCause().getCause());

    realExecutor.shutdown();
  }

  @Test
  public void testAbortCancelsPendingWritesAndShutsDownExecutors() throws IOException {
    SpannerDataWriter writer = createWriterMockExecutors(properties);

    // Simulate some pending writes that are still active
    CompletableFuture<Void> pendingFuture1 = new CompletableFuture<>();
    CompletableFuture<Void> pendingFuture2 = new CompletableFuture<>();
    writer.pendingWrites.add(pendingFuture1);
    writer.pendingWrites.add(pendingFuture2);

    // Call abort
    writer.abort();

    // Verify shutdown methods were called
    verify(mockExecutor, times(1)).shutdownNow();
    verify(mockScheduledExecutor, times(1)).shutdownNow();

    // Verify pending futures were cancelled
    assertTrue(pendingFuture1.isCancelled());
    assertTrue(pendingFuture2.isCancelled());

    // Verify Spanner client is closed
    verify(mockSpanner, times(1)).close();
  }

  private ServerStream<BatchWriteResponse> createTransientFailureStream() {
    BatchWriteResponse transientError =
        BatchWriteResponse.newBuilder()
            .addIndexes(0)
            .setStatus(Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED_VALUE).build())
            .build();
    ServerStream<BatchWriteResponse> mockStream = mock(ServerStream.class);
    when(mockStream.iterator()).thenReturn(Collections.singletonList(transientError).iterator());
    return mockStream;
  }

  @Test
  public void testIdempotentWriteFailsAfterMaxRetriesForPartialFailure() {
    properties.put("assumeIdempotentRows", "true");
    SpannerDataWriter writer = createWriter(properties);

    // Always return a stream indicating a partial failure for all MAX_RETRIES + 1 calls
    when(mockDatabaseClient.batchWriteAtLeastOnce(any()))
        .thenAnswer(invocation -> createTransientFailureStream()) // Call 1
        .thenAnswer(invocation -> createTransientFailureStream()) // Call 2 (Retry 1)
        .thenAnswer(invocation -> createTransientFailureStream()) // Call 3 (Retry 2)
        .thenAnswer(invocation -> createTransientFailureStream()) // Call 4 (Retry 3)
        .thenAnswer(invocation -> createTransientFailureStream()); // Call 5 (Retry 4, MAX_RETRIES)

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> {
              writer.write(serializer.apply(createMockRow(1L)));
              writer.commit();
            });

    assertThat(thrown).isNotNull();
    assertThat(thrown.getCause()).isInstanceOf(IOException.class);
    assertThat(thrown.getCause().getMessage()).contains("Exhausted retries");
    // We expect MAX_RETRIES + 1 total calls.
    verify(mockDatabaseClient, times(5)).batchWriteAtLeastOnce(any());
  }

  @Test
  public void testIdempotentWriteFailsAfterMaxRetriesForTotalFailure() {
    properties.put("assumeIdempotentRows", "true");
    SpannerDataWriter writer = createWriter(properties);

    SpannerException permanentError =
        SpannerExceptionFactory.newSpannerException(
            ErrorCode.UNAVAILABLE, "Simulated permanent transport error");

    // Always throw an exception when the client is called
    when(mockDatabaseClient.batchWriteAtLeastOnce(any())).thenThrow(permanentError);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> {
              writer.write(serializer.apply(createMockRow(1L)));
              writer.commit();
            });

    assertEquals(permanentError, thrown.getCause());
    // We expect MAX_RETRIES + 1 total calls.
    verify(mockDatabaseClient, times(5)).batchWriteAtLeastOnce(any());
  }
}
