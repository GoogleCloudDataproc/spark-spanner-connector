package com.google.cloud.spark.spanner;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Code;
import com.google.spanner.v1.BatchWriteResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerDataWriter implements DataWriter<InternalRow> {

  private static final Logger log = LoggerFactory.getLogger(SpannerDataWriter.class);
  private static final int MAX_RETRIES = 4;
  public static final String MUTATIONS_PER_TRANSACTION_DEFAULT_STR = "1000";
  public static final String BYTES_PER_TRANSACTION_DEFAULT_STR = "1048576";
  public static final String ASSUME_IDEMPOTENT_ROWS_DEFAULT_STR = "false";
  public static final String MAX_PENDING_TRANSACTIONS_DEFAULT_STR = "20";

  private final int partitionId;
  private final long taskId;
  private final String tableName;
  private final StructType schema;

  // Limits
  private final int mutationsPerTransaction;
  private final long bytesPerTransaction;

  // Connector options
  private final boolean assumeIdempotentRows;

  private final int maxPendingTransactions;

  // Buffers
  private final List<Mutation> mutationBuffer = new ArrayList<>();
  private final BatchClientWithCloser batchClient;
  private long currentBatchBytes = 0;

  private final ScheduledExecutorService scheduler;
  // Async Execution
  private final ExecutorService executor;

  @VisibleForTesting
  protected final List<CompletableFuture<Void>> pendingWrites = new ArrayList<>();

  public SpannerDataWriter(
      int partitionId,
      long taskId,
      Map<String, String> properties,
      StructType schema,
      BatchClientWithCloser batchClient,
      ExecutorService executor,
      ScheduledExecutorService scheduler) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.tableName = SpannerUtils.getRequiredOption(properties, "table");

    this.schema = schema;

    // Default to 1MB (Safety) and 1000 Mutations
    this.mutationsPerTransaction =
        Integer.parseInt(
            properties.getOrDefault(
                "mutationsPerTransaction", MUTATIONS_PER_TRANSACTION_DEFAULT_STR));
    this.bytesPerTransaction =
        Long.parseLong(
            properties.getOrDefault(
                "bytesPerTransaction", BYTES_PER_TRANSACTION_DEFAULT_STR)); // 1 MB default
    this.assumeIdempotentRows =
        Boolean.parseBoolean(
            properties.getOrDefault("assumeIdempotentRows", ASSUME_IDEMPOTENT_ROWS_DEFAULT_STR));
    this.maxPendingTransactions =
        Integer.parseInt(
            properties.getOrDefault(
                "maxPendingTransactions", MAX_PENDING_TRANSACTIONS_DEFAULT_STR));

    this.batchClient = batchClient;
    this.executor = executor;
    this.scheduler = scheduler;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // 1. Convert to Spanner Mutation
    Mutation mutation = SpannerWriterUtils.internalRowToMutation(tableName, record, schema);

    // 2. Estimate Size (Crucial for preventing OOM)
    long mutationSize = estimateMutationSize(record, schema);

    // 3. Check if buffer is full (Count OR Bytes)
    if (!mutationBuffer.isEmpty()
        && (mutationBuffer.size() >= mutationsPerTransaction
            || currentBatchBytes + mutationSize > bytesPerTransaction)) {
      flushBufferAsync();
    }

    // 4. Add to Buffer
    mutationBuffer.add(mutation);
    currentBatchBytes += mutationSize;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    try {
      // 1. Flush the final partial buffer (if any rows remain)
      flushBufferAsync();

      // 2. Wait for ALL in-flight writes to finish
      for (CompletableFuture<Void> writeTask : pendingWrites) {
        // join() blocks until the task is done or throws an unchecked exception
        writeTask.join();
      }

    } catch (Exception e) {
      // If ANY batch failed (even after retries), we must crash the Spark task here.
      // This triggers Spark's retry mechanism for the whole partition.
      // join call above can  wraps any exceptions from task  in CompletionException -- unwrap it.
      Throwable rootCause = (e instanceof CompletionException) ? e.getCause() : e;
      throw new IOException("Failed to commit Spanner partition " + partitionId, rootCause);
    }

    pendingWrites.clear();

    return new SpannerWriterCommitMessage(partitionId, taskId);
  }

  private void flushBufferAsync() {
    if (mutationBuffer.isEmpty()) return;

    final List<Mutation> rawMutations = new ArrayList<>(mutationBuffer);
    mutationBuffer.clear();
    currentBatchBytes = 0;

    // Create a "Shell" Future that represents the whole retry lifecycle
    CompletableFuture<Void> batchLifecycle = new CompletableFuture<>();

    // Track the lifecycle future, not the raw runnable
    pendingWrites.add(batchLifecycle);

    if (assumeIdempotentRows) {
      // High throughput, manual retries, at-least-once writes
      List<MutationGroup> groups = new ArrayList<>(rawMutations.size());
      for (Mutation m : rawMutations) {
        groups.add(MutationGroup.of(m));
      }

      dispatchWriteAtLeastOnceRecursive(groups, 0, batchLifecycle);

    } else {
      // Strict Transaction, No Manual Retries, Exactly-Once
      dispatchStandardWrite(rawMutations, batchLifecycle);
    }
    // Manage Backpressure
    cleanUpFinishedWrites();
    while (pendingWrites.size() >= this.maxPendingTransactions) {
      waitForOneWrite();
    }
  }

  private void dispatchWriteAtLeastOnceRecursive(
      List<MutationGroup> currentBatch, int attempt, CompletableFuture<Void> lifecycle) {

    // Submit I/O to the worker thread
    executor.submit(
        () -> {
          try {
            List<MutationGroup> failedGroups = new ArrayList<>();

            ServerStream<BatchWriteResponse> stream =
                this.batchClient.databaseClient.batchWriteAtLeastOnce(currentBatch);

            for (BatchWriteResponse response : stream) {
              if (response.getStatus().getCode() != Code.OK.getNumber()) {
                for (int index : response.getIndexesList()) {
                  failedGroups.add(currentBatch.get(index));
                }
              }
            }

            if (failedGroups.isEmpty()) {
              // SUCCESS
              lifecycle.complete(null);
            } else {
              // PARTIAL FAILURE: Attempt to retry specific items
              scheduleRetryOrComplete(failedGroups, attempt, lifecycle, null);
            }

          } catch (Exception e) {
            // TRANSPORT/TOTAL FAILURE: Attempt to retry the whole batch
            scheduleRetryOrComplete(currentBatch, attempt, lifecycle, e);
          }
        });
  }

  /**
   * * Handles the decision to retry or fail the lifecycle based on max retries. Encapsulates the
   * delay calculation and scheduler interaction.
   */
  private void scheduleRetryOrComplete(
      List<MutationGroup> batchToRetry,
      int currentAttempt,
      CompletableFuture<Void> lifecycle,
      Throwable cause) {

    if (currentAttempt >= MAX_RETRIES) {
      // Fail the future if limit exceeded
      Throwable finalException =
          (cause != null)
              ? cause
              : new IOException("Exhausted retries for " + batchToRetry.size() + " items.");

      lifecycle.completeExceptionally(finalException);
      return;
    }

    // Calculate Delay (Exponential Backoff + Jitter)
    long delayMs = calculateBackoffDelay(currentAttempt);

    log.info(
        "Failure detected. Scheduling retry {} in {}ms. Items: {}",
        currentAttempt + 1,
        delayMs,
        batchToRetry.size());

    // Schedule the recursive call
    scheduler.schedule(
        () -> dispatchWriteAtLeastOnceRecursive(batchToRetry, currentAttempt + 1, lifecycle),
        delayMs,
        TimeUnit.MILLISECONDS);
  }

  /** Function to calculate retry delay -- 2^attempt * 1000 + random_jitter(0-500) */
  private long calculateBackoffDelay(int attempt) {
    return (long) Math.pow(2, attempt) * 1000 + (long) (Math.random() * 500);
  }

  /**
   * PATH B: Non-Idempotent / Exactly-Once Uses standard 'write()'. NO RETRY LOOP for generic errors
   * (to prevent duplicates). The Spanner Client library handles 'Aborted' retries internally.
   */
  private void dispatchStandardWrite(List<Mutation> batch, CompletableFuture<Void> lifecycle) {
    executor.submit(
        () -> {
          try {
            // This blocks until Commit or Failure
            // The Spanner client library AUTOMATICALLY retries 'Aborted' (locking) errors.
            this.batchClient.databaseClient.write(batch);

            // If we get here, it is committed exactly once.
            lifecycle.complete(null);

          } catch (Exception e) {
            // CRITICAL: We DO NOT retry generic exceptions (like Timeout/Unavailable) manually
            // here.
            // Because we don't know if the data was written or not.
            // We must fail the Spark Task and let Spark handle the partition recovery.
            lifecycle.completeExceptionally(e);
          }
        });
  }

  // Helper to keep the queue clean
  private void cleanUpFinishedWrites() {
    // Only remove futures that completed successfully.
    // Errored futures should be left so that their exceptions can be thrown during commit().
    pendingWrites.removeIf(future -> future.isDone() && !future.isCompletedExceptionally());
  }

  private void waitForOneWrite() {
    if (pendingWrites.isEmpty()) return;
    try {
      // Wait for the oldest lifecycle to complete (success or fail)
      pendingWrites.get(0).get();
      cleanUpFinishedWrites();
    } catch (Exception e) {
      Throwable rootCause = e;
      if (e instanceof ExecutionException || e instanceof CompletionException) {
        if (e.getCause() != null) {
          rootCause = e.getCause();
        }
      }
      throw new SpannerConnectorException(
          "Write failed while waiting for a pending batch to complete.", rootCause);
    }
  }

  @Override
  public void abort() throws IOException {
    mutationBuffer.clear();
    executor.shutdownNow();
    pendingWrites.forEach(f -> f.cancel(true));
    scheduler.shutdownNow();
    if (this.batchClient != null) {
      try {
        this.batchClient.close();
      } catch (Exception e) {
        log.warn("Failed to close batch client", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    mutationBuffer.clear();
    if (!executor.isShutdown()) {
      executor.shutdown();
    }
    if (!scheduler.isShutdown()) {
      scheduler.shutdown();
    }
    if (this.batchClient != null) {
      try {
        this.batchClient.close();
      } catch (Exception e) {
        log.warn("Failed to close batch client", e);
      }
    }
  }

  private long estimateMutationSize(InternalRow row, StructType schema) {
    long size = 0;
    StructField[] fields = schema.fields();

    for (int i = 0; i < row.numFields(); i++) {
      if (row.isNullAt(i)) continue;
      size += estimateValueSize(row.get(i, fields[i].dataType()), fields[i].dataType());
    }
    return size;
  }

  /** Helper to estimate size of ANY Spark value */
  private long estimateValueSize(Object val, DataType type) {
    if (val == null) return 0;

    // Strings
    if (type instanceof StringType) {
      return ((UTF8String) val).numBytes();
    }
    // Binary
    else if (type instanceof BinaryType) {
      return ((byte[]) val).length;
    }
    // Primitives (Fixed Width) - Safe to use constant
    else {
      return type.defaultSize();
    }
  }
}
