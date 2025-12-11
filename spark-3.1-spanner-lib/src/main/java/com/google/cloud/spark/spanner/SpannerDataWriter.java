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
import java.util.concurrent.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerDataWriter implements DataWriter<InternalRow> {

  private static final Logger log = LoggerFactory.getLogger(SpannerDataWriter.class);
  private static final int MAX_RETRIES = 4;

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
    this.tableName = properties.get("table");
    this.schema = schema;

    // Default to 1MB (Safety) and 1000 Mutations
    this.mutationsPerTransaction =
        Integer.parseInt(properties.getOrDefault("mutationsPerTransaction", "1000"));
    this.bytesPerTransaction =
        Long.parseLong(properties.getOrDefault("bytesPerTransaction", "1048576")); // 1 MB default
    this.assumeIdempotentRows =
        Boolean.parseBoolean(properties.getOrDefault("assumeIdempotentRows", "false"));
    this.maxPendingTransactions =
        Integer.parseInt(properties.getOrDefault("maxPendingTransactions", "20"));

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
    // Submit the heavy I/O to the worker thread (don't run on scheduler thread)
    executor.submit(
        () -> {
          try {
            List<MutationGroup> failedGroups = new ArrayList<>();

            // --- SPANNER CALL (Blocking I/O) ---
            ServerStream<BatchWriteResponse> stream =
                this.batchClient.databaseClient.batchWriteAtLeastOnce(currentBatch);

            for (BatchWriteResponse response : stream) {
              if (response.getStatus().getCode() != Code.OK.getNumber()) {
                for (int index : response.getIndexesList()) {
                  failedGroups.add(currentBatch.get(index));
                }
              }
            }
            // -----------------------------------

            if (failedGroups.isEmpty()) {
              // SUCCESS: Mark the lifecycle as complete
              lifecycle.complete(null);
            } else {
              // FAILURE: Check if we can retry
              if (attempt >= MAX_RETRIES) {
                lifecycle.completeExceptionally(
                    new IOException("Exhausted retries for " + failedGroups.size() + " items."));
              } else {
                // NON-BLOCKING BACKOFF:
                // specific delay calculation
                long delayMs = (long) Math.pow(2, attempt) * 1000 + (long) (Math.random() * 500);

                log.info("Partial failure. Scheduling retry {} in {}ms", attempt + 1, delayMs);

                // Schedule the NEXT attempt. This thread terminates immediately.
                scheduler.schedule(
                    () -> dispatchWriteAtLeastOnceRecursive(failedGroups, attempt + 1, lifecycle),
                    delayMs,
                    TimeUnit.MILLISECONDS);
              }
            }

          } catch (Exception e) {
            // Handle fatal transport errors by scheduling a retry of the whole batch
            if (attempt < MAX_RETRIES) {
              long delayMs = (long) Math.pow(2, attempt) * 1000;
              scheduler.schedule(
                  () -> dispatchWriteAtLeastOnceRecursive(currentBatch, attempt + 1, lifecycle),
                  delayMs,
                  TimeUnit.MILLISECONDS);
            } else {
              lifecycle.completeExceptionally(e);
            }
          }
        });
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
      throw new RuntimeException("Write failed", e);
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
        // Log warning (e.g., via Logger or System.err)
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
        // Log warning (e.g., via Logger or System.err)
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

  /** Recursive helper to estimate size of ANY Spark value */
  private long estimateValueSize(Object val, DataType type) {
    if (val == null) return 0;

    // 1. Strings (Most common heavy hitter)
    if (type instanceof StringType) {
      return ((UTF8String) val).numBytes();
    }

    // 2. Structs (Recursion)
    else if (type instanceof StructType) {
      StructType structType = (StructType) type;
      InternalRow nestedRow = (InternalRow) val;
      long structSize = 0;
      StructField[] fields = structType.fields();

      for (int i = 0; i < nestedRow.numFields(); i++) {
        if (!nestedRow.isNullAt(i)) {
          structSize +=
              estimateValueSize(nestedRow.get(i, fields[i].dataType()), fields[i].dataType());
        }
      }
      return structSize;
    }

    // 3. Arrays (Smart Iteration)
    else if (type instanceof ArrayType) {
      ArrayData arrayData = (ArrayData) val;
      DataType elementType = ((ArrayType) type).elementType();
      return estimateArraySize(arrayData, elementType);
    }

    // 4. Maps (Keys + Values)
    else if (type instanceof MapType) {
      MapData mapData = (MapData) val;
      MapType mapType = (MapType) type;
      // A Map is just two Arrays: Keys and Values
      return estimateArraySize(mapData.keyArray(), mapType.keyType())
          + estimateArraySize(mapData.valueArray(), mapType.valueType());
    }

    // 5. Binary
    else if (type instanceof BinaryType) {
      return ((byte[]) val).length;
    }

    // 6. Primitives (Fixed Width) - Safe to use constant
    else {
      return type.defaultSize();
    }
  }

  private long estimateArraySize(ArrayData arr, DataType elementType) {
    long size = 0;
    int numElements = arr.numElements();

    // OPTIMIZATION: If element is fixed-width (Int, Long, Timestamp),
    // don't iterate! Just multiply.
    if (isFixedWidth(elementType)) {
      return (long) numElements * elementType.defaultSize();
    }

    // SLOW PATH: Variable width (String, Struct, Nested Array)
    // We must iterate to be safe.
    for (int i = 0; i < numElements; i++) {
      if (arr.isNullAt(i)) continue;

      Object element = arr.get(i, elementType);
      size += estimateValueSize(element, elementType);
    }
    return size;
  }

  private boolean isFixedWidth(DataType type) {
    return (type instanceof IntegerType
        || type instanceof LongType
        || type instanceof DoubleType
        || type instanceof FloatType
        || type instanceof BooleanType
        || type instanceof TimestampType
        || type instanceof DateType);
  }
}
