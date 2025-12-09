package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SessionPoolOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  private final int partitionId;
  private final long taskId;
  private final String tableName;
  private final StructType schema;

  // Limits
  private final int maxMutationsPerBatch;
  private final long bytesPerTransaction;

  // Connector options
  private final boolean assumeIdempotentWrites;

  // Buffers
  private final List<Mutation> mutationBuffer = new ArrayList<>();
  private final BatchClientWithCloser batchClient;
  private long currentBatchBytes = 0;

  // Async Execution
  private final ExecutorService executor;
  private final List<Future<?>> pendingWrites = new ArrayList<>();

  public SpannerDataWriter(
      int partitionId, long taskId, Map<String, String> properties, StructType schema) {
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.tableName = properties.get("table");
    this.schema = schema;

    // Default to 1MB (Safety) and 1000 Mutations
    this.maxMutationsPerBatch =
        Integer.parseInt(properties.getOrDefault("mutationsPerTransaction", "1000"));
    this.bytesPerTransaction =
        Long.parseLong(properties.getOrDefault("bytesPerTransaction", "1048576")); // 1 MB default
    this.assumeIdempotentWrites =
        Boolean.parseBoolean(properties.getOrDefault("assumeIdempotentRows", "false"));
      int numThreads = Integer.parseInt(properties.getOrDefault("numWriteThreads", "8"));
    SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder().setMinSessions(1).setMaxSessions(numThreads).build();
    this.batchClient = SpannerUtils.batchClientFromProperties(properties, sessionPoolOptions);
    this.executor = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  public void write(InternalRow record) throws IOException {
    // 1. Convert to Spanner Mutation
    Mutation mutation = SpannerWriterUtils.internalRowToMutation(tableName, record, schema);

    // 2. Estimate Size (Crucial for preventing OOM)
    long mutationSize = estimateMutationSize(record, schema);

    // 3. Check if buffer is full (Count OR Bytes)
    if (!mutationBuffer.isEmpty()
        && (mutationBuffer.size() >= maxMutationsPerBatch
            || currentBatchBytes + mutationSize > bytesPerTransaction)) {
      flushBufferAsync();
    }

    // 4. Add to Buffer
    mutationBuffer.add(mutation);
    currentBatchBytes += mutationSize;
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    // Flush whatever is left in the buffer
    if (!mutationBuffer.isEmpty()) {
      flushBufferAsync();
    }

    // Wait for all async writes to finish
    waitForPendingWrites();

    // Clean up
    executor.shutdown();

    return new SpannerWriterCommitMessage(partitionId, taskId);
  }

  private void flushBufferAsync() throws IOException {
    // Create a copy of the buffer for the async thread
    final List<Mutation> batchToSend = new ArrayList<>(mutationBuffer);

    // Clear the main buffer immediately so we can keep reading
    mutationBuffer.clear();
    currentBatchBytes = 0;

    // Submit to thread pool
    Future<?> future =
        executor.submit(
            () -> {
              batchClient.databaseClient.write(batchToSend);
              return null;
            });

    pendingWrites.add(future);

    // Backpressure
    // If we have too many pending requests (e.g., > 20), wait for them to clear.
    // This prevents the ExecutorQueue from growing infinitely and causing OOM.
    if (pendingWrites.size() > 20) {
      waitForPendingWrites();
    }
  }

  private void waitForPendingWrites() throws IOException {
    try {
      for (Future<?> future : pendingWrites) {
        future.get(); // Blocks until completion, throws exception if write failed
      }
      pendingWrites.clear();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException("Failed to write batch to Spanner", e);
    }
  }

  @Override
  public void abort() throws IOException {
    mutationBuffer.clear();
    executor.shutdownNow();
  }

  @Override
  public void close() throws IOException {
    mutationBuffer.clear();
    if (!executor.isShutdown()) {
      executor.shutdown();
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
