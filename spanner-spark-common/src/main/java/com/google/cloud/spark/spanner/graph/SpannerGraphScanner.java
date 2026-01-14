package com.google.cloud.spark.spanner.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.cloud.Tuple;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.SpannerOptions.CallContextConfigurator;
import com.google.cloud.spark.spanner.*;
import com.google.cloud.spark.spanner.graph.query.SpannerGraphQuery;
import com.google.common.collect.ImmutableSet;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logically and physically represents a scan of a graph in Spanner */
public class SpannerGraphScanner implements Batch, Scan {

  private static final Logger log = LoggerFactory.getLogger(SpannerScanner.class);

  private final Map<String, String> options;
  private final @Nullable Map<String, List<String>> extraHeaders;
  private final TimestampBound readTimestamp;
  private final @Nullable Long partitionSizeBytes;
  private final Options.ReadAndQueryOption dataBoostEnabled;
  private final @Nullable ImmutableSet<String> requiredColumns;
  private final StructType readSchema;
  private final List<Tuple<Statement, SpannerRowConverter>> queryAndRowConverters;
  private final List<SpannerPartition> partitions;

  public SpannerGraphScanner(
      Map<String, String> options,
      @Nullable Map<String, List<String>> extraHeaders,
      TimestampBound readTimestamp,
      @Nullable Long partitionSizeBytes,
      Options.ReadAndQueryOption dataBoostEnabled,
      SpannerGraphQuery graphQuery,
      @Nullable Set<String> requiredColumns,
      StructType readSchema) {
    // Potential improvement: support filter pushdown.
    this.options = new CaseInsensitiveStringMap(options);
    this.extraHeaders = extraHeaders;
    this.readTimestamp = readTimestamp;
    this.partitionSizeBytes = partitionSizeBytes;
    this.dataBoostEnabled = dataBoostEnabled;
    this.requiredColumns = requiredColumns == null ? null : ImmutableSet.copyOf(requiredColumns);
    this.readSchema = readSchema;
    this.queryAndRowConverters =
        graphQuery.graphSubqueries.stream()
            .map(q -> q.getQueryAndConverter(readSchema))
            .collect(Collectors.toList());
    this.partitions = new ArrayList<>(); // Filled later
  }

  /**
   * Returns a list of {@link SpannerPartition input partitions}. Each {@link SpannerPartition}
   * represents a data split that can be processed by one Spark task. The number of input partitions
   * returned here is the same as the number of RDD partitions this scan outputs.
   */
  @Override
  public SpannerPartition[] planInputPartitions() {
    if (extraHeaders == null || extraHeaders.isEmpty()) {
      return doPlanInputPartitions();
    }
    Context context =
        Context.current()
            .withValue(
                SpannerOptions.CALL_CONTEXT_CONFIGURATOR_KEY,
                new CallContextConfigurator() {
                  @Override
                  public <ReqT, RespT> ApiCallContext configure(
                      ApiCallContext context, ReqT request, MethodDescriptor<ReqT, RespT> method) {
                    return GrpcCallContext.createDefault().withExtraHeaders(extraHeaders);
                  }
                });
    try {
      return context.call(this::doPlanInputPartitions);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SpannerPartition[] doPlanInputPartitions() {
    String optionsJson;
    try {
      optionsJson = SpannerUtils.serializeMap(options);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    try (BatchClientWithCloser batchClient = SpannerUtils.batchClientFromProperties(options)) {
      try (BatchReadOnlyTransaction txn =
          batchClient.batchClient.batchReadOnlyTransaction(readTimestamp)) {
        BatchTransactionId txnId = txn.getBatchTransactionId();
        PartitionOptions options = PartitionOptions.getDefaultInstance();
        if (partitionSizeBytes != null) {
          options = PartitionOptions.newBuilder().setPartitionSizeBytes(partitionSizeBytes).build();
        }

        partitions.clear();
        for (Tuple<Statement, SpannerRowConverter> queryAndRowConverter : queryAndRowConverters) {
          List<Partition> rawPartitions =
              txn.partitionQuery(options, queryAndRowConverter.x(), dataBoostEnabled);

          for (Partition rawPartition : rawPartitions) {
            SpannerInputPartitionContext context =
                new SpannerInputPartitionContext(
                    rawPartition, txnId, optionsJson, queryAndRowConverter.y());
            int index = partitions.size();
            partitions.add(new SpannerPartition(rawPartition, index, context));
          }
        }
        log.info("Number of partitions: " + partitions.size());
        return partitions.toArray(new SpannerPartition[0]);
      }
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SpannerPartitionReaderFactory();
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public String description() {
    return String.format(
        "%s\nRequired Columns: %s\nRead Timestamp: %s"
            + "\nStatements (%d):\n%s\nNumber of Partitions: %d",
        this.getClass(),
        requiredColumns,
        readTimestamp,
        queryAndRowConverters.size(),
        queryAndRowConverters.stream()
            .map(qc -> qc.x().toString())
            .collect(Collectors.joining("\n")),
        partitions.size());
  }
}
