package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.graph.query.SpannerGraphQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Represents the Spanner Graph data source in Spark */
public class SpannerGraph implements Table, SupportsRead, SupportsWrite {

  static final List<String> requiredOptions =
      ImmutableList.of("projectId", "instanceId", "databaseId", "graph", "type");

  public final Map<String, String> options;
  public final Options.ReadAndQueryOption dataBoostEnabled;
  public final SpannerGraphConfigs configs;
  public final @Nullable Statement directQuery;
  public final boolean nodeDataframe;
  public final SpannerGraphQuery spannerGraphQuery;
  public final TimestampBound readTimestamp;
  public final String graphName;

  SpannerGraph(
      Map<String, String> options,
      String graphName,
      SpannerGraphConfigs configs,
      @Nullable Statement directQuery,
      boolean dataBoost,
      boolean node,
      TimestampBound readTimestamp,
      SpannerGraphQuery spannerGraphQuery) {
    checkOptions(options);
    this.graphName = graphName;
    this.options = new CaseInsensitiveStringMap(options);
    this.configs = Objects.requireNonNull(configs);
    this.directQuery = directQuery;
    this.dataBoostEnabled = Options.dataBoostEnabled(dataBoost);
    this.nodeDataframe = node;
    this.readTimestamp = readTimestamp;
    this.spannerGraphQuery = spannerGraphQuery;
  }

  static void checkOptions(Map<String, String> options) {
    for (String o : requiredOptions) {
      Objects.requireNonNull(options.get(o), "missing " + o + " in the options");
    }
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SpannerGraphScanBuilder(this);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    throw new SpannerConnectorException(
        SpannerErrorCode.WRITES_NOT_SUPPORTED,
        "writes are not supported in the Spark Spanner Connector");
  }

  @Override
  public String name() {
    return graphName;
  }

  /** Returns the schema of this table. */
  @Override
  public StructType schema() {
    return spannerGraphQuery.dataframeSchema;
  }

  /** Returns the set of capabilities for this table. */
  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }
}
