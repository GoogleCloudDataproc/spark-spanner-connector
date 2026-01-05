package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/** Builder for {@link SpannerGraphScanner} */
public class SpannerGraphScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

  private final SpannerGraph spannerGraph;
  private @Nullable Set<String> requiredColumns;

  public SpannerGraphScanBuilder(SpannerGraph spannerGraph) {
    this.spannerGraph = spannerGraph;
  }

  @Override
  public Scan build() {
    return new SpannerGraphScanner(
        spannerGraph.options,
        spannerGraph.configs.extraHeaders,
        spannerGraph.readTimestamp,
        spannerGraph.configs.partitionSizeBytes,
        spannerGraph.dataBoostEnabled,
        spannerGraph.spannerGraphQuery,
        requiredColumns,
        SpannerUtils.pruneSchema(spannerGraph.schema(), requiredColumns));
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredColumns = ImmutableSet.copyOf(requiredSchema.fieldNames());
  }
}
