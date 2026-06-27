package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;

public final class LogicalQuery implements Serializable {
  public SpannerTable getSource() {
    return spannerTable;
  }

  public Set<String> getProjections() {
    return requiredColumns;
  }

  public Filter[] getFilter() {
    return pushedFilters;
  }

  public Map<String, StructField> getFields() {
    return fields;
  }

  private final SpannerTable spannerTable;
  private final Set<String> requiredColumns;
  private final Filter[] pushedFilters;
  private final Map<String, StructField> fields;

  public LogicalQuery(
      SpannerTable spannerTable,
      Set<String> requiredColumns,
      Filter[] pushedFilters,
      Map<String, StructField> fields) {

    this.spannerTable = spannerTable;
    this.requiredColumns = requiredColumns;
    this.pushedFilters = pushedFilters;
    this.fields = fields;
  }
}
