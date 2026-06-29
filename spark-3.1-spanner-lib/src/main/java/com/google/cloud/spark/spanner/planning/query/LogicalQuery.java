package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;

public final class LogicalQuery {
  public SpannerTable getSource() {
    return spannerTable;
  }

  public Set<String> getProjections() {
    return requiredColumns;
  }

  public Filter[] getFilter() {
    return pushedFilters != null ? pushedFilters.clone() : new Filter[0];
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

    if (spannerTable == null) {
      throw new NullPointerException("spannerTable cannot be null");
    }
    this.spannerTable = spannerTable;
    this.requiredColumns =
        requiredColumns != null ? requiredColumns : java.util.Collections.emptySet();
    this.pushedFilters = pushedFilters != null ? pushedFilters.clone() : new Filter[0];
    this.fields = fields != null ? fields : java.util.Collections.emptyMap();
  }
}
