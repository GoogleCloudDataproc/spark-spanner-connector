package com.google.cloud.spark.spanner.planning;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public final class LogicalQuery implements Serializable {
  private final Relation source;
  private final List<String> projections;
  private final Optional<BoolExpr> filter;

  public LogicalQuery(Relation source, List<String> projections, Optional<BoolExpr> filter) {

    this.source = source;
    this.projections = projections;
    this.filter = filter;
  }
}
