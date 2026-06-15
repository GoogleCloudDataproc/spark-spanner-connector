package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public final class LogicalQuery implements Serializable {
  public Relation getSource() {
    return source;
  }

  public List<String> getProjections() {
    return projections;
  }

  public Optional<BoolExpr> getFilter() {
    return filter;
  }

  private final Relation source;
  private final List<String> projections;
  private final Optional<BoolExpr> filter;

  public LogicalQuery(Relation source, List<String> projections, Optional<BoolExpr> filter) {

    this.source = source;
    this.projections = projections;
    this.filter = filter;
  }
}
