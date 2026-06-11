package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;

public final class LogicalQuery implements Serializable {
  public Relation getSource() {
    return source;
  }

  public ArrayList<String> getProjections() {
    return projections;
  }

  public Optional<BoolExpr> getFilter() {
    return filter;
  }

  private final Relation source;
  private final ArrayList<String> projections;
  private final Optional<BoolExpr> filter;

  public LogicalQuery(Relation source, ArrayList<String> projections, Optional<BoolExpr> filter) {

    this.source = source;
    this.projections = projections;
    this.filter = filter;
  }
}
