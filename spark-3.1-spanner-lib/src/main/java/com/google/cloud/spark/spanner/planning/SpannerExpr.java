package com.google.cloud.spark.spanner.planning;

import java.io.Serializable;

public interface SpannerExpr extends Serializable {
  <T> T accept(SpannerExprVisitor<T> visitor);
}
