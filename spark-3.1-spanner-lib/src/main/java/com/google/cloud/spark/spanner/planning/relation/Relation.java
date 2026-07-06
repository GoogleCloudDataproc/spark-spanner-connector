package com.google.cloud.spark.spanner.planning.relation;

import java.io.Serializable;

public interface Relation extends Serializable {
  <T> T accept(RelationVisitor<T> visitor);
}
