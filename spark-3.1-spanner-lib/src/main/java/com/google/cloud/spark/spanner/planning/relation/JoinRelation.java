// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner.planning.relation;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import java.util.Objects;

public final class JoinRelation implements Relation {
  private final Relation left;
  private final Relation right;
  private final JoinType joinType;
  private final BoolExpr condition;

  public JoinRelation(Relation left, Relation right, JoinType joinType, BoolExpr condition) {
    this.left = Objects.requireNonNull(left, "left relation cannot be null");
    this.right = Objects.requireNonNull(right, "right relation cannot be null");
    this.joinType = Objects.requireNonNull(joinType, "joinType cannot be null");
    this.condition = Objects.requireNonNull(condition, "condition cannot be null");
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public BoolExpr getCondition() {
    return condition;
  }

  @Override
  public <T> T accept(RelationVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
