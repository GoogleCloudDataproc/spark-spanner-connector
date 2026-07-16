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

package com.google.cloud.spark.spanner.planning.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public final class InExpr implements BoolExpr {
  private final ValueExpr left;
  private final List<ValueExpr> values;

  public InExpr(ValueExpr left, List<ValueExpr> values) {
    this.left = Objects.requireNonNull(left, "left cannot be null");
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values cannot be null"));
  }

  public ValueExpr getLeft() {
    return left;
  }

  public List<ValueExpr> getValues() {
    return values;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
