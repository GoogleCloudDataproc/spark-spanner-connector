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

import java.util.Objects;

public final class StartsWithExpr implements BoolExpr {
  private final ValueExpr left;
  private final ValueExpr prefix;

  public StartsWithExpr(ValueExpr left, ValueExpr prefix) {
    this.left = Objects.requireNonNull(left, "left cannot be null");
    this.prefix = Objects.requireNonNull(prefix, "prefix cannot be null");
  }

  public ValueExpr getLeft() {
    return left;
  }

  public ValueExpr getPrefix() {
    return prefix;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
