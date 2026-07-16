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
import org.apache.spark.sql.types.DataType;

public final class LiteralExpr implements ValueExpr {

  private final Object value;
  private final DataType sparkType;

  public LiteralExpr(Object value, DataType sparkType) {
    this.value = value;
    this.sparkType = Objects.requireNonNull(sparkType, "sparkType cannot be null");
  }

  public Object getValue() {
    return value;
  }

  public DataType getSparkType() {
    return sparkType;
  }

  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
