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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class FunctionExpr implements ValueExpr {

  public enum Function {
    LOWER(1),
    UPPER(1),
    TRIM(1),
    LENGTH(1),

    CONCAT(2, Integer.MAX_VALUE),
    COALESCE(2, Integer.MAX_VALUE),
    GREATEST(2, Integer.MAX_VALUE),
    LEAST(2, Integer.MAX_VALUE);

    private final int minArgs;
    private final int maxArgs;

    Function(int exactArgs) {
      this(exactArgs, exactArgs);
    }

    Function(int minArgs, int maxArgs) {
      this.minArgs = minArgs;
      this.maxArgs = maxArgs;
    }

    public void validate(List<ValueExpr> arguments) {
      int count = arguments.size();

      if (count < minArgs || count > maxArgs) {
        throw new IllegalArgumentException(
            String.format(
                "%s expects between %d and %d arguments, received %d",
                this, minArgs, maxArgs == Integer.MAX_VALUE ? Integer.MAX_VALUE : maxArgs, count));
      }
    }
  }

  private final Function function;
  private final List<ValueExpr> arguments;

  public FunctionExpr(Function function, List<ValueExpr> arguments) {

    this.function = Objects.requireNonNull(function, "function cannot be null");

    this.arguments =
        Collections.unmodifiableList(
            new ArrayList<>(Objects.requireNonNull(arguments, "arguments cannot be null")));

    function.validate(this.arguments);
  }

  public Function function() {
    return function;
  }

  public List<ValueExpr> arguments() {
    return arguments;
  }

  @Override
  public <T> T accept(SpannerExprVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
