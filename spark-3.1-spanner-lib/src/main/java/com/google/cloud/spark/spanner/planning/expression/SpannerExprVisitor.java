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

public interface SpannerExprVisitor<T> {
  T visit(EqExpr expr);

  T visit(EqNullSafeExpr expr);

  T visit(GtExpr expr);

  T visit(GteExpr expr);

  T visit(LtExpr expr);

  T visit(LteExpr expr);

  T visit(AndExpr expr);

  T visit(OrExpr expr);

  T visit(ColumnExpr expr);

  T visit(LiteralExpr expr);

  T visit(TrueExpr expr);

  T visit(IsNullExpr expr);

  T visit(IsNotNullExpr expr);

  T visit(InExpr expr);

  T visit(NotExpr expr);

  T visit(StartsWithExpr expr);

  T visit(EndsWithExpr expr);

  T visit(ContainsExpr expr);
}
