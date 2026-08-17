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

package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import java.util.Objects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Executes user-provided SQL without rewriting its statement or inferred output schema. */
public final class DirectSqlQueryPlan implements ExecutableQuery {

  private final CaseInsensitiveStringMap options;
  private final StructType schema;
  private final Statement statement;

  /** Creates an executable plan for a query whose result schema has already been inferred. */
  public DirectSqlQueryPlan(
      CaseInsensitiveStringMap options, StructType schema, Statement statement) {
    this.options = Objects.requireNonNull(options, "options");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  @Override
  public CaseInsensitiveStringMap getOptions() {
    return options;
  }

  @Override
  public StructType getReadSchema() {
    return schema;
  }

  @Override
  public Statement buildStatement(Dialect dialect) {
    return statement;
  }
}
