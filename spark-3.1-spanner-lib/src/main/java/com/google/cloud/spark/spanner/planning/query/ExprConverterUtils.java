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

import com.google.cloud.spark.spanner.planning.expression.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprConverterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExprConverterUtils.class);

  public static ColumnExpr toColumn(String name, StructType schema) {
    logger.debug("Looking up column '{}' in schema {}", name, schema.treeString());

    StructField field = schema.apply(name);

    return new ColumnExpr(name, field.dataType(), field.nullable());
  }

  public static LiteralExpr toLiteral(Object value, StructType schema, String columnName) {
    logger.debug("Looking up literal column '{}' in schema {}", columnName, schema.treeString());
    logger.info(
        "Literal value class={}, value={}",
        value == null ? null : value.getClass().getName(),
        value);
    StructField field = schema.apply(columnName);

    return new LiteralExpr(value, field.dataType());
  }
}
