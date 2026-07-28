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
import java.util.Map;
import org.apache.spark.sql.sources.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprConverterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExprConverterUtils.class);

  public static ColumnExpr toColumn(String name, Map<String, ColumnResolution> resolutionMap) {
    logger.debug("Looking up column '{}' in resolutionMap {}", name, resolutionMap);

    ColumnResolution columnResolution = resolutionMap.get(name);

    return new ColumnExpr(
        columnResolution.getTableAlias(),
        columnResolution.getColumnName(),
        columnResolution.getSparkType(),
        columnResolution.isNullable());
  }

  public static LiteralExpr toLiteral(
      Object value, Map<String, ColumnResolution> resolutionMap, String columnName) {
    logger.debug("Looking up literal column '{}' in resolutionMap {}", columnName, resolutionMap);
    logger.info(
        "Literal value class={}, value={}",
        value == null ? null : value.getClass().getName(),
        value);
    ColumnResolution columnResolution = resolutionMap.get(columnName);

    return new LiteralExpr(value, columnResolution.getSparkType());
  }
}
