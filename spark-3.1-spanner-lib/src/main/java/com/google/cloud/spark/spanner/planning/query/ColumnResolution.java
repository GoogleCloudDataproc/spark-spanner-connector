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

import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POJO used in mapping Spark column name aliases to physical column names. Used to avoid column
 * name ambiguity when name column name is used by more that one table.
 */
public final class ColumnResolution {

  private static final Logger logger = LoggerFactory.getLogger(ColumnResolution.class);

  // column name provided by Spark which will be an alias if multiple columns share the same name.
  private final String
      outputName; // this is the column alias name if exists otherwise the physical column name.
  private final String columnName; // physical table column name
  private final String tableAlias;
  private final DataType sparkType;
  private final boolean nullable;

  public ColumnResolution(
      String outputName,
      String columnName,
      String tableAlias,
      DataType sparkType,
      boolean nullable) {
    this.outputName = outputName;
    this.columnName = columnName;
    this.tableAlias = tableAlias;
    this.sparkType = sparkType;
    this.nullable = nullable;
    logger.debug(
        "outputname: {}, columnName: {}, tableAlias: {}, sparkType: {}, nullable: {}",
        outputName,
        columnName,
        tableAlias,
        sparkType,
        nullable);
  }

  public String getOutputName() {
    return outputName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  public DataType getSparkType() {
    return sparkType;
  }

  public boolean isNullable() {
    return nullable;
  }
}
