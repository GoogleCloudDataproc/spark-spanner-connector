// Copyright 2023 Google LLC
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

package com.google.cloud.spark.spanner;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerRelation
    implements InsertableRelation, TableScan, PrunedFilteredScan, PrunedScan {

  private final SparkSession session;
  private final String table;
  private static final Logger log = LoggerFactory.getLogger(SpannerRelation.class);

  public SpannerRelation(String table, SparkSession session) {
    this.table = table;
    this.session = session;
  }

  /*
   * This method overrides PrunedFilteredScan.buildScan
   */
  @Override
  public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
    // TODO: Implement me.
    // 1. Create a SQL query from the table by column names
    // and the conjunction of filters by an "AND".
    // SpannerTable st = new SpannerTable(this.session.getAll());

    // 2. Query Cloud Spanner with the constructed SQL query.
    // 3. Convert the results to RDD per ResultSet.Row, as we do in SpannerSpark.execute.
    // TODO: https://github.com/GoogleCloudDataproc/spark-spanner-connector/issues/45
    log.error("Unimplemented:: buildScan");
    return null;
  }

  /*
   * This method overrides PrunedScan.buildScan
   */
  @Override
  public RDD<Row> buildScan(String[] requiredColumns) {
    // TODO: Implement me.
    return this.buildScan(requiredColumns, null);
  }

  /*
   * This method overrides TableScan.buildScan
   */
  @Override
  public RDD<Row> buildScan() {
    return this.buildScan(null, null);
  }

  /*
   * This method overrides InsertableRelation.insert
   */
  @Override
  public void insert(Dataset<Row> data, boolean overwrite) {
    // TODO: Implement me.
    log.error("Unimplemented:: insert");
  }
}
