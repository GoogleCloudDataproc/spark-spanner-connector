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

  private String buildSQL(String[] requiredColumns, Filter[] filters) {
    // 1. Create the conjuction of filters.
    String filtered = getCompiledFilter(filters);
    // 2. Join the requiredColumns by ","
    String whereClause = filtered.length() > 0 ? " WHERE " + filtered : "";
    // 3. Build the final SQL string and return it.
    return "SELECT " + String.join(", ", requiredColumns) + " FROM " + table + whereClause;
  }

  private String getCompiledFilter(Filter[] filters) {
    return SparkFilterUtils.getCompiledFilter(true, null, filters);
  }

  /*
   * This method overrides PrunedFilteredScan.buildScan
   */
  @Override
  public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
    // requiredColumns=["id", "first_name"] or []
    //  if requiredColumns=[], turn it into "*"
    // filters = [(age > 10),
    // TODO: Figure out what requiredColumns would be for the 2 queries below:
    //  SELECT COUNT(*) FROM <TABLE> WHERE <FILTERS> -- that requiredColumns is NULL
    //  SELECT * FROM <TABLE> WHERE <FILTERS> -- question is WHAT WOULD BE requiredColumns in this
    // case
    //
    //  SELECT * FROM <TABLE> -- if requiredColumns is NULL.
    //  1. Run Hao's experiment and print out what required columns is
    //  2. Asking David in a Github issue.
    //
    //  Hao's suggestions:
    //      * in the beginning just return ALL columns, then later ask David what they did for
    // BigQuery
    //      * if aggregating on *, use the primary index -- this case is not valid for Cloud Spanner
    //
    //  SELECT * FROM Games WHERE 1=1 GROUP BY id
    //
    // Verdicts:
    //  * Treat "*" as fetch all columns
    // Straightforward:
    //  SELECT COL1, COL9 FROM <TABLE> (WHERE <FILTERS>)?
    //
    //
    // TODO: Implement me.
    // 1. Create a SQL query from the table by column names
    // and the conjunction of filters by an "AND".
    String sql = buildSQL(requiredColumns, filters);
    log.info("SQL built {}", sql);

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
