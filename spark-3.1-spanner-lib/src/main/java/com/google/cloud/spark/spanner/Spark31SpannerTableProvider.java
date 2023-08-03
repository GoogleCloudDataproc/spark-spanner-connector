/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.spanner;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class Spark31SpannerTableProvider
    implements CreatableRelationProvider, DataSourceRegister, TableProvider {

  /*
   * Infers the schema of the table identified by the given options.
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    SpannerTable st = new SpannerTable(null, options);
    return st.schema();
  }

  /*
   * Returns a Table instance with the specified table schema,
   * partitioning and properties to perform a read or write.
   */
  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new SpannerTable(schema, properties);
  }

  /*
   * Returns true if the source has the ability of
   * accepting external table metadata when getting tables.
   */
  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }

  /*
   * Implements DataSourceRegister.shortName(). This method allows Spark to match
   * the DataSource when spark.read(...).format("cloud-spanner") is invoked.
   */
  @Override
  public String shortName() {
    return "cloud-spanner";
  }

  /*
   * Implements CreateRelationProvider.createRelation which essentially saves
   * a DataFrame to the destination using the data-source specific parameters.
   */
  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      SaveMode mode,
      scala.collection.immutable.Map<String, String> parameters,
      Dataset<Row> data) {
    return new SpannerBaseRelation(sqlContext, mode, parameters, data);
  }
}
