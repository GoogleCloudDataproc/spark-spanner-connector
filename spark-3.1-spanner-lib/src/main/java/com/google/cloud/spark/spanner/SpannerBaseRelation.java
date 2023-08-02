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

import java.util.HashMap;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

/*
 * SpannerBaseRelation implements BaseRelation.
 */
public class SpannerBaseRelation extends BaseRelation {
  private final SQLContext sqlContext;
  private final StructType schema;
  private final SpannerScanner scan;

  public SpannerBaseRelation(SQLContext sqlContext, Map<String, String> opts, StructType schema) {
    this.scan = new SpannerScanner(scalaToJavaMap(opts));
    this.sqlContext = sqlContext;
    this.schema = schema;
  }

  /*
   * needsConversion is a BaseRelation method that returns whether it is needed
   * to convert the objects in Row to internal representation, for example:
   *    java.lang.Decimal to Decimal
   *    java.lang.String to UTF8String
   */
  @Override
  public boolean needConversion() {
    return true;
  }

  @Override
  public long sizeInBytes() {
    // TODO: Calculate the sizes from the schema's values.
    // TODO: Perhaps we can quickly calculate those sizes
    //       as we construct the table from Cloud Spanner.
    return -1;
  }

  @Override
  public Filter[] unhandledFilters(Filter[] filters) {
    // TODO: Implement me.
    return null;
  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

  @Override
  public StructType schema() {
    if (this.schema == null) {
      return this.schema;
    }
    return this.scan.readSchema();
  }

  private <K, V> java.util.Map<K, V> scalaToJavaMap(scala.collection.immutable.Map<K, V> map) {
    java.util.Map<K, V> result = new HashMap<>();
    map.foreach(entry -> result.put(entry._1(), entry._2()));
    return result;
  }
}
