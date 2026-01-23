package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerRowConverter;
import java.util.List;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Handles a single SQL query (e.g., for an individual element table, for a GQL query) */
public interface GraphSubQuery {

  /**
   * Get the statement for this sub-query and a row converter that converts outputs to a row in the
   * DataFrame
   *
   * @param dataframeSchema schema of the DataFrame that will store the outputs
   * @return the statement and the row converter
   */
  Tuple<Statement, SpannerRowConverter> getQueryAndConverter(StructType dataframeSchema);

  /** Get a list of fields that this sub-query will output */
  List<StructField> getOutputSparkFields();
}
