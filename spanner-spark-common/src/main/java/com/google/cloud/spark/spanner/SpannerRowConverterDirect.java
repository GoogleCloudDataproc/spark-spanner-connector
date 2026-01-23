package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.Struct;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;

/** Converts rows from Spanner query outputs to rows in a Spark DataFrame with 1:1 field mapping. */
public class SpannerRowConverterDirect implements SpannerRowConverter, Serializable {

  /**
   * Converts a spanner row to a Spark DataFrame row with 1:1 field mapping.
   *
   * @param spannerRow the row from Spanner to convert.
   * @return a Spark DataFrame row with the same length as the input. Each field in the output is
   *     converted directly from the field at the same position in the input.
   */
  @Override
  public InternalRow convert(Struct spannerRow) {
    return SpannerUtils.spannerStructToInternalRow(spannerRow);
  }
}
