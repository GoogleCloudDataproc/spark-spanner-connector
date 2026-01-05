package com.google.cloud.spark.spanner;

import com.google.cloud.spanner.Struct;
import org.apache.spark.sql.catalyst.InternalRow;

/** Converts rows from Spanner query outputs to rows in Spark DataFrame */
public interface SpannerRowConverter {

  /** Generates a Spark row based on the Spanner row */
  InternalRow convert(Struct spannerRow);
}
