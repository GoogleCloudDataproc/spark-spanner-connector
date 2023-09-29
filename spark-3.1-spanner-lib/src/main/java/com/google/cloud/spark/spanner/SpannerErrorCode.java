package com.google.cloud.spark.spanner;

public enum SpannerErrorCode {
  SPANNER_FAILED_TO_EXECUTE_QUERY(0),
  SPANNER_FAILED_TO_PARSE_OPTIONS(1),
  COLUMNAR_READS_NOT_SUPPORTED(2),

  // Should be last
  UNSUPPORTED(9998),
  UNKNOWN(9999);

  final int code;

  SpannerErrorCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
