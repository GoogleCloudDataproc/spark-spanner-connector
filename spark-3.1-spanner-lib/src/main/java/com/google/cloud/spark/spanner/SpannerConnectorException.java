package com.google.cloud.spark.spanner;

public class SpannerConnectorException extends RuntimeException {

  final SpannerErrorCode errorCode;

  public SpannerConnectorException(String message) {
    this(SpannerErrorCode.UNKNOWN, message);
  }

  public SpannerConnectorException(String message, Throwable cause) {
    this(SpannerErrorCode.UNKNOWN, message, cause);
  }

  public SpannerConnectorException(SpannerErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public SpannerConnectorException(SpannerErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public SpannerErrorCode getErrorCode() {
    return errorCode;
  }
}
