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
