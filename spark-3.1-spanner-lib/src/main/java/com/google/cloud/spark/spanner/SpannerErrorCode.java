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

public enum SpannerErrorCode {
  SPANNER_FAILED_TO_EXECUTE_QUERY(0),
  SPANNER_FAILED_TO_PARSE_OPTIONS(1),
  COLUMNAR_READS_NOT_SUPPORTED(2),
  WRITES_NOT_SUPPORTED(3),
  RESOURCE_EXHAUSTED_ON_SPANNER(4),

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
