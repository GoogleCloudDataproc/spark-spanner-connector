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

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;

public class BatchClientWithCloser implements AutoCloseable {
  public BatchClient batchClient;
  public DatabaseClient databaseClient;
  private Spanner spanner;

  public BatchClientWithCloser(
      Spanner spanner, BatchClient batchClient, DatabaseClient databaseClient) {
    this.spanner = spanner;
    this.batchClient = batchClient;
    this.databaseClient = databaseClient;
  }

  /*
   * close is a runtime hook for AutoCloseable to properly shut down resources
   * before this object is garbage collected. It is useful in scenarios such as
   * asynchronous processing for which we won't have a deterministic time/scope
   * for when a Spanner object will be closed.
   */
  @Override
  public void close() {
    if (this.spanner != null) {
      this.spanner.close();
      this.spanner = null;
    }
  }
}
