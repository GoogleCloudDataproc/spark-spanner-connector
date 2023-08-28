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
import com.google.cloud.spanner.Spanner;
import java.io.Closeable;

public class BatchClientWithCloser implements Closeable {
  public BatchClient batchClient;
  private Spanner spanner;

  public BatchClientWithCloser(Spanner spanner, BatchClient batchClient) {
    this.spanner = spanner;
    this.batchClient = batchClient;
  }

  @Override
  public void close() {
    this.spanner.close();
  }
}
