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

import java.io.IOException;
import org.apache.spark.sql.connector.read.PartitionReader;

public class SpannerPartitionReader<T> implements PartitionReader<T> {

  private InputPartitionReaderContext<T> context;

  public SpannerPartitionReader(InputPartitionReaderContext<T> context) {
    this.context = context;
  }

  @Override
  public boolean next() throws IOException {
    return this.context.next();
  }

  @Override
  public T get() {
    return this.context.get();
  }

  @Override
  public void close() throws IOException {
    this.context.close();
  }
}
