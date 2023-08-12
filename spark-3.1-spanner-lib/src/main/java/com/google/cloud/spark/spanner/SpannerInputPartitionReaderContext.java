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

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;

public class SpannerInputPartitionReaderContext
    implements InputPartitionReaderContext<InternalRow> {

  private BatchReadOnlyTransaction txn;
  private ResultSet rs;

  public SpannerInputPartitionReaderContext(BatchReadOnlyTransaction txn, ResultSet rs) {
    this.txn = txn;
    this.rs = rs;
  }

  @Override
  public boolean next() throws IOException {
    return this.rs.next();
  }

  @Override
  public InternalRow get() {
    return SpannerUtils.resultSetRowToInternalRow(this.rs);
  }

  @Override
  public void close() throws IOException {
    this.rs.close();
    this.txn.close();
  }
}
