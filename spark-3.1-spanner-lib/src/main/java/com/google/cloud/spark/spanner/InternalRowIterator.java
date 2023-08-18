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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import java.util.Iterator;
import org.apache.spark.sql.catalyst.InternalRow;

// This class translates Cloud Spanner ResultSet.row to InternalRow per iteration.
// The underlying Spanner.ResultSet.close() is automatically invoked after
// the iterator in InternalRowIterator is exhausted.
public class InternalRowIterator implements Iterator<InternalRow> {

  private ResultSet rs;
  private Struct currentRow;
  private boolean nextOK;
  private final int columnCount;

  public InternalRowIterator(ResultSet rs) {
    this.columnCount = rs.getColumnCount();
    this.rs = rs;
  }

  @Override
  public boolean hasNext() {
    boolean ok = this.nextOK && this.currentRow != null;
    if (!ok && this.rs != null) {
      this.rs.close();
      this.rs = null;
    }
    return ok;
  }

  @Override
  public InternalRow next() {
    if (this.rs == null) {
      return null;
    }

    if (!this.rs.next()) {
      this.nextOK = false;
      this.currentRow = null;
      return null;
    }

    this.currentRow = this.rs.getCurrentRowAsStruct();
    return SpannerUtils.spannerStructToInternalRow(this.rs, this.currentRow, this.columnCount);
  }
}
