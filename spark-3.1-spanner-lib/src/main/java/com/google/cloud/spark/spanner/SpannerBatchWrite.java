// Copyright 2025 Google LLC
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

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerBatchWrite implements BatchWrite {
  private static final Logger log = LoggerFactory.getLogger(SpannerBatchWrite.class);

  private final LogicalWriteInfo info;

  public SpannerBatchWrite(LogicalWriteInfo info) {
    this.info = info;
    log.info("Creating SpannerBatchWrite for queryId {}", info.queryId());
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new SpannerDataWriterFactory(
        this.info.options().asCaseSensitiveMap(), this.info.schema());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    // This is a no-op. There is no per-batch resources allocated.
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // This is a no-op. There is no per-batch resources allocated.
  }
}
