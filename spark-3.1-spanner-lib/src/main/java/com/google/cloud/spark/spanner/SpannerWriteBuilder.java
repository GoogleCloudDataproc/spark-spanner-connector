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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerWriteBuilder implements WriteBuilder, SupportsTruncate {

  private static final Logger log = LoggerFactory.getLogger(SpannerWriteBuilder.class);
  private final LogicalWriteInfo info;
  private final CaseInsensitiveStringMap properties;
  private final StructType schema;

  public SpannerWriteBuilder(LogicalWriteInfo info, CaseInsensitiveStringMap properties) {
    this.info = info;
    this.properties = properties;
    this.schema = info.schema();
  }

  @Override
  public BatchWrite buildForBatch() {
    return new SpannerBatchWrite(info, properties);
  }

  @Override
  public WriteBuilder truncate() {
    CaseInsensitiveStringMap opts = this.info.options();
    String overwriteMode = opts.getOrDefault("overwriteMode", "truncate");

    if (overwriteMode.equalsIgnoreCase("recreate")) {
      recreateTable(this.properties);
    } else if (overwriteMode.equalsIgnoreCase("truncate")) {
      truncateTable(this.properties);
    } else {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Unsupported overwriteMode '"
              + overwriteMode
              + "'. Supported modes are 'recreate' and 'truncate'.");
    }

    return this;
  }

  private void recreateTable(CaseInsensitiveStringMap opts) {
    String projectId = SpannerUtils.getRequiredOption(opts, "projectId");
    String instanceId = SpannerUtils.getRequiredOption(opts, "instanceId");
    String databaseId = SpannerUtils.getRequiredOption(opts, "databaseId");
    String tableName = SpannerUtils.getRequiredOption(opts, "table");

    try (Spanner spanner = SpannerUtils.buildSpannerOptions(opts).getService()) {
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      Dialect dialect = dbAdminClient.getDatabase(instanceId, databaseId).getDialect();
      SpannerInformationSchema schemaInfo = SpannerInformationSchema.create(dialect);

      String dropDdl = schemaInfo.dropTableDdl(tableName);
      dbAdminClient
          .updateDatabaseDdl(instanceId, databaseId, Collections.singletonList(dropDdl), null)
          .get();

      // Create the table.
      Identifier ident = Identifier.of(new String[0], tableName);
      String createDdl = schemaInfo.createTableDdl(ident, this.schema);
      dbAdminClient
          .updateDatabaseDdl(instanceId, databaseId, Collections.singletonList(createDdl), null)
          .get();

    } catch (InterruptedException | ExecutionException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION, "Error recreating table " + tableName, e);
    }
  }

  private void truncateTable(CaseInsensitiveStringMap opts) {
    String projectId = SpannerUtils.getRequiredOption(opts, "projectId");
    String instanceId = SpannerUtils.getRequiredOption(opts, "instanceId");
    String databaseId = SpannerUtils.getRequiredOption(opts, "databaseId");
    String tableName = SpannerUtils.getRequiredOption(opts, "table");

    try (Spanner spanner = SpannerUtils.buildSpannerOptions(opts).getService()) {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
      Dialect dialect = dbClient.getDialect();

      SpannerInformationSchema informationSchema = SpannerInformationSchema.create(dialect);

      truncateTable(dbClient, tableName, informationSchema);
    } catch (SpannerException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.SPANNER_FAILED_TO_EXECUTE_QUERY, "Error truncating table " + tableName, e);
    }
  }

  private long truncateTable(
      DatabaseClient dbClient, String tableName, SpannerInformationSchema informationSchema) {

    Statement statement = informationSchema.truncateTableDml(tableName);

    try {
      // Execute partitioned update. This is a blocking call.
      long deletedRowCount = dbClient.executePartitionedUpdate(statement);
      log.info("Successfully deleted " + deletedRowCount + " rows.");
      return deletedRowCount;

    } catch (SpannerException e) {
      log.error("Failed to execute Partitioned DML on table: " + tableName, e);
      throw e;
    }
  }
}
