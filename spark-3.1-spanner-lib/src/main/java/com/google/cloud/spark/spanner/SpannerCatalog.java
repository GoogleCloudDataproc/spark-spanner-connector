// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spark.spanner.graph.SpannerGraphBuilder;
import com.google.common.base.Verify;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerCatalog implements TableCatalog, AutoCloseable {
  public static final String GRAPH_IDENTIFIER_PREFIX = "__spanner_graph__";
  private static final Gson GSON = new Gson();

  public static final Metadata PRIMARY_KEY_METADATA =
      new MetadataBuilder().putBoolean(SpannerUtils.PRIMARY_KEY_TAG, true).build();
  private static final Logger log = LoggerFactory.getLogger(SpannerCatalog.class);
  private String catalogName;
  private CaseInsensitiveStringMap options;
  private Spanner spanner;
  private String projectId;
  private String instanceId;
  private String databaseId;

  // For testing purposes.
  protected Spanner createSpanner(CaseInsensitiveStringMap options) {
    return SpannerUtils.buildSpannerOptions(options).getService();
  }

  // For testing purposes.
  protected SpannerInformationSchema createSchemaInfo(Dialect dialect) {
    return SpannerInformationSchema.create(dialect);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.options = options;
    this.projectId = SpannerUtils.getRequiredOption(options, "projectId");
    this.instanceId = SpannerUtils.getRequiredOption(options, "instanceId");
    this.databaseId = SpannerUtils.getRequiredOption(options, "databaseId");
    this.spanner = createSpanner(options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    if (namespace.length > 0) {
      log.warn("Invalid namespace for listing tables: {}", String.join(".", namespace));
      return new Identifier[0];
    }

    DatabaseClient dbClient = getDatabaseClient();

    try (ReadContext readContext = dbClient.readOnlyTransaction()) {
      Dialect dialect = dbClient.getDialect();
      return createSchemaInfo(dialect).listTables(readContext, namespace);
    } catch (Exception e) {
      log.error(
          "Error listing tables in namespace {}: {}", String.join(".", namespace), e.getMessage());
      return new Identifier[0];
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    if (ident.namespace().length != 0) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Invalid identifier namespace: " + String.join(".", ident.namespace()));
    }
    if (isGraphIdentifier(ident)) {
      return factorySpannerGraph(ident);
    }
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident);
    }
    return factorySpannerTable(ident);
  }

  public static boolean isGraphIdentifier(Identifier ident) {
    return ident.name().startsWith(GRAPH_IDENTIFIER_PREFIX);
  }

  protected Table factorySpannerTable(Identifier ident) {
    String table = ident.name();
    Verify.verifyNotNull(table, "table");

    return new SpannerTable(projectId, instanceId, databaseId, table, options, null);
  }

  protected Table factorySpannerGraph(Identifier ident) {
    String json = ident.name().substring(GRAPH_IDENTIFIER_PREFIX.length());
    if (json.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "Graph identifier has no encoded properties");
    }
    Map<String, String> graphProps;
    try {
      graphProps = GSON.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    } catch (JsonSyntaxException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "Malformed graph identifier JSON: " + e.getMessage(),
          e);
    }
    if (graphProps == null) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "Graph identifier decoded to null");
    }
    Map<String, String> allOptions = new HashMap<>(options.asCaseSensitiveMap());
    for (String key : SparkSpannerTableProviderBase.GRAPH_OPTION_KEYS) {
      String val = graphProps.get(key);
      if (val != null) {
        allOptions.put(key, val);
      }
    }
    return SpannerGraphBuilder.build(allOptions);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {

    if (tableExists(ident)) {
      throw new TableAlreadyExistsException(ident);
    }

    DatabaseClient dbClient = getDatabaseClient();
    Dialect dialect = dbClient.getDialect();
    String ddl = toDdl(ident, schema, dialect);
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        dbAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(ddl), null);

    try {
      op.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION,
          "Exception while creating table " + ident.name() + ": " + e.getMessage(),
          e);
    }

    return factorySpannerTable(ident);
  }

  public static String toDdl(Identifier ident, StructType schema, Dialect dialect) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("CREATE TABLE ").append(quoteIdentifier(ident.name(), dialect)).append(" (");
    for (StructField field : schema.fields()) {
      ddl.append(quoteIdentifier(field.name(), dialect))
          .append(" ")
          .append(sparkTypeToSpannerType(field, dialect));
      if (!field.nullable()) {
        ddl.append(" NOT NULL");
      }
      ddl.append(", ");
    }

    List<String> primaryKeys =
        Arrays.stream(schema.fields())
            .filter(
                f ->
                    f.metadata().contains(SpannerUtils.PRIMARY_KEY_TAG)
                        && f.metadata().getBoolean(SpannerUtils.PRIMARY_KEY_TAG))
            .map(f -> quoteIdentifier(f.name(), dialect))
            .collect(Collectors.toList());

    if (primaryKeys.isEmpty()) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT,
          "No primary key found for table "
              + ident.name()
              + ". Please specify at least one primary key column.");
    }

    ddl.append("PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
    ddl.append(")");
    return ddl.toString();
  }

  static String quoteIdentifier(String identifier, Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static String sparkTypeToSpannerType(StructField field, Dialect dialect) {
    if (dialect == Dialect.POSTGRESQL) {
      if (field.dataType().equals(DataTypes.LongType)) {
        return "bigint";
      }
      if (field.dataType().equals(DataTypes.StringType)) {
        return "varchar";
      }
      if (field.dataType().equals(DataTypes.BooleanType)) {
        return "boolean";
      }
      if (field.dataType().equals(DataTypes.DoubleType)) {
        return "float8";
      }
      if (field.dataType().equals(DataTypes.BinaryType)) {
        return "bytea";
      }
      if (field.dataType().equals(DataTypes.TimestampType)) {
        return "timestamptz";
      }
      if (field.dataType().equals(DataTypes.DateType)) {
        return "date";
      }
      if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
        return "numeric";
      }
      if (field.dataType() instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) field.dataType();
        if (arrayType.elementType() instanceof ArrayType) {
          throw new SpannerConnectorException(
              SpannerErrorCode.UNSUPPORTED_DATATYPE,
              "Nested arrays are not supported by Spanner: " + field.dataType());
        }
        StructField elementField =
            DataTypes.createStructField(
                field.name(), arrayType.elementType(), arrayType.containsNull());
        return sparkTypeToSpannerType(elementField, dialect) + "[]";
      }
    }

    // GoogleSQL types
    if (field.dataType().equals(DataTypes.LongType)) {
      return "INT64";
    }
    if (field.dataType().equals(DataTypes.StringType)) {
      return "STRING(MAX)";
    }
    if (field.dataType().equals(DataTypes.BooleanType)) {
      return "BOOL";
    }
    if (field.dataType().equals(DataTypes.DoubleType)) {
      return "FLOAT64";
    }
    if (field.dataType().equals(DataTypes.BinaryType)) {
      return "BYTES(MAX)";
    }
    if (field.dataType().equals(DataTypes.TimestampType)) {
      return "TIMESTAMP";
    }
    if (field.dataType().equals(DataTypes.DateType)) {
      return "DATE";
    }
    if (field.dataType() instanceof org.apache.spark.sql.types.DecimalType) {
      return "NUMERIC";
    }
    if (field.dataType() instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) field.dataType();
      if (arrayType.elementType() instanceof ArrayType) {
        throw new SpannerConnectorException(
            SpannerErrorCode.UNSUPPORTED_DATATYPE,
            "Nested arrays are not supported by Spanner: " + field.dataType());
      }
      StructField elementField =
          DataTypes.createStructField(
              field.name(), arrayType.elementType(), arrayType.containsNull());
      return "ARRAY<" + sparkTypeToSpannerType(elementField, dialect) + ">";
    }

    throw new SpannerConnectorException(
        SpannerErrorCode.UNSUPPORTED_DATATYPE,
        "Unsupported data type in CREATE TABLE: " + field.dataType());
  }

  @Override
  public boolean tableExists(Identifier ident) {
    if (ident.namespace().length > 0) {
      log.warn("Invalid namespace for listing tables: {}", String.join(".", ident.namespace()));
      return false;
    }

    String tableName = ident.name();

    DatabaseClient dbClient = getDatabaseClient();

    try (ReadContext readContext = dbClient.singleUse()) {
      return createSchemaInfo(dbClient.getDialect()).tableExists(readContext, tableName);
    } catch (Exception e) {
      log.error("Error checking table existence {}: {}", tableName, e.getMessage());
      return false;
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("ALTER TABLE is not supported for SpannerCatalog");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    if (!tableExists(ident)) {
      return false;
    }

    Dialect dialect = getDatabaseClient().getDialect();
    String ddl = "DROP TABLE " + quoteIdentifier(ident.name(), dialect);

    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        dbAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(ddl), null);

    try {
      op.get();
      return true;
    } catch (ExecutionException | InterruptedException e) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DDL_EXCEPTION,
          "Exception while dropping table " + ident.name() + ": " + e.getMessage(),
          e);
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("RENAME TABLE is not supported for SpannerCatalog");
  }

  private DatabaseClient getDatabaseClient() {
    return spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
  }

  @Override
  public void close() {
    if (spanner != null) {
      spanner.close();
    }
  }
}
