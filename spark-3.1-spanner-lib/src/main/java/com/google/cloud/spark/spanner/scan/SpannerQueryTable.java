package com.google.cloud.spark.spanner.scan;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.common.collect.ImmutableSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeAnnotationCode;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SpannerQueryTable implements Table, SupportsRead {

  private final CaseInsensitiveStringMap properties;
  private final Statement statement;
  private final StructType schema;

  public SpannerQueryTable(CaseInsensitiveStringMap properties) {
    String projectId = SpannerUtils.getRequiredOption(properties, "projectId");
    String instanceId = SpannerUtils.getRequiredOption(properties, "instanceId");
    String databaseId = SpannerUtils.getRequiredOption(properties, "databaseId");
    String query = SpannerUtils.getRequiredOption(properties, "query");
    this.properties = properties;
    this.statement = Statement.of(query);

    try (Connection connection =
        SpannerUtils.connectionFromProperties(
            projectId, instanceId, databaseId, properties.get("emulatorHost"))) {
      validateQuery(statement, connection.getDialect());
      connection.setReadOnly(true);
      connection.setAutocommit(true);
      connection.setReadOnlyStaleness(SpannerScanner.getReadTimestamp(properties));
      try (ResultSet resultSet = connection.analyzeQuery(statement, QueryAnalyzeMode.PLAN)) {
        this.schema = resultSetMetadataToSchema(resultSet.getMetadata());
      }
    }
  }

  static void validateQuery(Statement statement, Dialect dialect) {
    AbstractStatementParser parser = AbstractStatementParser.getInstance(dialect);
    if (!parser.isQuery(parser.removeCommentsAndTrim(statement.getSql()))) {
      throw new SpannerConnectorException(
          SpannerErrorCode.INVALID_ARGUMENT, "The query option must contain a read-only SQL query");
    }
  }

  static StructType resultSetMetadataToSchema(ResultSetMetadata metadata) {
    StructType result = new StructType();
    Set<String> columnNames = new HashSet<>();
    for (com.google.spanner.v1.StructType.Field field : metadata.getRowType().getFieldsList()) {
      String name = field.getName();
      if (name.isEmpty()) {
        throw new SpannerConnectorException(
            SpannerErrorCode.INVALID_ARGUMENT,
            "Every query output column must have a name; use an alias for expressions");
      }
      if (!columnNames.add(name.toLowerCase(Locale.ROOT))) {
        throw new SpannerConnectorException(
            SpannerErrorCode.INVALID_ARGUMENT, "Duplicate query output column name: " + name);
      }
      result = result.add(toStructField(field));
    }
    return result;
  }

  private static StructField toStructField(com.google.spanner.v1.StructType.Field field) {
    Type type = field.getType();
    MetadataBuilder metadata = new MetadataBuilder();
    if (type.getTypeAnnotation() == TypeAnnotationCode.PG_JSONB) {
      metadata.putString(SpannerUtils.COLUMN_TYPE, "jsonb");
    } else if (type.getCode() == com.google.spanner.v1.TypeCode.JSON) {
      metadata.putString(SpannerUtils.COLUMN_TYPE, "json");
    }
    return new StructField(field.getName(), toSparkDataType(type), true, metadata.build());
  }

  private static DataType toSparkDataType(Type type) {
    switch (type.getCode()) {
      case ARRAY:
        return DataTypes.createArrayType(toSparkDataType(type.getArrayElementType()), true);
      case STRUCT:
        StructType result = new StructType();
        for (com.google.spanner.v1.StructType.Field field : type.getStructType().getFieldsList()) {
          result = result.add(toStructField(field));
        }
        return result;
      default:
        DataType dataType = SpannerTable.ofSpannerStrType(type.getCode().name(), true);
        if (!DataTypes.NullType.equals(dataType)) {
          return dataType;
        }
        throw new SpannerConnectorException(
            SpannerErrorCode.UNSUPPORTED_DATATYPE,
            "Query output column type " + type.getCode() + " is not supported");
    }
  }

  @Override
  public String name() {
    return "Spanner query";
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return () -> new SpannerScanner(properties, schema, statement);
  }

  @Override
  public CaseInsensitiveStringMap properties() {
    return properties;
  }
}
