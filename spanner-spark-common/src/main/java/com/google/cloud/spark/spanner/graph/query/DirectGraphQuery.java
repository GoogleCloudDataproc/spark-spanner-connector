package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.ReadContext.QueryAnalyzeMode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.SpannerRowConverter;
import com.google.cloud.spark.spanner.SpannerRowConverterDirect;
import com.google.cloud.spark.spanner.SpannerTableSchema;
import com.google.common.collect.ImmutableSet;
import com.google.spanner.v1.ResultSetMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** A user-provided GQL query for fetching nodes/edges */
public class DirectGraphQuery implements GraphSubQuery {

  private final Statement query;
  private final List<StructField> outputSparkFields;

  public DirectGraphQuery(Connection conn, Statement query, boolean node) {
    this.query = query;
    this.outputSparkFields = Collections.unmodifiableList(getOutputSparkFields(conn, query, node));
  }

  private static List<StructField> getOutputSparkFields(
      Connection conn, Statement query, boolean node) {
    final Set<String> idColumns = node ? ImmutableSet.of("id") : ImmutableSet.of("src", "dst");

    List<StructField> fields;
    try (ResultSet rs = conn.analyzeQuery(query, QueryAnalyzeMode.PLAN)) {
      fields = resultSetMetadataToSchema(rs.getMetadata(), idColumns);
    }
    for (String idColumn : idColumns) {
      boolean hasField = fields.stream().map(StructField::name).anyMatch(n -> n.equals(idColumn));
      if (!hasField) {
        throw new IllegalArgumentException(
            String.format(
                "Column %s missing in the query output. Query: %s. Spark fields: %s",
                idColumn, query, fields));
      }
    }
    return fields;
  }

  private static List<StructField> resultSetMetadataToSchema(
      ResultSetMetadata metadata, Set<String> notNullableColumns) {
    List<StructField> fields = new ArrayList<>();
    for (com.google.spanner.v1.StructType.Field column : metadata.getRowType().getFieldsList()) {
      String name = column.getName();
      String type = column.getType().getCode().name();
      boolean isNullable = !notNullableColumns.contains(name);
      fields.add(SpannerTableSchema.getSparkStructField(name, type, isNullable, false));
    }
    return fields;
  }

  @Override
  public Tuple<Statement, SpannerRowConverter> getQueryAndConverter(StructType dataframeSchema) {
    if (Arrays.equals(outputSparkFields.toArray(new StructField[0]), dataframeSchema.fields())) {
      return Tuple.of(query, new SpannerRowConverterDirect());
    } else {
      String selectedColumnNames =
          Arrays.stream(dataframeSchema.fields())
              .map(StructField::name)
              .collect(Collectors.joining(", "));
      if (selectedColumnNames.isEmpty()) {
        selectedColumnNames = "null";
      }
      String prunedSql = String.format("SELECT %s FROM (%s)", selectedColumnNames, query.getSql());
      return Tuple.of(
          query.toBuilder().replace(prunedSql).build(), new SpannerRowConverterDirect());
    }
  }

  @Override
  public List<StructField> getOutputSparkFields() {
    return outputSparkFields;
  }
}
