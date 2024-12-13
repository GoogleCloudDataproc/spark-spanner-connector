package com.google.cloud.spark.spanner.graph;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.cloud.spark.spanner.SpannerErrorCode;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.cloud.spark.spanner.graph.query.SpannerGraphQuery;
import java.util.Map;
import java.util.Objects;

/** Builder for {@link SpannerGraph} */
public class SpannerGraphBuilder {

  public static SpannerGraph build(Map<String, String> options) {
    SpannerGraph.checkOptions(options);
    String graphName = Objects.requireNonNull(options.get("graph"));
    String directQueryString = options.get("graphQuery");
    boolean dataBoost = getEnableDataBoost(options);
    String configsJson = options.get("configs");
    SpannerGraphConfigs configs =
        configsJson != null ? SpannerGraphConfigs.fromJson(configsJson) : new SpannerGraphConfigs();
    boolean node = getIsNodeDataframe(options);
    TimestampBound readTimestamp = getReadTimestamp(options);
    Statement directQuery = directQueryString != null ? Statement.of(directQueryString) : null;

    SpannerGraphQuery graphQuery;
    try (Connection conn = getConnection(options)) {
      // Ensure the version of the schema read matches the specified timestamp
      conn.setReadOnly(true);
      conn.setAutocommit(true);
      conn.setReadOnlyStaleness(readTimestamp);

      PropertyGraph graphSchema = PropertyGraph.Builder.getFromSpanner(conn, graphName);
      configs.validate(graphSchema, directQuery != null);
      if (directQuery != null) {
        checkQueryIsSql(directQuery);
        // Test if the provided query is root-partitionable
        // Will throw an exception if the query is not root-partitionable
        conn.partitionQuery(directQuery, PartitionOptions.getDefaultInstance()).close();
        graphQuery = new SpannerGraphQuery(conn, directQuery, node);
      } else {
        graphQuery = new SpannerGraphQuery(conn, graphSchema, configs, node);
      }
    }

    return new SpannerGraph(
        options, graphName, configs, directQuery, dataBoost, node, readTimestamp, graphQuery);
  }

  private static boolean getEnableDataBoost(Map<String, String> options) {
    final String dataBoostEnabledKey = "enableDataBoost";
    String dataBoost = options.getOrDefault(dataBoostEnabledKey, "false");
    if ("true".equalsIgnoreCase(dataBoost)) {
      return true;
    } else if ("false".equalsIgnoreCase(dataBoost)) {
      return false;
    } else {
      throw new IllegalArgumentException(dataBoostEnabledKey + " must be true or false");
    }
  }

  private static boolean getIsNodeDataframe(Map<String, String> options) {
    String type = Objects.requireNonNull(options.get("type"));
    if ("node".equalsIgnoreCase(type)) {
      return true;
    } else if ("edge".equalsIgnoreCase(type)) {
      return false;
    } else {
      throw new IllegalArgumentException("type must be node or edge");
    }
  }

  private static TimestampBound getReadTimestamp(Map<String, String> options) {
    String timestamp = options.get("timestamp");
    return TimestampBound.ofReadTimestamp(
        timestamp == null ? Timestamp.now() : Timestamp.parseTimestamp(timestamp));
  }

  private static Connection getConnection(Map<String, String> options) {
    Connection conn = SpannerUtils.connectionFromProperties(options);
    if (!conn.getDialect().equals(Dialect.GOOGLE_STANDARD_SQL)) {
      throw new SpannerConnectorException(
          SpannerErrorCode.DATABASE_DIALECT_NOT_SUPPORTED,
          "Expecting dialect: GOOGLE_STANDARD_SQL, but the actual dialect used is "
              + conn.getDialect());
    }
    return conn;
  }

  private static void checkQueryIsSql(Statement query) {
    AbstractStatementParser parser =
        AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL);
    if (!parser.isQuery(parser.removeCommentsAndTrim(query.getSql()))) {
      throw new IllegalArgumentException(
          "Only SQL queries starting with SELECT are supported. Query provided: " + query);
    }
  }
}
