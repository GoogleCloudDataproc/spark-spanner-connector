package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SparkFilterUtils;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(SpannerQueryBuilder.class);

  private final LogicalQuery logicalQuery;
  private final Dialect dialect;
  private final SpannerTable spannerTable;
  private final Set<String> requiredColumns;
  private final Filter[] filters;
  private final Map<String, StructField> fields;

  private SpannerQueryBuilder(LogicalQuery logicalQuery, Dialect dialect) {
    this.logicalQuery = logicalQuery;
    this.dialect = dialect;
    this.spannerTable = logicalQuery.getSource();
    this.requiredColumns = logicalQuery.getProjections();
    this.filters = logicalQuery.getFilter();
    this.fields = logicalQuery.getFields();
  }

  public static SpannerQueryBuilder newBuilder(LogicalQuery logicalQuery, Dialect dialect) {
    return new SpannerQueryBuilder(logicalQuery, dialect);
  }

  public static String buildColumnsWithTablePrefix(
      String tableName, Set<String> columns, boolean isPostgreSql) {
    String quotedTableName = isPostgreSql ? "\"" + tableName + "\"" : "`" + tableName + "`";
    return columns.stream()
        .map(col -> isPostgreSql ? "\"" + col + "\"" : "`" + col + "`")
        .map(quotedCol -> quotedTableName + "." + quotedCol)
        .collect(Collectors.joining(", "));
  }

  private Statement buildLegacySql() {
    boolean isPostgreSql = this.dialect.equals(Dialect.POSTGRESQL);

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.logicalQuery.getProjections() != null && !this.requiredColumns.isEmpty()) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          buildColumnsWithTablePrefix(this.spannerTable.name(), this.requiredColumns, isPostgreSql);
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    String quotedTableName =
        isPostgreSql ? "\"" + spannerTable.name() + "\"" : "`" + spannerTable.name() + "`";
    String sqlStmt = selectPrefix + " FROM " + quotedTableName;
    if (this.filters.length > 0) {
      sqlStmt +=
          " WHERE "
              + SparkFilterUtils.getCompiledFilter(
                  true, Optional.empty(), isPostgreSql, fields, this.filters);
    }
    return Statement.of(sqlStmt);
  }

  public Statement buildStatement() {
    return buildLegacySql();
  }
}
