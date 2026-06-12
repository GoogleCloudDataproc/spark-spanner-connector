package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.scan.SpannerScanner;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(SpannerQueryBuilder.class);

  private final LogicalQuery logicalQuery;
  private final StructType schema;
  private final Dialect dialect;

  private SpannerQueryBuilder(LogicalQuery logicalQuery, StructType schema, Dialect dialect) {
    this.logicalQuery = logicalQuery;
    this.schema = schema;
    this.dialect = dialect;
  }

  public static SpannerQueryBuilder newBuilder(
      LogicalQuery logicalQuery, StructType schema, Dialect dialect) {
    return new SpannerQueryBuilder(logicalQuery, schema, dialect);
  }

  private RenderResult buildSql() {
    Relation relation = logicalQuery.getSource();
    String alias = null;
    if (relation instanceof TableRelation) {
      TableRelation tableRelation = (TableRelation) relation;
      alias = tableRelation.getAlias();
    }

    // 1. Use * if no requiredColumns were requested else select them.
    String selectPrefix = "SELECT *";
    if (this.logicalQuery.getProjections() != null
        && this.logicalQuery.getProjections().size() > 0) {
      // Prefix each column with the table name to avoid ambiguity when column name
      // matches table name
      String columnsWithTablePrefix =
          SpannerScanner.buildColumnsWithTablePrefix(
              alias,
              new LinkedHashSet(this.logicalQuery.getProjections()),
              dialect.equals(Dialect.POSTGRESQL));
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    SqlRelationVisitor relationVisitor = new SqlRelationVisitor(this.dialect);
    String query =
        selectPrefix + " FROM " + logicalQuery.getSource().accept(relationVisitor).getSql();

    Optional<RenderResult> filterResult = renderFilters();
    Map<String, LiteralExpr> bindings = new HashMap<>();
    if (filterResult.isPresent()) {
      RenderResult result = filterResult.get();
      query += " WHERE " + parenthesize(result.getSql());
      bindings.putAll(result.getBindings());
    }
    logger.debug("query: {}", query);
    for (Map.Entry<String, LiteralExpr> entry : bindings.entrySet()) {
      logger.debug(
          "bindings: Key: {}, Value type: {}: {}",
          entry.getKey(),
          entry.getValue().getSparkType().toString(),
          entry.getValue().getValue().toString());
    }
    return new RenderResult(query, bindings);
  }

  private Optional<RenderResult> renderFilters() {
    Optional<BoolExpr> exprOptional = this.logicalQuery.getFilter();
    if (exprOptional.isPresent()) {
      BoolExpr expr = exprOptional.get();
      SqlExprVisitor exprVisitor = SqlExprVisitor.create(dialect);
      RenderResult result = expr.accept(exprVisitor);
      return Optional.of(result);
    } else {
      return Optional.empty();
    }
  }

  public Statement buildStatement() {
    RenderResult renderResult = this.buildSql();

    Statement.Builder builder = Statement.newBuilder(renderResult.getSql());
    Map<String, LiteralExpr> bindings = renderResult.getBindings();
    if (bindings != null) {
      for (Map.Entry<String, LiteralExpr> entry : bindings.entrySet()) {
        SpannerTypeBinder.bind(builder, entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private String parenthesize(String in) {
    return "(" + in + ")";
  }
}
