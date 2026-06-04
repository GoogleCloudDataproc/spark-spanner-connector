package com.google.cloud.spark.spanner.rendering;

import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.expression.SqlExprVisitor;
import com.google.cloud.spark.spanner.planning.query.FilterToExprConverter;
import com.google.cloud.spark.spanner.planning.query.LogicalQuery;
import com.google.cloud.spark.spanner.planning.relation.Relation;
import com.google.cloud.spark.spanner.planning.relation.SqlRelationVisitor;
import com.google.cloud.spark.spanner.planning.relation.TableRelation;
import com.google.cloud.spark.spanner.scan.SpannerScanner;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class SpannerQueryBuilder {
  private LogicalQuery logicalQuery;
  private Filter[] filters;
  private StructType schema;

  private SpannerQueryBuilder(LogicalQuery logicalQuery, Filter[] filters, StructType schema) {
    this.logicalQuery = logicalQuery;
    this.filters = filters;
    this.schema = schema;
  }

  public static SpannerQueryBuilder newBuilder(
      LogicalQuery logicalQuery, Filter[] filters, StructType schema) {
    return new SpannerQueryBuilder(logicalQuery, filters, schema);
  }

  public String buildSql() {
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
              alias, new LinkedHashSet(this.logicalQuery.getProjections()), false);
      selectPrefix = "SELECT " + columnsWithTablePrefix;
    }

    SqlRelationVisitor relationVisitor = new SqlRelationVisitor();
    String query =
        "SELECT " + selectPrefix + " FROM " + logicalQuery.getSource().accept(relationVisitor);
    Optional<BoolExpr> exprOptional =
        FilterToExprConverter.translateFilters(this.filters, this.schema);
    if (exprOptional.isPresent()) {
      BoolExpr expr = exprOptional.get();
      SqlExprVisitor exprVisitor = new SqlExprVisitor();
      RenderResult result = expr.accept(exprVisitor);
      query += " WHERE " + result.getSql();
    }

    return query;
  }

  public RenderResult build() {
    return new RenderResult(buildSql(), Collections.emptyMap());
  }
}
