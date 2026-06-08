package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FilterToExprConverter {

  private static final Logger logger = LoggerFactory.getLogger(FilterToExprConverter.class);

  private static BoolExpr translateFilter(Filter filter, StructType schema) {

    if (filter instanceof EqualTo) {
      logger.info("Filter EqualTo expression");
      EqualTo eq = (EqualTo) filter;
      return new EqExpr(
          column((String) eq.attribute(), schema),
          literal(eq.value(), schema, (String) eq.attribute()));
    }

    if (filter instanceof GreaterThan) {
      logger.info("Filter GreaterThan expression");
      GreaterThan gt = (GreaterThan) filter;
      return new GtExpr(
          column((String) gt.attribute(), schema),
          literal(gt.value(), schema, (String) gt.attribute()));
    }

    if (filter instanceof LessThan) {
      logger.info("Filter LessThan expression");
      LessThan lt = (LessThan) filter;
      return new LtExpr(
          column((String) lt.attribute(), schema),
          literal(lt.value(), schema, (String) lt.attribute()));
    }

    if (filter instanceof And) {
      logger.info("Filter And expression");
      And and = (And) filter;
      return new AndExpr(translateFilter(and.left(), schema), translateFilter(and.right(), schema));
    }

    if (filter instanceof Or) {
      logger.info("Filter Or expression");
      Or or = (Or) filter;
      return new OrExpr(translateFilter(or.left(), schema), translateFilter(or.right(), schema));
    }

    if (filter instanceof IsNotNull) {
      logger.info("Filter IsNotNull expression");
      IsNotNull isNotNull = (IsNotNull) filter;

      return new IsNotNullExpr(column(isNotNull.attribute(), schema));
    }

    if (filter instanceof In) {
      logger.info("Filter In expression");
      In in = (In) filter;
      List<LiteralExpr> values = new ArrayList<>();

      for (Object value : in.values()) {
        values.add(literal(value, schema, (String) in.attribute()));
      }
      return new InExpr(column((String) in.attribute(), schema), values);
    }

    if (filter instanceof Not) {
      logger.info("Filter Not expression");
      Not not = (Not) filter;

      return new NotExpr(translateFilter(not.child(), schema));
    }

    logger.error("Unsupported filter: {}", filter);
    throw new UnsupportedOperationException("Unsupported filter: " + filter);
  }

  public static Optional<BoolExpr> translateFilters(Filter[] filters, StructType schema) {

    if (filters == null || filters.length == 0) {
      return Optional.empty();
    }

    BoolExpr result = translateFilter(filters[0], schema);

    for (int i = 1; i < filters.length; i++) {
      result = new AndExpr((BoolExpr) result, (BoolExpr) translateFilter(filters[i], schema));
    }

    return Optional.of(result);
  }

  private static ColumnExpr column(String name, StructType schema) {
    logger.debug("Looking up column '{}' in schema {}", name, schema.treeString());

    StructField field = schema.apply(name);

    return new ColumnExpr(name, field.dataType(), field.nullable());
  }

  private static LiteralExpr literal(Object value, StructType schema, String columnName) {
    logger.debug("Looking up literal column '{}' in schema {}", columnName, schema.treeString());

    StructField field = schema.apply(columnName);

    return new LiteralExpr(value, field.dataType());
  }
}
