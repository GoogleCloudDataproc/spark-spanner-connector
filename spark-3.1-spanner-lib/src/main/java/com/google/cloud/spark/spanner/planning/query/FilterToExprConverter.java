package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.planning.expression.*;
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
      EqualTo eq = (EqualTo) filter;
      return new EqExpr(
          column((String) eq.attribute(), schema),
          literal(eq.value(), schema, (String) eq.attribute()));
    }

    if (filter instanceof GreaterThan) {
      GreaterThan gt = (GreaterThan) filter;
      return new GtExpr(
          column((String) gt.attribute(), schema),
          literal(gt.value(), schema, (String) gt.attribute()));
    }

    if (filter instanceof LessThan) {
      LessThan lt = (LessThan) filter;
      return new LtExpr(
          column((String) lt.attribute(), schema),
          literal(lt.value(), schema, (String) lt.attribute()));
    }

    if (filter instanceof And) {
      And and = (And) filter;
      return new AndExpr(translateFilter(and.left(), schema), translateFilter(and.right(), schema));
    }

    if (filter instanceof Or) {
      Or or = (Or) filter;
      return new OrExpr(translateFilter(or.left(), schema), translateFilter(or.right(), schema));
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
    StructField field = schema.apply(name);

    return new ColumnExpr(name, field.dataType(), field.nullable());
  }

  private static LiteralExpr literal(Object value, StructType schema, String columnName) {
    StructField field = schema.apply(columnName);

    return new LiteralExpr(value, field.dataType());
  }
}
