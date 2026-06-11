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
      logger.debug("Filter EqualTo expression");
      EqualTo eq = (EqualTo) filter;
      return new EqExpr(
          column((String) eq.attribute(), schema),
          literal(eq.value(), schema, (String) eq.attribute()));
    }

    if (filter instanceof GreaterThan) {
      logger.debug("Filter GreaterThan expression");
      GreaterThan gt = (GreaterThan) filter;
      return new GtExpr(
          column((String) gt.attribute(), schema),
          literal(gt.value(), schema, (String) gt.attribute()));
    }

    if (filter instanceof GreaterThanOrEqual) {
      logger.debug("Filter GreaterThanOrEqual expression");
      GreaterThanOrEqual gte = (GreaterThanOrEqual) filter;
      return new GteExpr(
          column((String) gte.attribute(), schema),
          literal(gte.value(), schema, (String) gte.attribute()));
    }

    if (filter instanceof LessThan) {
      logger.debug("Filter LessThan expression");
      LessThan lt = (LessThan) filter;
      return new LtExpr(
          column((String) lt.attribute(), schema),
          literal(lt.value(), schema, (String) lt.attribute()));
    }

    if (filter instanceof LessThanOrEqual) {
      logger.debug("Filter LessThanOrEqual expression");
      LessThanOrEqual lte = (LessThanOrEqual) filter;
      return new LteExpr(
          column((String) lte.attribute(), schema),
          literal(lte.value(), schema, (String) lte.attribute()));
    }

    if (filter instanceof And) {
      logger.debug("Filter And expression");
      And and = (And) filter;
      return new AndExpr(translateFilter(and.left(), schema), translateFilter(and.right(), schema));
    }

    if (filter instanceof Or) {
      logger.debug("Filter Or expression");
      Or or = (Or) filter;
      return new OrExpr(translateFilter(or.left(), schema), translateFilter(or.right(), schema));
    }

    if (filter instanceof IsNull) {
      logger.debug("Filter IsNull expression");
      IsNull isNull = (IsNull) filter;

      return new IsNullExpr(column(isNull.attribute(), schema));
    }

    if (filter instanceof IsNotNull) {
      logger.debug("Filter IsNotNull expression");
      IsNotNull isNotNull = (IsNotNull) filter;

      return new IsNotNullExpr(column(isNotNull.attribute(), schema));
    }

    if (filter instanceof In) {
      logger.debug("Filter In expression");
      In in = (In) filter;
      List<LiteralExpr> values = new ArrayList<>();

      for (Object value : in.values()) {
        values.add(literal(value, schema, (String) in.attribute()));
      }
      return new InExpr(column((String) in.attribute(), schema), values);
    }

    if (filter instanceof Not) {
      logger.debug("Filter Not expression");
      Not not = (Not) filter;

      return new NotExpr(translateFilter(not.child(), schema));
    }

    if (filter instanceof StringStartsWith) {
      StringStartsWith startsWith = (StringStartsWith) filter;

      return new StartsWithExpr(
          column(startsWith.attribute(), schema),
          literal(startsWith.value(), schema, startsWith.attribute()));
    }

    if (filter instanceof StringEndsWith) {
      StringEndsWith endsWith = (StringEndsWith) filter;

      return new EndsWithExpr(
          column(endsWith.attribute(), schema),
          literal(endsWith.value(), schema, endsWith.attribute()));
    }

    if (filter instanceof StringContains) {
      StringContains contains = (StringContains) filter;

      return new ContainsExpr(
          column(contains.attribute(), schema),
          literal(contains.value(), schema, contains.attribute()));
    }

    // TODO: should SQL be passed through as is instead of throwing an exception?
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
