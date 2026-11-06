// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.AndExpr;
import com.google.cloud.spark.spanner.planning.expression.ArithmeticExpr;
import com.google.cloud.spark.spanner.planning.expression.BoolExpr;
import com.google.cloud.spark.spanner.planning.expression.ColumnExpr;
import com.google.cloud.spark.spanner.planning.expression.ContainsExpr;
import com.google.cloud.spark.spanner.planning.expression.EndsWithExpr;
import com.google.cloud.spark.spanner.planning.expression.EqExpr;
import com.google.cloud.spark.spanner.planning.expression.EqNullSafeExpr;
import com.google.cloud.spark.spanner.planning.expression.FunctionExpr;
import com.google.cloud.spark.spanner.planning.expression.GtExpr;
import com.google.cloud.spark.spanner.planning.expression.GteExpr;
import com.google.cloud.spark.spanner.planning.expression.InExpr;
import com.google.cloud.spark.spanner.planning.expression.IsNotNullExpr;
import com.google.cloud.spark.spanner.planning.expression.IsNullExpr;
import com.google.cloud.spark.spanner.planning.expression.LiteralExpr;
import com.google.cloud.spark.spanner.planning.expression.LtExpr;
import com.google.cloud.spark.spanner.planning.expression.LteExpr;
import com.google.cloud.spark.spanner.planning.expression.NotExpr;
import com.google.cloud.spark.spanner.planning.expression.OrExpr;
import com.google.cloud.spark.spanner.planning.expression.StartsWithExpr;
import com.google.cloud.spark.spanner.planning.expression.UnaryExpr;
import com.google.cloud.spark.spanner.planning.expression.ValueExpr;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PredicateToExprConverter {

  private static final Logger logger = LoggerFactory.getLogger(PredicateToExprConverter.class);

  @FunctionalInterface
  private interface PredicateConverter {
    BoolExpr convert(
        Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql);
  }

  private static final Map<String, PredicateConverter> CONVERTERS =
      Map.ofEntries(
          Map.entry("=", PredicateToExprConverter::equal),
          Map.entry("<=>", PredicateToExprConverter::equalNullSafe),
          Map.entry(">", PredicateToExprConverter::greaterThan),
          Map.entry(">=", PredicateToExprConverter::greaterThanOrEqual),
          Map.entry("<", PredicateToExprConverter::lessThan),
          Map.entry("<=", PredicateToExprConverter::lessThanOrEqual),
          Map.entry("AND", PredicateToExprConverter::and),
          Map.entry("OR", PredicateToExprConverter::or),
          Map.entry("NOT", PredicateToExprConverter::not),
          Map.entry("IN", PredicateToExprConverter::in),
          Map.entry("IS_NULL", PredicateToExprConverter::isNull),
          Map.entry("IS_NOT_NULL", PredicateToExprConverter::isNotNull),
          Map.entry("STARTS_WITH", PredicateToExprConverter::startsWith),
          Map.entry("ENDS_WITH", PredicateToExprConverter::endsWith),
          Map.entry("CONTAINS", PredicateToExprConverter::contains));

  public static BoolExpr translatePredicate(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap) {
    return translatePredicate(predicate, resolutionMap, false);
  }

  public static BoolExpr translatePredicate(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    logger.info("Converting predicate {} with {}", predicate, resolutionMap);
    logger.info(expressionToString(predicate));

    var converter = CONVERTERS.get(predicate.name());

    if (converter == null) {
      throw new UnsupportedOperationException(predicate.name());
    }

    return converter.convert(predicate, resolutionMap, isPostgreSql);
  }

  private static String expressionToString(Expression expr) {
    if (expr instanceof NamedReference) {
      NamedReference ref = (NamedReference) expr;
      return String.join(".", ref.fieldNames());
    }

    if (expr instanceof Literal<?>) {
      Literal<?> lit = (Literal<?>) expr;
      return String.valueOf(lit.value());
    }

    if (expr instanceof Predicate) {
      Predicate pred = (Predicate) expr;
      return pred.name()
          + "("
          + Arrays.stream(pred.children())
              .map(PredicateToExprConverter::expressionToString)
              .collect(Collectors.joining(", "))
          + ")";
    }

    return expr.getClass().getSimpleName();
  }

  private static BoolExpr equal(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, EqExpr::new);
  }

  private static BoolExpr equalNullSafe(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, EqNullSafeExpr::new);
  }

  private static BoolExpr greaterThan(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, GtExpr::new);
  }

  private static BoolExpr lessThan(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, LtExpr::new);
  }

  private static BoolExpr greaterThanOrEqual(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, GteExpr::new);
  }

  private static BoolExpr lessThanOrEqual(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return binary(predicate, resolutionMap, isPostgreSql, LteExpr::new);
  }

  private static BoolExpr and(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return new AndExpr(
        translatePredicate((Predicate) predicate.children()[0], resolutionMap, isPostgreSql),
        translatePredicate((Predicate) predicate.children()[1], resolutionMap, isPostgreSql));
  }

  private static BoolExpr or(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return new OrExpr(
        translatePredicate((Predicate) predicate.children()[0], resolutionMap, isPostgreSql),
        translatePredicate((Predicate) predicate.children()[1], resolutionMap, isPostgreSql));
  }

  private static BoolExpr not(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return new NotExpr(
        translatePredicate((Predicate) predicate.children()[0], resolutionMap, isPostgreSql));
  }

  private static BoolExpr in(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return translateIn(predicate, resolutionMap, isPostgreSql);
  }

  private static BoolExpr isNull(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return new IsNullExpr(
        translateExpression(predicate.children()[0], resolutionMap, isPostgreSql));
  }

  private static BoolExpr isNotNull(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    return new IsNotNullExpr(
        translateExpression(predicate.children()[0], resolutionMap, isPostgreSql));
  }

  private static BoolExpr startsWith(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    rejectUuidColumnForStringPattern(predicate, resolutionMap);
    return binary(predicate, resolutionMap, isPostgreSql, StartsWithExpr::new);
  }

  private static BoolExpr endsWith(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    rejectUuidColumnForStringPattern(predicate, resolutionMap);
    return binary(predicate, resolutionMap, isPostgreSql, EndsWithExpr::new);
  }

  private static BoolExpr contains(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    rejectUuidColumnForStringPattern(predicate, resolutionMap);
    return binary(predicate, resolutionMap, isPostgreSql, ContainsExpr::new);
  }

  private static String extractSingleFieldName(NamedReference reference) {
    String[] fieldNames = reference.fieldNames();
    if (fieldNames == null || fieldNames.length != 1) {
      throw new UnsupportedOperationException(
          "Nested or empty field references are not supported in predicate pushdown: "
              + Arrays.toString(fieldNames));
    }
    return fieldNames[0];
  }

  private static void rejectUuidColumnForStringPattern(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap) {
    for (Expression child : predicate.children()) {
      if (child instanceof NamedReference) {
        String fieldName = extractSingleFieldName((NamedReference) child);
        ColumnResolution resolution = resolutionMap == null ? null : resolutionMap.get(fieldName);
        if (resolution != null && ColumnResolution.isUuid(resolution.getSpannerType())) {
          throw new UnsupportedOperationException(
              "String pattern predicate " + predicate.name() + " is not supported on UUID column");
        }
      }
    }
  }

  private static BoolExpr translateIn(
      Predicate predicate, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {
    if (predicate.children().length == 0) {
      throw new IllegalArgumentException("IN predicate must have at least 1 child");
    }

    Expression leftChild = predicate.children()[0];
    ValueExpr left = translateExpression(leftChild, resolutionMap, isPostgreSql);
    if (!(left instanceof ColumnExpr) || !(leftChild instanceof NamedReference)) {
      throw new UnsupportedOperationException(
          "Left side of IN predicate must be a column reference");
    }

    String referenceName = extractSingleFieldName((NamedReference) leftChild);
    ColumnResolution resolution = resolutionMap == null ? null : resolutionMap.get(referenceName);
    if (resolution == null) {
      throw new IllegalArgumentException("No column resolution found for column: " + referenceName);
    }

    List<ValueExpr> values = new ArrayList<>();
    for (int i = 1; i < predicate.children().length; i++) {
      Expression value = predicate.children()[i];
      if (value instanceof Literal<?>) {
        values.add(
            ExprConverterUtils.toLiteral(((Literal<?>) value).value(), resolution, isPostgreSql));
      } else {
        values.add(translateExpression(value, resolutionMap, isPostgreSql));
      }
    }

    return new InExpr(left, values);
  }

  private static ValueExpr translateExpression(
      Expression expression, Map<String, ColumnResolution> resolutionMap, boolean isPostgreSql) {

    if (expression instanceof NamedReference) {
      return translateExpression((NamedReference) expression, resolutionMap);
    }

    if (expression instanceof GeneralScalarExpression) {
      return translateExpression((GeneralScalarExpression) expression, resolutionMap, isPostgreSql);
    }

    if (expression instanceof Literal<?>) {
      Literal<?> literal = (Literal<?>) expression;
      return translateLiteral(literal, isPostgreSql);
    }

    throw new UnsupportedOperationException(
        "Unsupported expression: " + expression.getClass().getName());
  }

  private static LiteralExpr translateLiteral(Literal<?> literal, boolean isPostgreSql) {
    Object value = ExprConverterUtils.normalizeLiteral(literal.value(), literal.dataType());
    if (!isPostgreSql
        && value instanceof BigDecimal
        && !SpannerTypeBinder.isValidGoogleSqlNumeric((BigDecimal) value)) {
      throw new IllegalArgumentException("Decimal out of Spanner NUMERIC range: " + value);
    }

    return new LiteralExpr(value, literal.dataType());
  }

  private static ColumnExpr translateExpression(
      NamedReference reference, Map<String, ColumnResolution> resolutionMap) {
    logger.info("translateExpression field names: {}", Arrays.toString(reference.fieldNames()));
    return ExprConverterUtils.toColumn(extractSingleFieldName(reference), resolutionMap);
  }

  private static LiteralExpr translateExpression(
      Literal<?> literal,
      NamedReference reference,
      Map<String, ColumnResolution> resolutionMap,
      boolean isPostgreSql) {

    return ExprConverterUtils.toLiteral(
        literal.value(), resolutionMap, extractSingleFieldName(reference), isPostgreSql);
  }

  private static ValueExpr translateExpression(
      GeneralScalarExpression expression,
      Map<String, ColumnResolution> resolutionMap,
      boolean isPostgreSql) {

    if (isFunction(expression.name())) {
      return translateFunction(expression, resolutionMap, isPostgreSql);
    }

    Expression[] children = expression.children();

    if (children.length == 2) {
      return new ArithmeticExpr(
          translateExpression(children[0], resolutionMap, isPostgreSql),
          toArithmeticOperator(expression.name()),
          translateExpression(children[1], resolutionMap, isPostgreSql));
    }

    if (children.length == 1) {
      return new UnaryExpr(
          toUnaryOperator(expression.name()),
          translateExpression(children[0], resolutionMap, isPostgreSql));
    }

    throw new UnsupportedOperationException(
        "Expression does not have 1 or 2 arguments. Actual: " + children.length);
  }

  private static boolean isFunction(String name) {
    try {
      FunctionExpr.Function.valueOf(name.toUpperCase(Locale.ROOT));
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static ValueExpr translateFunction(
      GeneralScalarExpression expression,
      Map<String, ColumnResolution> resolutionMap,
      boolean isPostgreSql) {

    FunctionExpr.Function function =
        FunctionExpr.Function.valueOf(expression.name().toUpperCase(Locale.ROOT));

    List<ValueExpr> arguments =
        Arrays.stream(expression.children())
            .map(child -> translateExpression(child, resolutionMap, isPostgreSql))
            .collect(Collectors.toList());

    return new FunctionExpr(function, arguments);
  }

  // Translates Spark predicate operator representation to this connector's internal representation.
  private static ArithmeticExpr.Operator toArithmeticOperator(String name) {
    switch (name) {
      case "+":
        return ArithmeticExpr.Operator.ADD;
      case "-":
        return ArithmeticExpr.Operator.SUBTRACT;
      case "*":
        return ArithmeticExpr.Operator.MULTIPLY;
      case "/":
        return ArithmeticExpr.Operator.DIVIDE;
      case "%":
        return ArithmeticExpr.Operator.MOD;
      default:
        throw new UnsupportedOperationException("Unsupported arithmetic operator: " + name);
    }
  }

  // Translates Spark predicate operator representation to this connector's internal representation.
  private static UnaryExpr.Operator toUnaryOperator(String name) {
    switch (name) {
      case "+":
        return UnaryExpr.Operator.PLUS;
      case "-":
        return UnaryExpr.Operator.NEGATE;
      default:
        throw new UnsupportedOperationException("Unsupported unary operator: " + name);
    }
  }

  private static BoolExpr binary(
      Predicate predicate,
      Map<String, ColumnResolution> resolutionMap,
      boolean isPostgreSql,
      BiFunction<ValueExpr, ValueExpr, BoolExpr> factory) {

    if (predicate.children().length != 2) {
      throw new IllegalArgumentException("Binary predicate must have exactly 2 children");
    }

    Expression leftExpression = predicate.children()[0];
    Expression rightExpression = predicate.children()[1];

    ValueExpr left =
        translateBinaryOperand(leftExpression, rightExpression, resolutionMap, isPostgreSql);

    ValueExpr right =
        translateBinaryOperand(rightExpression, leftExpression, resolutionMap, isPostgreSql);

    return factory.apply(left, right);
  }

  private static ValueExpr translateBinaryOperand(
      Expression expression,
      Expression otherExpression,
      Map<String, ColumnResolution> resolutionMap,
      boolean isPostgreSql) {

    if (expression instanceof Literal<?> && otherExpression instanceof NamedReference) {
      Literal<?> literal = (Literal<?>) expression;
      NamedReference reference = (NamedReference) otherExpression;
      return translateExpression(literal, reference, resolutionMap, isPostgreSql);
    }

    return translateExpression(expression, resolutionMap, isPostgreSql);
  }
}
