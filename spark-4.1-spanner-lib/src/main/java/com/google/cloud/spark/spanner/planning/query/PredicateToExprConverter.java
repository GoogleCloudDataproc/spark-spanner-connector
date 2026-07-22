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

import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.*;
import java.util.function.BiFunction;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PredicateToExprConverter {

  private static final Logger logger = LoggerFactory.getLogger(PredicateToExprConverter.class);

  private static final Map<String, BiFunction<Predicate, StructType, BoolExpr>> CONVERTERS =
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

  public static BoolExpr translatePredicate(Predicate predicate, StructType schema) {

    var converter = CONVERTERS.get(predicate.name());

    if (converter == null) {
      throw new UnsupportedOperationException(predicate.name());
    }

    return converter.apply(predicate, schema);
  }

  private static BoolExpr equal(Predicate predicate, StructType schema) {
    return binary(predicate, schema, EqExpr::new);
  }

  private static BoolExpr equalNullSafe(Predicate predicate, StructType schema) {
    return binary(predicate, schema, EqNullSafeExpr::new);
  }

  private static BoolExpr greaterThan(Predicate predicate, StructType schema) {
    return binary(predicate, schema, GtExpr::new);
  }

  private static BoolExpr lessThan(Predicate predicate, StructType schema) {
    return binary(predicate, schema, LtExpr::new);
  }

  private static BoolExpr greaterThanOrEqual(Predicate predicate, StructType schema) {
    return binary(predicate, schema, GteExpr::new);
  }

  private static BoolExpr lessThanOrEqual(Predicate predicate, StructType schema) {
    return binary(predicate, schema, LteExpr::new);
  }

  private static BoolExpr and(Predicate predicate, StructType schema) {
    return new AndExpr(
        translatePredicate((Predicate) predicate.children()[0], schema),
        translatePredicate((Predicate) predicate.children()[1], schema));
  }

  private static BoolExpr or(Predicate predicate, StructType schema) {
    return new OrExpr(
        translatePredicate((Predicate) predicate.children()[0], schema),
        translatePredicate((Predicate) predicate.children()[1], schema));
  }

  private static BoolExpr not(Predicate predicate, StructType schema) {
    return new NotExpr(translatePredicate((Predicate) predicate.children()[0], schema));
  }

  private static BoolExpr in(Predicate predicate, StructType schema) {
    return translateIn(predicate, schema);
  }

  private static BoolExpr isNull(Predicate predicate, StructType schema) {
    return new IsNullExpr(translateExpression(predicate.children()[0], schema));
  }

  private static BoolExpr isNotNull(Predicate predicate, StructType schema) {
    return new IsNotNullExpr(translateExpression(predicate.children()[0], schema));
  }

  private static BoolExpr startsWith(Predicate predicate, StructType schema) {
    return binary(predicate, schema, StartsWithExpr::new);
  }

  private static BoolExpr endsWith(Predicate predicate, StructType schema) {
    return binary(predicate, schema, EndsWithExpr::new);
  }

  private static BoolExpr contains(Predicate predicate, StructType schema) {
    return binary(predicate, schema, ContainsExpr::new);
  }

  private static BoolExpr translateIn(Predicate predicate, StructType schema) {
    if (predicate.children().length == 0) {
      throw new IllegalArgumentException("IN predicate must have at least 1 child");
    }
    ValueExpr left = translateExpression(predicate.children()[0], schema);
    if (!(left instanceof ColumnExpr)) {
      throw new UnsupportedOperationException(
          "Left side of IN predicate must be a column reference");
    }

    List<ValueExpr> values = new ArrayList<>();

    for (int i = 1; i < predicate.children().length; i++) {
      values.add(translateExpression(predicate.children()[i], schema));
    }

    return new InExpr(left, values);
  }

  private static ValueExpr translateExpression(Expression expression, StructType schema) {

    if (expression instanceof NamedReference) {
      return translateExpression((NamedReference) expression, schema);
    }

    if (expression instanceof Literal<?>) {
      return translateExpression((Literal<?>) expression, schema);
    }

    if (expression instanceof GeneralScalarExpression) {
      return translateExpression((GeneralScalarExpression) expression, schema);
    }

    throw new UnsupportedOperationException(
        "Unsupported expression: " + expression.getClass().getName());
  }

  private static ColumnExpr translateExpression(NamedReference reference, StructType schema) {

    return ExprConverterUtils.toColumn(reference.fieldNames()[0], schema);
  }

  private static LiteralExpr translateExpression(
      Literal<?> literal, NamedReference reference, StructType schema) {

    return ExprConverterUtils.toLiteral(literal.value(), schema, reference.fieldNames()[0]);
  }

  private static ValueExpr translateExpression(
      GeneralScalarExpression expression, StructType schema) {

    Expression[] children = expression.children();

    if (children.length == 2) {
      return new ArithmeticExpr(
          translateExpression(children[0], schema),
          toArithmeticOperator(expression.name()),
          translateExpression(children[1], schema));
    } else if (children.length == 1) {
      return new UnaryExpr(
          toUnaryOperator(expression.name()), translateExpression(children[0], schema));
    }
    throw new UnsupportedOperationException(
        "Expression does not have 1 or 2 arguments. Actual: " + children.length);
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
        throw new UnsupportedOperationException("Unsupported arithmetic operator: " + name);
    }
  }

  private static BoolExpr binary(
      Predicate predicate, StructType schema, BiFunction<ValueExpr, ValueExpr, BoolExpr> factory) {

    if (predicate.children().length < 2) {
      throw new IllegalArgumentException("Binary predicate must have at least 2 children");
    }
    ValueExpr left = translateExpression((Expression) predicate.children()[0], schema);

    ValueExpr right = translateExpression((Expression) predicate.children()[1], schema);

    return factory.apply(left, right);
  }
}
