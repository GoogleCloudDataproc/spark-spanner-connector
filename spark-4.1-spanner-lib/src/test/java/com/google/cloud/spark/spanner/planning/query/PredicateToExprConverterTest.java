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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spark.spanner.planning.expression.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class PredicateToExprConverterTest {
  private static final String UUID1 = "550e8400-e29b-41d4-a716-446655440000";

  private static final String UUID2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

  private static Map<String, ColumnResolution> resolutionMap() {
    return Map.of(
        "uuid_col",
        new ColumnResolution("uuid_col", "uuid_col", "", DataTypes.StringType, "UUID", false),
        "string_col",
        new ColumnResolution("string_col", "string_col", "", DataTypes.StringType, "STRING", true));
  }

  private static NamedReference uuidColumn() {
    return Expressions.column("uuid_col");
  }

  private static NamedReference stringColumn() {
    return Expressions.column("string_col");
  }

  private static Literal<String> stringLiteral(String value) {
    return Expressions.literal(value);
  }

  private static Predicate equal(Expression left, Expression right) {
    return new Predicate("=", new Expression[] {left, right});
  }

  private static Predicate in(Expression column, Expression... values) {
    Expression[] children = new Expression[values.length + 1];
    children[0] = column;
    System.arraycopy(values, 0, children, 1, values.length);

    return new Predicate("IN", children);
  }

  private static Predicate and(Predicate left, Predicate right) {
    return new Predicate("AND", new Expression[] {left, right});
  }

  @Test
  public void equalProducesEqExpr() {

    Map<String, ColumnResolution> resolutionMap = new HashMap<>();
    resolutionMap.put(
        "A", new ColumnResolution("A", "A", "ATable", DataTypes.IntegerType, "INT64", true));
    NamedReference ref = mock(NamedReference.class);
    when(ref.fieldNames()).thenReturn(new String[] {"A"});

    Literal<Integer> lit = mock(Literal.class);
    when(lit.value()).thenReturn(1);

    Predicate predicate = mock(Predicate.class);
    when(predicate.name()).thenReturn("=");
    when(predicate.children()).thenReturn(new Expression[] {ref, lit});

    BoolExpr expr = PredicateToExprConverter.translatePredicate(predicate, resolutionMap);

    assertThat(expr).isInstanceOf(EqExpr.class);

    EqExpr eq = (EqExpr) expr;

    ValueExpr leftExpr = eq.getLeft();
    assertTrue(leftExpr instanceof ColumnExpr);
    assertEquals("A", ((ColumnExpr) leftExpr).getColumnName());
    assertEquals("ATable", ((ColumnExpr) leftExpr).getTableAlias());

    ValueExpr rightExpr = eq.getRight();
    assertTrue(rightExpr instanceof LiteralExpr);
    LiteralExpr literalExpr = (LiteralExpr) rightExpr;
    assertEquals(DataTypes.IntegerType, literalExpr.getSparkType());
    assertEquals(Integer.valueOf("1"), (Integer) literalExpr.getValue());
  }

  @Test
  public void uuidColumnEqualsLiteralGetsUuidTargetType() {
    Predicate predicate = equal(uuidColumn(), stringLiteral(UUID1));

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof EqExpr);

    EqExpr eq = (EqExpr) result;

    assertTrue(eq.getLeft() instanceof ColumnExpr);
    assertTrue(eq.getRight() instanceof LiteralExpr);

    ColumnExpr column = (ColumnExpr) eq.getLeft();
    LiteralExpr literal = (LiteralExpr) eq.getRight();

    assertEquals("uuid_col", column.getColumnName());
    assertEquals(UUID1, literal.getValue());
    assertEquals(DataTypes.StringType, literal.getSparkType());
    assertEquals("UUID", literal.getSpannerType());
  }

  @Test
  public void literalEqualsUuidColumnGetsUuidTargetType() {
    Predicate predicate = equal(uuidColumn(), stringLiteral(UUID1));

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof EqExpr);

    EqExpr eq = (EqExpr) result;

    assertTrue(eq.getLeft() instanceof ColumnExpr);
    assertTrue(eq.getRight() instanceof LiteralExpr);

    ColumnExpr column = (ColumnExpr) eq.getLeft();
    LiteralExpr literal = (LiteralExpr) eq.getRight();

    assertEquals(UUID1, literal.getValue());
    assertEquals(DataTypes.StringType, literal.getSparkType());
    assertEquals("UUID", literal.getSpannerType());

    assertEquals("uuid_col", column.getColumnName());
  }

  @Test
  public void uuidColumnInGetsUuidTargetTypeForAllLiterals() {
    Predicate predicate = in(uuidColumn(), stringLiteral(UUID1), stringLiteral(UUID2));

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof InExpr);

    InExpr in = (InExpr) result;

    assertTrue(in.getLeft() instanceof ColumnExpr);
    assertEquals(2, in.getValues().size());

    LiteralExpr first = (LiteralExpr) in.getValues().get(0);
    LiteralExpr second = (LiteralExpr) in.getValues().get(1);

    assertEquals(UUID1, first.getValue());
    assertEquals(DataTypes.StringType, first.getSparkType());
    assertEquals("UUID", first.getSpannerType());

    assertEquals(UUID2, second.getValue());
    assertEquals(DataTypes.StringType, second.getSparkType());
    assertEquals("UUID", second.getSpannerType());
  }

  @Test
  public void stringColumnInKeepsUuidLookingValuesAsStrings() {
    Predicate predicate = in(uuidColumn(), stringLiteral(UUID1), stringLiteral(UUID2));

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof InExpr);

    InExpr in = (InExpr) result;

    assertEquals(2, in.getValues().size());

    LiteralExpr first = (LiteralExpr) in.getValues().get(0);
    LiteralExpr second = (LiteralExpr) in.getValues().get(1);

    assertEquals(UUID1, first.getValue());
    assertEquals(DataTypes.StringType, first.getSparkType());
    assertEquals("UUID", first.getSpannerType());

    assertEquals(UUID2, second.getValue());
    assertEquals(DataTypes.StringType, second.getSparkType());
    assertEquals("UUID", second.getSpannerType());
  }

  @Test
  public void andResolvesEachLiteralAgainstItsOwnColumn() {
    Predicate uuidPredicate = equal(uuidColumn(), stringLiteral(UUID1));

    Predicate stringPredicate = equal(stringColumn(), stringLiteral(UUID1));

    Predicate predicate = and(uuidPredicate, stringPredicate);

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof AndExpr);

    AndExpr and = (AndExpr) result;

    assertTrue(and.getLeft() instanceof EqExpr);
    assertTrue(and.getRight() instanceof EqExpr);

    EqExpr uuidEq = (EqExpr) and.getLeft();
    EqExpr stringEq = (EqExpr) and.getRight();

    LiteralExpr uuidLiteral = (LiteralExpr) uuidEq.getRight();

    LiteralExpr stringLiteral = (LiteralExpr) stringEq.getRight();

    assertEquals(UUID1, uuidLiteral.getValue());
    assertEquals(DataTypes.StringType, uuidLiteral.getSparkType());
    assertEquals("UUID", uuidLiteral.getSpannerType());

    assertEquals(UUID1, stringLiteral.getValue());
    assertEquals(DataTypes.StringType, stringLiteral.getSparkType());
    assertEquals("STRING", stringLiteral.getSpannerType());
  }
}
