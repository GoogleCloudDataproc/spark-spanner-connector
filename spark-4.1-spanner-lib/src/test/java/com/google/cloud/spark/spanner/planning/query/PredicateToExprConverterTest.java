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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.binding.SpannerTypeBinder;
import com.google.cloud.spark.spanner.planning.expression.*;
import com.google.cloud.spark.spanner.rendering.RenderResult;
import com.google.cloud.spark.spanner.rendering.SqlExprVisitor;
import com.google.cloud.spark.spanner.scan.InterleaveTableMetadata;
import com.google.cloud.spark.spanner.scan.SpannerTable;
import com.google.cloud.spark.spanner.scan.Spark41SpannerScanBuilder;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
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
    Predicate predicate = equal(stringLiteral(UUID1), uuidColumn());

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof EqExpr);
    EqExpr eq = (EqExpr) result;
    assertTrue(eq.getLeft() instanceof LiteralExpr);
    assertTrue(eq.getRight() instanceof ColumnExpr);

    LiteralExpr literal = (LiteralExpr) eq.getLeft();
    ColumnExpr column = (ColumnExpr) eq.getRight();

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
    Predicate predicate = in(stringColumn(), stringLiteral(UUID1), stringLiteral(UUID2));

    BoolExpr result = PredicateToExprConverter.translatePredicate(predicate, resolutionMap());

    assertTrue(result instanceof InExpr);

    InExpr in = (InExpr) result;

    assertEquals(2, in.getValues().size());

    LiteralExpr first = (LiteralExpr) in.getValues().get(0);
    LiteralExpr second = (LiteralExpr) in.getValues().get(1);

    assertEquals(UUID1, first.getValue());
    assertEquals(DataTypes.StringType, first.getSparkType());
    assertEquals("STRING", first.getSpannerType());

    assertEquals(UUID2, second.getValue());
    assertEquals(DataTypes.StringType, second.getSparkType());
    assertEquals("STRING", second.getSpannerType());
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

  @Test
  public void stringPatternPredicatesAreRejectedOnUuidColumn() {
    for (String op : new String[] {"STARTS_WITH", "ENDS_WITH", "CONTAINS"}) {
      Predicate uuidPattern =
          new Predicate(op, new Expression[] {uuidColumn(), stringLiteral("550e")});
      assertThrows(
          UnsupportedOperationException.class,
          () -> PredicateToExprConverter.translatePredicate(uuidPattern, resolutionMap()));

      Predicate stringPattern =
          new Predicate(op, new Expression[] {stringColumn(), stringLiteral("550e")});
      assertThat(PredicateToExprConverter.translatePredicate(stringPattern, resolutionMap()))
          .isNotNull();
    }
  }

  @Test
  public void pushDownJoinReturnsFalseOnInvalidUuidLiteralOrPatternPredicate() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.StringType, false, Metadata.empty())
            });
    Map<String, String> spannerTypes = Map.of("id", "UUID");
    CaseInsensitiveStringMap props =
        new CaseInsensitiveStringMap(Map.of("enablePredicateSql", "true"));

    SpannerTable parentTable = mock(SpannerTable.class);
    InterleaveTableMetadata parentMeta = mock(InterleaveTableMetadata.class);
    when(parentMeta.getTableName()).thenReturn("Parent");
    when(parentMeta.getParentTable()).thenReturn(null);
    when(parentTable.name()).thenReturn("Parent");
    when(parentTable.schema()).thenReturn(schema);
    when(parentTable.properties()).thenReturn(props);
    when(parentTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(parentTable.getInterleavedTableMetadata()).thenReturn(parentMeta);

    SpannerTable childTable = mock(SpannerTable.class);
    InterleaveTableMetadata childMeta = mock(InterleaveTableMetadata.class);
    when(childMeta.getTableName()).thenReturn("Child");
    when(childMeta.getParentTable()).thenReturn("Parent");
    when(childTable.name()).thenReturn("Child");
    when(childTable.schema()).thenReturn(schema);
    when(childTable.properties()).thenReturn(props);
    when(childTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(childTable.getInterleavedTableMetadata()).thenReturn(childMeta);

    Spark41SpannerScanBuilder leftBuilder = new Spark41SpannerScanBuilder(parentTable);
    Spark41SpannerScanBuilder rightBuilder = new Spark41SpannerScanBuilder(childTable);

    ColumnWithAlias[] leftCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "p_id")};
    ColumnWithAlias[] rightCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "c_id")};

    Predicate invalidUuidPred =
        and(
            equal(Expressions.column("p_id"), Expressions.column("c_id")),
            equal(Expressions.column("p_id"), stringLiteral("not-a-uuid")));
    assertFalse(
        leftBuilder.pushDownJoin(
            rightBuilder, JoinType.INNER_JOIN, leftCols, rightCols, invalidUuidPred));

    Predicate uppercaseUuidPred =
        and(
            equal(Expressions.column("p_id"), Expressions.column("c_id")),
            equal(Expressions.column("p_id"), stringLiteral(UUID1.toUpperCase())));
    assertFalse(
        leftBuilder.pushDownJoin(
            rightBuilder, JoinType.INNER_JOIN, leftCols, rightCols, uppercaseUuidPred));
  }

  @Test
  public void timestampEpochMicrosLiteralTranslatesAndBindsInJoinPredicate() {
    Map<String, ColumnResolution> resMap =
        Map.of(
            "ts_col",
            new ColumnResolution(
                "ts_col", "ts_col", "Orders", DataTypes.TimestampType, "TIMESTAMP", false));

    long epochMicros = 1767225600123456L;
    Literal<Long> v2TimestampLiteral = LiteralValue.apply(epochMicros, DataTypes.TimestampType);
    Predicate pred =
        new Predicate(">", new Expression[] {Expressions.column("ts_col"), v2TimestampLiteral});

    BoolExpr expr = PredicateToExprConverter.translatePredicate(pred, resMap);
    RenderResult rendered = expr.accept(SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL));
    Statement.Builder builder = Statement.newBuilder("SELECT 1 WHERE " + rendered.getSql());
    for (Map.Entry<String, LiteralExpr> entry : rendered.getBindings().entrySet()) {
      SpannerTypeBinder.bind(
          builder, entry.getKey(), entry.getValue(), Dialect.GOOGLE_STANDARD_SQL);
    }
    Statement stmt = builder.build();
    assertThat(stmt.getParameters().get("p1").getTimestamp().toSqlTimestamp().toInstant())
        .isEqualTo(Instant.ofEpochSecond(1767225600L, 123_456_000L));
  }

  @Test
  public void pushDownJoinRejectsGoogleSqlDecimalOverflowButAllowsPostgres() {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField("amount", DataTypes.createDecimalType(38, 12), true, Metadata.empty())
            });
    Map<String, String> spannerTypes = Map.of("id", "INT64", "amount", "NUMERIC");
    CaseInsensitiveStringMap props =
        new CaseInsensitiveStringMap(Map.of("enablePredicateSql", "true"));

    for (boolean isPostgres : new boolean[] {false, true}) {
      SpannerTable parentTable = mock(SpannerTable.class);
      InterleaveTableMetadata parentMeta = mock(InterleaveTableMetadata.class);
      when(parentMeta.getTableName()).thenReturn("Parent");
      when(parentMeta.getParentTable()).thenReturn(null);
      when(parentTable.name()).thenReturn("Parent");
      when(parentTable.schema()).thenReturn(schema);
      when(parentTable.properties()).thenReturn(props);
      when(parentTable.getSpannerTypes()).thenReturn(spannerTypes);
      when(parentTable.getInterleavedTableMetadata()).thenReturn(parentMeta);
      when(parentTable.isPostgreSql()).thenReturn(isPostgres);

      SpannerTable childTable = mock(SpannerTable.class);
      InterleaveTableMetadata childMeta = mock(InterleaveTableMetadata.class);
      when(childMeta.getTableName()).thenReturn("Child");
      when(childMeta.getParentTable()).thenReturn("Parent");
      when(childTable.name()).thenReturn("Child");
      when(childTable.schema()).thenReturn(schema);
      when(childTable.properties()).thenReturn(props);
      when(childTable.getSpannerTypes()).thenReturn(spannerTypes);
      when(childTable.getInterleavedTableMetadata()).thenReturn(childMeta);
      when(childTable.isPostgreSql()).thenReturn(isPostgres);

      Spark41SpannerScanBuilder leftBuilder = new Spark41SpannerScanBuilder(parentTable);
      Spark41SpannerScanBuilder rightBuilder = new Spark41SpannerScanBuilder(childTable);

      ColumnWithAlias[] leftCols =
          new ColumnWithAlias[] {
            new ColumnWithAlias("id", "p_id"), new ColumnWithAlias("amount", "p_amount")
          };
      ColumnWithAlias[] rightCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "c_id")};

      Literal<BigDecimal> highScaleLiteral =
          LiteralValue.apply(new BigDecimal("0.0000000001"), DataTypes.createDecimalType(38, 12));
      Predicate directOverflowPred =
          and(
              equal(Expressions.column("p_id"), Expressions.column("c_id")),
              new Predicate(
                  ">", new Expression[] {Expressions.column("p_amount"), highScaleLiteral}));

      assertEquals(
          isPostgres,
          leftBuilder.pushDownJoin(
              rightBuilder, JoinType.INNER_JOIN, leftCols, rightCols, directOverflowPred));

      // Also test arithmetic literal without direct column pairing (translateLiteral path)
      Expression addExpr =
          new GeneralScalarExpression(
              "+", new Expression[] {Expressions.column("p_amount"), highScaleLiteral});
      Literal<BigDecimal> oneLiteral =
          LiteralValue.apply(BigDecimal.ONE, DataTypes.createDecimalType(38, 9));
      Predicate arithmeticOverflowPred =
          and(
              equal(Expressions.column("p_id"), Expressions.column("c_id")),
              new Predicate(">", new Expression[] {addExpr, oneLiteral}));

      Spark41SpannerScanBuilder leftArith = new Spark41SpannerScanBuilder(parentTable);
      Spark41SpannerScanBuilder rightArith = new Spark41SpannerScanBuilder(childTable);
      assertEquals(
          isPostgres,
          leftArith.pushDownJoin(
              rightArith, JoinType.INNER_JOIN, leftCols, rightCols, arithmeticOverflowPred));
    }
  }

  @Test
  public void nestedFieldReferencesUntypedNullsMissingColumnsAndJsonColumnsFailCleanly() {
    StructType structType =
        new StructType(
            new StructField[] {
              new StructField("region", DataTypes.StringType, true, Metadata.empty())
            });
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, Metadata.empty()),
              new StructField("profile", structType, true, Metadata.empty()),
              new StructField("payload", DataTypes.StringType, true, Metadata.empty())
            });
    Map<String, String> spannerTypes =
        Map.of("id", "INT64", "profile", "STRUCT<region STRING>", "payload", "JSON");
    CaseInsensitiveStringMap props =
        new CaseInsensitiveStringMap(Map.of("enablePredicateSql", "true"));

    SpannerTable parentTable = mock(SpannerTable.class);
    InterleaveTableMetadata parentMeta = mock(InterleaveTableMetadata.class);
    when(parentMeta.getTableName()).thenReturn("Parent");
    when(parentMeta.getParentTable()).thenReturn(null);
    when(parentTable.name()).thenReturn("Parent");
    when(parentTable.schema()).thenReturn(schema);
    when(parentTable.properties()).thenReturn(props);
    when(parentTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(parentTable.getInterleavedTableMetadata()).thenReturn(parentMeta);

    SpannerTable childTable = mock(SpannerTable.class);
    InterleaveTableMetadata childMeta = mock(InterleaveTableMetadata.class);
    when(childMeta.getTableName()).thenReturn("Child");
    when(childMeta.getParentTable()).thenReturn("Parent");
    when(childTable.name()).thenReturn("Child");
    when(childTable.schema()).thenReturn(schema);
    when(childTable.properties()).thenReturn(props);
    when(childTable.getSpannerTypes()).thenReturn(spannerTypes);
    when(childTable.getInterleavedTableMetadata()).thenReturn(childMeta);

    ColumnWithAlias[] leftCols =
        new ColumnWithAlias[] {
          new ColumnWithAlias("id", "p_id"),
          new ColumnWithAlias("profile", "profile"),
          new ColumnWithAlias("payload", "payload")
        };
    ColumnWithAlias[] rightCols = new ColumnWithAlias[] {new ColumnWithAlias("id", "c_id")};

    Predicate baseJoin = equal(Expressions.column("p_id"), Expressions.column("c_id"));

    // 1. Multi-part nested struct reference (both left and right positions)
    NamedReference nestedRef = FieldReference.apply("profile.region");
    assertEquals(2, nestedRef.fieldNames().length);
    for (Predicate nestedPred :
        new Predicate[] {
          and(baseJoin, equal(nestedRef, stringLiteral("US"))),
          and(baseJoin, equal(stringLiteral("US"), nestedRef))
        }) {
      Spark41SpannerScanBuilder lb = new Spark41SpannerScanBuilder(parentTable);
      Spark41SpannerScanBuilder rb = new Spark41SpannerScanBuilder(childTable);
      assertFalse(lb.pushDownJoin(rb, JoinType.INNER_JOIN, leftCols, rightCols, nestedPred));
    }

    // 2. Missing column resolution (must return false, not throw NullPointerException)
    Predicate missingColPred =
        and(baseJoin, equal(Expressions.column("nonexistent_col"), Expressions.column("c_id")));
    assertFalse(
        new Spark41SpannerScanBuilder(parentTable)
            .pushDownJoin(
                new Spark41SpannerScanBuilder(childTable),
                JoinType.INNER_JOIN,
                leftCols,
                rightCols,
                missingColPred));

    // 3. JSON column in join predicate
    Predicate jsonPred = and(baseJoin, equal(Expressions.column("payload"), stringLiteral("{}")));
    assertFalse(
        new Spark41SpannerScanBuilder(parentTable)
            .pushDownJoin(
                new Spark41SpannerScanBuilder(childTable),
                JoinType.INNER_JOIN,
                leftCols,
                rightCols,
                jsonPred));

    // 4. Untyped NULL literal without column context
    Literal<Object> untypedNull = LiteralValue.apply(null, DataTypes.NullType);
    Predicate untypedNullPred = and(baseJoin, equal(untypedNull, untypedNull));
    assertFalse(
        new Spark41SpannerScanBuilder(parentTable)
            .pushDownJoin(
                new Spark41SpannerScanBuilder(childTable),
                JoinType.INNER_JOIN,
                leftCols,
                rightCols,
                untypedNullPred));
  }
}
