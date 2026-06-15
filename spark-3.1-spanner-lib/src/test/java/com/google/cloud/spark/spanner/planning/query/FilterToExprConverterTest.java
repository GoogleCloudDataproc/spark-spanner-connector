package com.google.cloud.spark.spanner.planning.query;

import static org.junit.Assert.*;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spark.spanner.planning.expression.*;
import com.google.cloud.spark.spanner.rendering.SqlExprVisitor;
import java.util.Optional;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.junit.Before;
import org.junit.Test;

public class FilterToExprConverterTest {

  private StructType schema;
  private SqlExprVisitor sqlExprVisitor;

  @Before
  public void setUp() {
    schema =
        new StructType(
            new StructField[] {
              new StructField("word", DataTypes.StringType, true, Metadata.empty()),
              new StructField("word_count", DataTypes.LongType, false, Metadata.empty())
            });
    sqlExprVisitor = SqlExprVisitor.create(Dialect.GOOGLE_STANDARD_SQL);
  }

  @Test
  public void testEqualTo() {
    Filter[] filters = {new EqualTo("word", "augurs")};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof EqExpr);

    EqExpr expr = (EqExpr) result.get();

    ValueExpr leftExpr = expr.getLeft();
    assertTrue(leftExpr instanceof ColumnExpr);
    assertEquals("word", ((ColumnExpr) leftExpr).getColumnName());

    ValueExpr rightExpr = expr.getRight();
    assertTrue(rightExpr instanceof LiteralExpr);
    LiteralExpr literalExpr = (LiteralExpr) rightExpr;
    assertEquals(DataTypes.StringType, literalExpr.getSparkType());
    assertEquals("augurs", (String) literalExpr.getValue());
  }

  @Test
  public void testGreaterThan() {
    Filter[] filters = {new GreaterThan("word_count", 10L)};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof GtExpr);

    GtExpr expr = (GtExpr) result.get();

    ValueExpr leftExpr = expr.getLeft();
    assertTrue(leftExpr instanceof ColumnExpr);
    assertEquals("word_count", ((ColumnExpr) leftExpr).getColumnName());

    ValueExpr rightExpr = expr.getRight();
    assertTrue(rightExpr instanceof LiteralExpr);
    LiteralExpr literalExpr = (LiteralExpr) rightExpr;
    assertEquals(DataTypes.LongType, literalExpr.getSparkType());
    assertEquals((Long) 10L, (Long) literalExpr.getValue());
  }

  @Test
  public void testAndFiltersAreCombined() {
    Filter[] filters = {new EqualTo("word", "augurs"), new GreaterThan("word_count", 10L)};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof AndExpr);

    AndExpr and = (AndExpr) result.get();

    assertTrue(and.getLeft() instanceof EqExpr);
    assertTrue(and.getRight() instanceof GtExpr);
  }

  @Test
  public void testSparkAndFilter() {
    Filter[] filters = {new And(new EqualTo("word", "augurs"), new GreaterThan("word_count", 10L))};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof AndExpr);
  }

  @Test
  public void testInFilter() {
    Filter[] filters = {new In("word", new Object[] {"augurs", "hamlet"})};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof InExpr);

    InExpr expr = (InExpr) result.get();

    assertEquals(2, expr.getValues().size());
    assertEquals("augurs", expr.getValues().get(0).getValue());
    assertEquals("hamlet", expr.getValues().get(1).getValue());
  }

  @Test
  public void testIsNull() {
    Filter[] filters = {new IsNull("word")};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof IsNullExpr);
  }

  @Test
  public void testIsNotNull() {
    Filter[] filters = {new IsNotNull("word")};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof IsNotNullExpr);
  }

  @Test
  public void testStartsWith() {
    Filter[] filters = {new StringStartsWith("word", "aug")};

    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(filters, schema);

    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof StartsWithExpr);

    StartsWithExpr expr = (StartsWithExpr) result.get();

    assertEquals("aug", expr.getPrefix().getValue());
  }

  @Test
  public void testEmptyFiltersReturnsEmptyOptional() {
    Optional<BoolExpr> result = FilterToExprConverter.translateFilters(new Filter[0], schema);

    assertFalse(result.isPresent());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedFilterThrows() {
    Filter[] filters = {new AlwaysTrue()};

    FilterToExprConverter.translateFilters(filters, schema);
  }

  @Test
  public void testLiteralUsesColumnType() {
    Filter[] filters = {new EqualTo("word_count", 123L)};

    EqExpr expr = (EqExpr) FilterToExprConverter.translateFilters(filters, schema).get();

    assertEquals(DataTypes.LongType, ((LiteralExpr) expr.getRight()).getSparkType());
  }
}
