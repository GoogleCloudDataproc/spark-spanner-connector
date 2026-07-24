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
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class PredicateToExprConverterTest {

  @Test
  public void equalProducesEqExpr() {

    StructType schema = new StructType().add("A", DataTypes.IntegerType);
    NamedReference ref = mock(NamedReference.class);
    when(ref.fieldNames()).thenReturn(new String[] {"A"});

    Literal<Integer> lit = mock(Literal.class);
    when(lit.value()).thenReturn(1);

    Predicate predicate = mock(Predicate.class);
    when(predicate.name()).thenReturn("=");
    when(predicate.children()).thenReturn(new Expression[] {ref, lit});

    BoolExpr expr = PredicateToExprConverter.translatePredicate(predicate, schema);

    assertThat(expr).isInstanceOf(EqExpr.class);

    EqExpr eq = (EqExpr) expr;

    ValueExpr leftExpr = eq.getLeft();
    assertTrue(leftExpr instanceof ColumnExpr);
    assertEquals("A", ((ColumnExpr) leftExpr).getColumnName());

    ValueExpr rightExpr = eq.getRight();
    assertTrue(rightExpr instanceof LiteralExpr);
    LiteralExpr literalExpr = (LiteralExpr) rightExpr;
    assertEquals(DataTypes.IntegerType, literalExpr.getSparkType());
    assertEquals(Integer.valueOf("1"), (Integer) literalExpr.getValue());
  }
}
