package com.google.cloud.spark;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spark.spanner.SpannerRelation;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.NotEqualTo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerRelationTest {

  @Test
  public void testBuildSQL() {
    SpannerRelation sr = new SpannerRelation("ATable", null);

    Filter[] filters = {new EqualTo("A", 10)};
    String reqColsNull = sr.buildSQL(null, filters);
    assertEquals("SELECT * FROM ATable WHERE (`A` = 10)", reqColsNull);

    String allNull = sr.buildSQL(null, null);
    assertEquals("SELECT * FROM ATable", allNull);

    String[] reqCols = {"A", "C"};
    String sReqCols = sr.buildSQL(reqCols, null);
    assertEquals("SELECT A, C FROM ATable", sReqCols);

    Filter[] noD = {new NotEqualTo("E", 20.9)};
    String sReqColPlusD = sr.buildSQL(reqCols, noD);
    assertEquals("SELECT A, C FROM ATable WHERE (E != 20.9)", sReqColPlusD);
  }
}
