package com.google.cloud.spark.spanner.graph.query;

import java.util.Collection;
import java.util.stream.Collectors;

/** Represents a field in the SELECT clause */
public class SelectField {

  public final String inputExpression;
  public final String outputName;

  public SelectField(String columnName) {
    this(columnName, columnName);
  }

  public SelectField(String inputExpression, String outputName) {
    this.inputExpression = inputExpression.trim();
    this.outputName = outputName.trim();
  }

  @Override
  public String toString() {
    if (inputExpression.equals(outputName)) {
      return outputName;
    } else {
      return inputExpression + " AS " + outputName;
    }
  }

  public static String join(Collection<SelectField> selectFields) {
    return selectFields.stream().map(SelectField::toString).collect(Collectors.joining(", "));
  }
}
