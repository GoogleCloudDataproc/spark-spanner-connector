package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.spark.spanner.SparkFilterUtils;
import java.util.Collection;
import java.util.stream.Collectors;

/** Represents a field in the SELECT clause */
public class SelectField {

  public final String inputExpression;
  public final String outputName;
  private final String renderedSql;

  public SelectField(String columnName) {
    this(columnName, columnName);
  }

  public SelectField(String inputExpression, String outputName) {
    String trimmedInput = inputExpression.trim();
    String trimmedOutput = outputName.trim();
    this.inputExpression = trimmedInput;
    this.outputName = trimmedOutput;
    String quotedOutput = SparkFilterUtils.quoteIdentifier(trimmedOutput, false);
    if (trimmedInput.equals(trimmedOutput)) {
      this.renderedSql = quotedOutput;
    } else {
      this.renderedSql = trimmedInput + " AS " + quotedOutput;
    }
  }

  private SelectField(String inputExpression, String outputName, String renderedSql) {
    this.inputExpression = inputExpression;
    this.outputName = outputName;
    this.renderedSql = renderedSql;
  }

  public static SelectField column(String columnName) {
    String trimmed = columnName.trim();
    String quoted = SparkFilterUtils.quoteIdentifier(trimmed, false);
    return new SelectField(quoted, trimmed, quoted);
  }

  public static SelectField columnWithAlias(String inputColumn, String outputAlias) {
    String trimmedInput = inputColumn.trim();
    String trimmedOutput = outputAlias.trim();
    String quotedInput = SparkFilterUtils.quoteIdentifier(trimmedInput, false);
    String quotedOutput = SparkFilterUtils.quoteIdentifier(trimmedOutput, false);
    if (trimmedInput.equals(trimmedOutput)) {
      return new SelectField(quotedInput, trimmedOutput, quotedOutput);
    }
    return new SelectField(quotedInput, trimmedOutput, quotedInput + " AS " + quotedOutput);
  }

  public static SelectField expressionWithAlias(String sqlExpression, String outputAlias) {
    String trimmedExpr = sqlExpression.trim();
    String trimmedOutput = outputAlias.trim();
    String quotedOutput = SparkFilterUtils.quoteIdentifier(trimmedOutput, false);
    return new SelectField(trimmedExpr, trimmedOutput, trimmedExpr + " AS " + quotedOutput);
  }

  public static SelectField literal(String sqlLiteral) {
    String trimmed = sqlLiteral.trim();
    return new SelectField(trimmed, trimmed, trimmed);
  }

  @Override
  public String toString() {
    return renderedSql;
  }

  public static String join(Collection<SelectField> selectFields) {
    return selectFields.stream().map(SelectField::toString).collect(Collectors.joining(", "));
  }
}
