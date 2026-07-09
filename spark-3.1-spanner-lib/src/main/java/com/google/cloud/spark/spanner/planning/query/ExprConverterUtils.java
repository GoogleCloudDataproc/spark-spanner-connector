package com.google.cloud.spark.spanner.planning.query;

import com.google.cloud.spark.spanner.planning.expression.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprConverterUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExprConverterUtils.class);

  public static ColumnExpr toColumn(String name, StructType schema) {
    logger.debug("Looking up column '{}' in schema {}", name, schema.treeString());

    StructField field = schema.apply(name);

    return new ColumnExpr(name, field.dataType(), field.nullable());
  }

  public static LiteralExpr toLiteral(Object value, StructType schema, String columnName) {
    logger.debug("Looking up literal column '{}' in schema {}", columnName, schema.treeString());

    StructField field = schema.apply(columnName);

    return new LiteralExpr(value, field.dataType());
  }
}
