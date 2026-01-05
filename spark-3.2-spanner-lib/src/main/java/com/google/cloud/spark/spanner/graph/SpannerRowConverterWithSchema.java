package com.google.cloud.spark.spanner.graph;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spark.spanner.SpannerRowConverter;
import com.google.cloud.spark.spanner.SpannerUtils;
import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Converts rows from Spanner query outputs to rows in a Spark DataFrame with specific schema. */
public class SpannerRowConverterWithSchema implements SpannerRowConverter, Serializable {

  private final List<Field> sparkFields = new ArrayList<>();

  public SpannerRowConverterWithSchema(
      StructType dataframeSchema,
      List<String> queryOutputColumns,
      Map<String, Integer> fixedValues) {
    Map<String, Long> nameToQueryOutputColumnIndex =
        Streams.mapWithIndex(queryOutputColumns.stream(), Tuple::of)
            .collect(Collectors.toMap(Tuple::x, Tuple::y));
    for (StructField field : dataframeSchema.fields()) {
      Integer fixedValue = fixedValues.get(field.name());
      if (fixedValue != null) {
        sparkFields.add(new FixedIntField(fixedValue));
        continue;
      }
      Long spannerRowIndex = nameToQueryOutputColumnIndex.get(field.name());
      if (spannerRowIndex != null) {
        sparkFields.add(new ValueField(spannerRowIndex.intValue()));
        continue;
      }
      sparkFields.add(new NullField());
    }
  }

  @Override
  public InternalRow convert(Struct spannerRow) {
    GenericInternalRow sparkRow = new GenericInternalRow(sparkFields.size());
    for (int i = 0; i < sparkFields.size(); ++i) {
      sparkFields.get(i).update(sparkRow, spannerRow, i);
    }
    return sparkRow;
  }

  private abstract static class Field implements Serializable {
    public abstract void update(GenericInternalRow sparkRow, Struct spannerRow, int sparkRowIndex);
  }

  private static class NullField extends Field {
    @Override
    public void update(GenericInternalRow sparkRow, Struct spannerRow, int sparkRowIndex) {
      sparkRow.update(sparkRowIndex, null);
    }
  }

  private static class FixedIntField extends Field {

    private final int value;

    FixedIntField(int value) {
      this.value = value;
    }

    @Override
    public void update(GenericInternalRow sparkRow, Struct spannerRow, int sparkRowIndex) {
      sparkRow.setInt(sparkRowIndex, value);
    }
  }

  private static class ValueField extends Field {

    private final int spannerRowIndex;

    ValueField(int spannerRowIndex) {
      this.spannerRowIndex = spannerRowIndex;
    }

    @Override
    public void update(GenericInternalRow sparkRow, Struct spannerRow, int sparkRowIndex) {
      SpannerUtils.convertRowAt(spannerRow, spannerRowIndex, sparkRow, sparkRowIndex);
    }
  }
}
