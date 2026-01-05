package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spark.spanner.SpannerRowConverter;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.SpannerTableSchema;
import com.google.cloud.spark.spanner.graph.PropertyGraph;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphElementTable;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphPropertyDefinition;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import com.google.cloud.spark.spanner.graph.SpannerRowConverterWithSchema;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Query for an element table */
public abstract class ElementTableQuery implements GraphSubQuery {

  static final String internalFieldPrefix = "__spark_spanner_connector_internal_";
  static final String propertyPrefix = "property_";
  protected final List<SelectField> innerSelects = new ArrayList<>();
  protected final List<SelectField> outerSelects = new ArrayList<>();
  protected final List<StructField> outputSparkFields = new ArrayList<>();
  protected final Map<String, Integer> fixedValues = new HashMap<>();
  private final String whereClause;
  private final SpannerTableSchema baseTableSchema;

  protected ElementTableQuery(SpannerTableSchema baseTableSchema, String whereClause) {
    this.whereClause = whereClause.trim();
    this.baseTableSchema = Objects.requireNonNull(baseTableSchema);
  }

  public static List<LabelConfig> getMatchedLabels(
      GraphElementTable elementTable, @Nullable List<LabelConfig> labelConfigs) {
    Set<String> elementTableLabels = new HashSet<>(elementTable.labelNames);
    List<LabelConfig> matchedLabels = new ArrayList<>();
    if (labelConfigs != null) {
      for (LabelConfig labelConfig : labelConfigs) {
        if (labelConfig.label.equals("*") || elementTableLabels.contains(labelConfig.label)) {
          matchedLabels.add(labelConfig);
        }
      }
    }
    return matchedLabels;
  }

  protected static SelectField buildSelectFieldForId(
      String idColumnName, int tableId, List<String> keyColumns) {
    String elementKeysString;
    if (keyColumns.size() == 1) {
      elementKeysString = String.format("CAST(%s AS STRING)", keyColumns.get(0));
    } else {
      elementKeysString =
          keyColumns.stream()
              .sorted()
              .map(c -> String.format("CAST(%s AS STRING)", c))
              .map(c -> String.format("REPLACE(%s, \"\\\\\", \"\\\\\\\\\")", c))
              .map(c -> String.format("REPLACE(%s, \"|\", \"\\\\|\")", c))
              .collect(Collectors.joining(", \"|\", "));
    }
    return new SelectField(
        String.format("CONCAT(\"%d@\", %s)", tableId, elementKeysString), idColumnName);
  }

  protected static List<String> mergeProperties(
      GraphElementTable elementTable, List<LabelConfig> labelConfigs) {
    Set<String> availableProperties =
        elementTable.propertyDefinitions.stream()
            .map(p -> p.propertyDeclarationName)
            .collect(Collectors.toSet());
    Set<String> graphPropertiesSet = new HashSet<>();
    for (LabelConfig labelConfig : labelConfigs) {
      for (String property : labelConfig.properties) {
        if (availableProperties.contains(property)) {
          graphPropertiesSet.add(property);
        }
      }
    }
    return new ArrayList<>(graphPropertiesSet);
  }

  protected static String mergeWhereClauses(List<LabelConfig> labelConfigs) {
    return labelConfigs.stream()
        .map(lc -> lc.filter)
        .filter(Objects::nonNull)
        .map(String::trim)
        .filter(f -> !f.isEmpty())
        .collect(Collectors.joining(" OR "));
  }

  static String getInternalNameForKeyColumn(String keyColumn, String type) {
    return String.format(internalFieldPrefix + "%s_key_column_%s", type, keyColumn);
  }

  static String getNodeTableColumnName(String type) {
    return String.format(internalFieldPrefix + "%s_node_table", type);
  }

  static String getInternalNameForId(String type) {
    return internalFieldPrefix + type;
  }

  static String getNameForProperty(String property) {
    return propertyPrefix + property;
  }

  @Override
  public List<StructField> getOutputSparkFields() {
    return Collections.unmodifiableList(outputSparkFields);
  }

  @Override
  public Tuple<Statement, SpannerRowConverter> getQueryAndConverter(StructType dataframeSchema) {
    Set<String> outputColumnsToKeep =
        Arrays.stream(dataframeSchema.fields()).map(StructField::name).collect(Collectors.toSet());
    List<SelectField> innerSelects = fixEmptySelectFields(this.innerSelects);
    List<SelectField> prunedOuterSelects =
        outerSelects.stream()
            .filter(selectField -> outputColumnsToKeep.contains(selectField.outputName))
            .collect(Collectors.toList());
    prunedOuterSelects = fixEmptySelectFields(prunedOuterSelects);

    String innerQuery =
        String.format("SELECT %s FROM %s", SelectField.join(innerSelects), baseTableSchema.name);
    String outerQuery =
        String.format("SELECT %s FROM (%s)", SelectField.join(prunedOuterSelects), innerQuery);
    if (whereClause != null && !whereClause.isEmpty()) {
      outerQuery += " WHERE " + whereClause;
    }

    List<String> outputColumns =
        prunedOuterSelects.stream().map(sf -> sf.outputName).collect(Collectors.toList());
    SpannerRowConverterWithSchema rowConverter =
        new SpannerRowConverterWithSchema(dataframeSchema, outputColumns, fixedValues);
    return Tuple.of(Statement.of(outerQuery), rowConverter);
  }

  static StructType mergeDataframeSchema(
      List<ElementTableQuery> columns, @Nullable List<LabelConfig> labelConfigs) {
    Map<String, StructField> fields =
        columns.stream()
            .flatMap(c -> c.getOutputSparkFields().stream())
            .collect(
                Collectors.toMap(
                    StructField::name,
                    Function.identity(),
                    (sf1, sf2) -> {
                      if (sf1.dataType() != sf2.dataType()) {
                        throw new IllegalArgumentException(
                            String.format(
                                "StructFields have different data types. sf1=%s, sf2=%s ",
                                sf1, sf2));
                      }
                      Metadata metadata =
                          new MetadataBuilder()
                              .withMetadata(sf1.metadata())
                              .withMetadata(sf2.metadata())
                              .build();
                      return new StructField(
                          sf1.name(), sf2.dataType(), sf1.nullable() || sf2.nullable(), metadata);
                    }));

    List<String> idTypes = ImmutableList.of("id", "src", "dst");

    StructType schema = new StructType();
    schema = addFieldsIfExist(schema, fields, idTypes);

    for (String keyType : idTypes) {
      schema =
          addFieldsIfExist(
              schema, fields, ImmutableList.of(ElementTableQuery.getNodeTableColumnName(keyType)));
      List<String> keyColumns =
          fields.keySet().stream()
              .filter(s -> s.startsWith(ElementTableQuery.internalFieldPrefix + keyType))
              .sorted()
              .collect(Collectors.toList());
      schema = addFieldsIfExist(schema, fields, keyColumns);
    }

    if (labelConfigs != null) {
      List<String> requestedPropertyColumns =
          labelConfigs.stream()
              .flatMap(labelConfig -> labelConfig.properties.stream())
              .map(p -> propertyPrefix + p)
              .collect(Collectors.toList());
      if (!(new HashSet<>(requestedPropertyColumns)).equals(fields.keySet())) {
        throw new AssertionError(
            String.format(
                "Remaining columns do not match the columns for the requested properties. "
                    + "Requested property columns: %s. Remaining columns: %s",
                requestedPropertyColumns, fields.keySet()));
      }
      schema = addFieldsIfExist(schema, fields, requestedPropertyColumns);
    } else {
      if (!fields.isEmpty()) {
        throw new AssertionError(String.format("Leftover columns: %s", fields.keySet()));
      }
    }
    return schema;
  }

  protected void addDirectField(String inputColumn, String outputColumn) {
    String internalName = getInternalNameForId(outputColumn);
    innerSelects.add(new SelectField(inputColumn, internalName));
    outerSelects.add(new SelectField(internalName, outputColumn));
    DataType sparkType = baseTableSchema.getStructFieldForColumn(inputColumn).dataType();
    outputSparkFields.add(new StructField(outputColumn, sparkType, false, Metadata.empty()));
  }

  protected void addNodeTableColumn(String type, int nodeTableId) {
    String nodeTableColumnName = getNodeTableColumnName(type);
    outputSparkFields.add(
        new StructField(nodeTableColumnName, DataTypes.IntegerType, false, Metadata.empty()));
    fixedValues.put(nodeTableColumnName, nodeTableId);
  }

  protected void addIndividualKeyColumns(
      String type, List<String> inputColumns, List<String> outputColumns) {
    if (inputColumns.size() != outputColumns.size()) {
      throw new IllegalArgumentException("inputColumns.size() != outputColumns.size()");
    }
    for (int i = 0; i < inputColumns.size(); i++) {
      String outputName = getInternalNameForKeyColumn(outputColumns.get(i), type);
      innerSelects.add(new SelectField(inputColumns.get(i), outputName));
      outerSelects.add(new SelectField(outputName));
      DataType sparkType = baseTableSchema.getStructFieldForColumn(inputColumns.get(i)).dataType();
      outputSparkFields.add(new StructField(outputName, sparkType, true, Metadata.empty()));
    }
  }

  protected void addCombinedId(String idType, int tableId, List<String> keyColumns) {
    String internalIdName = getInternalNameForId(idType);
    innerSelects.add(buildSelectFieldForId(internalIdName, tableId, keyColumns));
    outerSelects.add(new SelectField(internalIdName, idType));
    outputSparkFields.add(new StructField(idType, DataTypes.StringType, false, Metadata.empty()));
  }

  protected void addInnerProperties(List<GraphPropertyDefinition> propertyDefinitions) {
    for (GraphPropertyDefinition gpd : propertyDefinitions) {
      innerSelects.add(new SelectField(gpd.valueExpressionSql, gpd.propertyDeclarationName));
    }
  }

  protected void addOutputProperties(PropertyGraph graphSchema, List<String> properties) {
    for (String property : properties) {
      String typeCode = graphSchema.getPropertyType(property);
      DataType dataType = SpannerTable.ofSpannerStrType(typeCode, true);
      String renamedProperty = getNameForProperty(property);
      outerSelects.add(new SelectField(property, renamedProperty));
      outputSparkFields.add(new StructField(renamedProperty, dataType, true, Metadata.empty()));
    }
  }

  /**
   * Make sure the fields to select is not empty, which might be the case when no column is needed
   * (e.g., when counting the number of rows).
   */
  private static List<SelectField> fixEmptySelectFields(List<SelectField> selectFields) {
    if (selectFields.isEmpty()) {
      return ImmutableList.of(new SelectField("null"));
    }
    return selectFields;
  }

  private static StructType addFieldsIfExist(
      StructType schema, Map<String, StructField> fieldsMap, Iterable<String> fieldsToAdd) {
    for (String prioritizedField : fieldsToAdd) {
      StructField field = fieldsMap.remove(prioritizedField);
      if (field != null) {
        schema = schema.add(field);
      }
    }
    return schema;
  }
}
