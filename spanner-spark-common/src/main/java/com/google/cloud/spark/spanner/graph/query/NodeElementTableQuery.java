package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.spark.spanner.SpannerTableSchema;
import com.google.cloud.spark.spanner.graph.PropertyGraph;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphElementTable;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import java.util.List;

/** Query for a node table */
public class NodeElementTableQuery extends ElementTableQuery {

  /**
   * Construct a query for a node element table
   *
   * @param graphSchema schema of the graph
   * @param elementTable the element table to construct a query for
   * @param configs user configs for exporting the graph
   * @param exportIdColumnDirectly export the key column directly as the "id" column to avoid the
   *     need of downstream ID translation. Should be true only when there is only one key column in
   *     this table.
   * @return a {@link EdgeElementTableQuery} for the element table.
   */
  public static NodeElementTableQuery create(
      PropertyGraph graphSchema,
      GraphElementTable elementTable,
      SpannerTableSchema baseTableSchema,
      SpannerGraphConfigs configs,
      boolean exportIdColumnDirectly) {

    List<LabelConfig> matchedLabels = getMatchedLabels(elementTable, configs.nodeLabelConfigs);

    return new NodeElementTableQuery(
        graphSchema,
        elementTable,
        baseTableSchema,
        configs.outputIndividualKeys,
        exportIdColumnDirectly,
        mergeProperties(elementTable, matchedLabels),
        mergeWhereClauses(matchedLabels));
  }

  private NodeElementTableQuery(
      PropertyGraph graphSchema,
      GraphElementTable elementTable,
      SpannerTableSchema baseTableSchema,
      boolean outputIndividualKeys,
      boolean exportIdColumnDirectly,
      List<String> properties,
      String whereClause) {
    super(baseTableSchema, whereClause);
    if (!PropertyGraph.GRAPH_ELEMENT_TABLE_KIND_NODE.equalsIgnoreCase(elementTable.kind)) {
      throw new IllegalArgumentException("Invalid elementTable kind: " + elementTable.kind);
    }

    if (exportIdColumnDirectly) {
      if (elementTable.keyColumns.size() != 1) {
        throw new IllegalArgumentException(
            "Cannot export multiple key columns directly as one ID column.");
      }
      addDirectField(elementTable.keyColumns.get(0), "id");
    } else {
      if (outputIndividualKeys) {
        addNodeTableColumn("id", graphSchema.getTableId(elementTable.name));
        addIndividualKeyColumns("id", elementTable.keyColumns, elementTable.keyColumns);
      } else {
        addCombinedId("id", graphSchema.getTableId(elementTable.name), elementTable.keyColumns);
      }
    }

    addInnerProperties(elementTable.propertyDefinitions);
    addOutputProperties(graphSchema, properties);
  }
}
