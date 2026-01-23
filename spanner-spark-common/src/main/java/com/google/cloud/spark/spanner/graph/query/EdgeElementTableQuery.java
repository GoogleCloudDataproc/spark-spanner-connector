package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.spark.spanner.SpannerTableSchema;
import com.google.cloud.spark.spanner.graph.PropertyGraph;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphElementTable;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphNodeTableReference;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import java.util.List;

/** Query for an edge table */
public class EdgeElementTableQuery extends ElementTableQuery {

  /**
   * Construct a query for an edge element table
   *
   * @param graphSchema schema of the graph
   * @param elementTable the element table to construct a query for
   * @param configs user configs for exporting the graph
   * @param exportIdColumnDirectly export the key column for src/dst directly to avoid the need of
   *     downstream ID translation. Should be true only when there is only one key column for src
   *     and one key column for dst.
   * @return a {@link EdgeElementTableQuery} for the element table.
   */
  public static EdgeElementTableQuery create(
      PropertyGraph graphSchema,
      GraphElementTable elementTable,
      SpannerTableSchema baseTableSchema,
      SpannerGraphConfigs configs,
      boolean exportIdColumnDirectly) {

    List<LabelConfig> matchedLabels = getMatchedLabels(elementTable, configs.edgeLabelConfigs);

    return new EdgeElementTableQuery(
        graphSchema,
        elementTable,
        baseTableSchema,
        configs.outputIndividualKeys,
        exportIdColumnDirectly,
        mergeProperties(elementTable, matchedLabels),
        mergeWhereClauses(matchedLabels));
  }

  private EdgeElementTableQuery(
      PropertyGraph graphSchema,
      GraphElementTable elementTable,
      SpannerTableSchema baseTableSchema,
      boolean outputIndividualKeys,
      boolean exportIdColumnDirectly,
      List<String> properties,
      String whereClause) {
    super(baseTableSchema, whereClause);
    if (!PropertyGraph.GRAPH_ELEMENT_TABLE_KIND_EDGE.equalsIgnoreCase(elementTable.kind)) {
      throw new IllegalArgumentException("Invalid elementTable kind: " + elementTable.kind);
    }
    graphSchema.checkEdgeReferenceKeyColumnsMatchNodeKeyColumns(elementTable);

    if (exportIdColumnDirectly) {
      if (elementTable.sourceNodeTable.edgeTableColumns.size() != 1
          || elementTable.destinationNodeTable.edgeTableColumns.size() != 1) {
        throw new IllegalArgumentException(
            "Cannot export multiple key columns directly as one SRC/DST column. ");
      }
      addDirectField(elementTable.sourceNodeTable.edgeTableColumns.get(0), "src");
      addDirectField(elementTable.destinationNodeTable.edgeTableColumns.get(0), "dst");
    } else {
      if (outputIndividualKeys) {
        addIndividualKeysForNodeTableReference("src", graphSchema, elementTable.sourceNodeTable);
        addIndividualKeysForNodeTableReference(
            "dst", graphSchema, elementTable.destinationNodeTable);
      } else {
        addCombinedId(
            "src",
            graphSchema.getTableId(elementTable.sourceNodeTable.nodeTableName),
            elementTable.sourceNodeTable.edgeTableColumns);
        addCombinedId(
            "dst",
            graphSchema.getTableId(elementTable.destinationNodeTable.nodeTableName),
            elementTable.destinationNodeTable.edgeTableColumns);
      }
    }

    addInnerProperties(elementTable.propertyDefinitions);
    addOutputProperties(graphSchema, properties);
  }

  private void addIndividualKeysForNodeTableReference(
      String type, PropertyGraph graphSchema, GraphNodeTableReference nodeTableReference) {
    addNodeTableColumn(type, graphSchema.getTableId(nodeTableReference.nodeTableName));
    addIndividualKeyColumns(
        type, nodeTableReference.edgeTableColumns, nodeTableReference.nodeTableColumns);
  }
}
