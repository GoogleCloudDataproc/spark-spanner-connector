package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.SpannerConnectorException;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parses INFORMATION_SCHEMA.PROPERTY_GRAPHS as defined in
 * https://cloud.google.com/spanner/docs/information-schema#property-graphs.
 */
public class PropertyGraph {

  public static String GRAPH_ELEMENT_TABLE_KIND_NODE = "NODE";
  public static String GRAPH_ELEMENT_TABLE_KIND_EDGE = "EDGE";

  private static final String GRAPH_SCHEMA_QUERY =
      "SELECT PROPERTY_GRAPH_METADATA_JSON FROM "
          + "INFORMATION_SCHEMA.PROPERTY_GRAPHS WHERE PROPERTY_GRAPH_NAME = @graph";

  public String catalog;
  public String schema;
  public String name;
  public List<GraphElementTable> nodeTables;
  public List<GraphElementTable> edgeTables;
  public List<GraphElementLabel> labels;
  public List<GraphPropertyDeclaration> propertyDeclarations;
  private Map<String, Integer> tableIdMapping;

  private PropertyGraph() {}

  public int getTableId(String elementTableName) {
    Integer tableId = tableIdMapping.get(elementTableName);
    if (tableId == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find tableId for table with name=%s", elementTableName));
    }
    return tableId;
  }

  public String getPropertyType(String propertyName) {
    for (GraphPropertyDeclaration gpd : propertyDeclarations) {
      if (gpd.name.equals(propertyName)) {
        return gpd.type;
      }
    }
    throw new IllegalArgumentException("Cannot find property: " + propertyName);
  }

  public void checkEdgeReferenceKeyColumnsMatchNodeKeyColumns(GraphElementTable edgeTable) {
    if (!edgeTable.kind.equalsIgnoreCase(GRAPH_ELEMENT_TABLE_KIND_EDGE)) {
      throw new IllegalArgumentException();
    }
    Map<String, Set<String>> nodeTableKeyColumns = new HashMap<>();
    for (GraphElementTable nodeTable : nodeTables) {
      nodeTableKeyColumns.put(nodeTable.name, new HashSet<>(nodeTable.keyColumns));
    }
    throwIfNodeTableKeyColumnsMismatch(
        nodeTableKeyColumns, edgeTable.sourceNodeTable, edgeTable.name, "source");
    throwIfNodeTableKeyColumnsMismatch(
        nodeTableKeyColumns, edgeTable.destinationNodeTable, edgeTable.name, "destination");
  }

  private static void throwIfNodeTableKeyColumnsMismatch(
      Map<String, Set<String>> nodeTableKeyColumns,
      GraphNodeTableReference nodeTableReference,
      String edgeTableName,
      String type) {
    String nodeTableName = nodeTableReference.nodeTableName;
    Set<String> expected = nodeTableKeyColumns.get(nodeTableReference.nodeTableName);
    if (!expected.equals(new HashSet<>(nodeTableReference.nodeTableColumns))) {
      throw new UnsupportedOperationException(
          String.format(
              "%s of edge table %s references node table %s using column(s) [%s], "
                  + "while key column(s) of node table %s are [%s]. "
                  + "Currently, the connector expects the key columns an edge table used to reference "
                  + "source/destination nodes to match the key columns of the node table.",
              type,
              edgeTableName,
              nodeTableName,
              String.join(", ", nodeTableReference.nodeTableColumns),
              nodeTableName,
              String.join(", ", expected)));
    }
  }

  public static class GraphElementTable {

    public String name;
    public String kind;
    public String baseCatalogName;
    public String baseSchemaName;
    public String baseTableName;
    public List<String> keyColumns;
    public List<String> labelNames;
    public List<GraphPropertyDefinition> propertyDefinitions;
    public GraphNodeTableReference sourceNodeTable;
    public GraphNodeTableReference destinationNodeTable;
  }

  public static class GraphNodeTableReference {

    public String nodeTableName;
    public List<String> edgeTableColumns;
    public List<String> nodeTableColumns;
  }

  public static class GraphElementLabel {

    public String name;
    public List<String> propertyDeclarationNames;
  }

  public static class GraphPropertyDeclaration {

    public String name;
    public String type;
  }

  public static class GraphPropertyDefinition {

    public String propertyDeclarationName;
    public String valueExpressionSql;
  }

  public static class Builder {

    public static PropertyGraph getFromSpanner(Connection conn, String graph) {
      Statement schemaQuery =
          Statement.newBuilder(GRAPH_SCHEMA_QUERY).bind("graph").to(graph).build();
      try (ResultSet rs = conn.executeQuery(schemaQuery)) {
        if (!rs.next()) {
          throw new SpannerConnectorException(
              String.format(
                  "Unable to find the schema for graph %s. Query: %s", graph, schemaQuery));
        }
        String schemaJson = rs.getCurrentRowAsStruct().getJson(0);
        if (rs.next()) {
          throw new SpannerConnectorException(
              String.format(
                  "Found more than one schema for graph %s. Query: %s", graph, schemaQuery));
        }
        return fromJson(schemaJson);
      }
    }

    public static PropertyGraph fromJson(String json) {
      PropertyGraph propertyGraph = new Gson().fromJson(json, PropertyGraph.class);
      propertyGraph.tableIdMapping =
          getTableIdMapping(propertyGraph.nodeTables, propertyGraph.edgeTables);
      return propertyGraph;
    }

    private static Map<String, Integer> getTableIdMapping(
        List<GraphElementTable> nodeTables, List<GraphElementTable> edgeTables) {
      int tableCount = 0;
      Map<String, Integer> tableIdMapping = new HashMap<>();
      for (String tableName :
          nodeTables.stream().map(t -> t.name).sorted().collect(Collectors.toList())) {
        tableIdMapping.put(tableName, ++tableCount);
      }
      for (String tableName :
          edgeTables.stream().map(t -> t.name).sorted().collect(Collectors.toList())) {
        tableIdMapping.put(tableName, ++tableCount);
      }
      return tableIdMapping;
    }
  }
}
