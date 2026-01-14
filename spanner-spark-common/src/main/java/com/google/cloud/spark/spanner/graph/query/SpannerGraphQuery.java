package com.google.cloud.spark.spanner.graph.query;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spark.spanner.SpannerTable;
import com.google.cloud.spark.spanner.SpannerTableSchema;
import com.google.cloud.spark.spanner.graph.PropertyGraph;
import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphElementTable;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs;
import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.IntegralType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles naming, schema, and mapping of columns across layers of the queries for a Spanner Graph
 */
public class SpannerGraphQuery {

  private static final Logger log = LoggerFactory.getLogger(SpannerTable.class);

  public final StructType dataframeSchema;
  public final List<GraphSubQuery> graphSubqueries;

  /** Constructor for user-provided graph query */
  public SpannerGraphQuery(Connection conn, Statement query, boolean node) {
    DirectGraphQuery directGraphQuery = new DirectGraphQuery(conn, query, node);
    this.graphSubqueries = ImmutableList.of(directGraphQuery);
    this.dataframeSchema = fieldsToStruct(directGraphQuery.getOutputSparkFields());
  }

  public SpannerGraphQuery(
      Connection conn, PropertyGraph graphSchema, SpannerGraphConfigs configs, boolean node) {
    List<GraphElementTable> nodeTables =
        getMatchedElementTables(graphSchema.nodeTables, configs.nodeLabelConfigs);
    List<GraphElementTable> edgeTables =
        getMatchedElementTables(graphSchema.edgeTables, configs.edgeLabelConfigs);
    Map<String, SpannerTableSchema> baseTableSchemas =
        getBaseTableSchemas(conn, Iterables.concat(nodeTables, edgeTables));
    boolean idColumnsExist =
        !configs.disableDirectIdExport && getIdColumnsExist(nodeTables, baseTableSchemas);

    List<ElementTableQuery> subQueries = new ArrayList<>();
    if (node) {
      if (nodeTables.size() == 0) {
        throw new IllegalArgumentException("No node table left.");
      }
      for (GraphElementTable table : nodeTables) {
        subQueries.add(
            NodeElementTableQuery.create(
                graphSchema,
                table,
                baseTableSchemas.get(table.baseTableName),
                configs,
                idColumnsExist));
      }
    } else {
      if (edgeTables.size() == 0) {
        throw new IllegalArgumentException("No edge table left.");
      }
      checkValidTableReference(nodeTables, edgeTables);
      for (GraphElementTable table : edgeTables) {
        subQueries.add(
            EdgeElementTableQuery.create(
                graphSchema,
                table,
                baseTableSchemas.get(table.baseTableName),
                configs,
                idColumnsExist));
      }
    }
    this.graphSubqueries = Collections.unmodifiableList(subQueries);
    this.dataframeSchema =
        ElementTableQuery.mergeDataframeSchema(
            subQueries, node ? configs.nodeLabelConfigs : configs.edgeLabelConfigs);
  }

  private static boolean getIdColumnsExist(
      List<GraphElementTable> nodeTables, Map<String, SpannerTableSchema> baseTableSchemas) {
    if (nodeTables.size() != 1 || nodeTables.get(0).keyColumns.size() != 1) {
      return false;
    }
    GraphElementTable nodeTable = nodeTables.get(0);
    if (nodeTable.keyColumns.size() != 1) {
      return false;
    }
    SpannerTableSchema tableSchema =
        Objects.requireNonNull(baseTableSchemas.get(nodeTable.baseTableName));
    String keyColumn = nodeTables.get(0).keyColumns.get(0);
    return tableSchema.getStructFieldForColumn(keyColumn).dataType() instanceof IntegralType;
  }

  private static Map<String, SpannerTableSchema> getBaseTableSchemas(
      Connection conn, Iterable<GraphElementTable> elementTables) {
    Map<String, SpannerTableSchema> schemas = new HashMap<>();
    for (GraphElementTable t : elementTables) {
      schemas.put(t.baseTableName, new SpannerTableSchema(conn, t.baseTableName, false));
    }
    return schemas;
  }

  /**
   * Filters a list of element tables based on label filters
   *
   * @param elementTables the list of element tables to filter
   * @param labelConfigs the label config specified by the user
   * @return a list of element tables that have a matched label config
   */
  private List<GraphElementTable> getMatchedElementTables(
      List<GraphElementTable> elementTables, List<LabelConfig> labelConfigs) {
    if (labelConfigs == null || labelConfigs.isEmpty() || labelConfigs.get(0).label.equals("*")) {
      return new ArrayList<>(elementTables);
    }

    Set<String> targetLabels =
        labelConfigs.stream().map(lc -> lc.label.trim().toLowerCase()).collect(Collectors.toSet());
    return elementTables.stream()
        .filter(
            t -> {
              for (String label : t.labelNames) {
                if (targetLabels.contains(label.toLowerCase())) {
                  return true;
                }
              }
              return false;
            })
        .collect(Collectors.toList());
  }

  private void checkValidTableReference(
      List<GraphElementTable> nodeTables, List<GraphElementTable> edgeTables) {
    Set<String> nodeTableNames = nodeTables.stream().map(t -> t.name).collect(Collectors.toSet());
    for (GraphElementTable t : edgeTables) {
      String srcTable = t.sourceNodeTable.nodeTableName;
      String dstTable = t.destinationNodeTable.nodeTableName;
      if (!nodeTableNames.contains(srcTable) || !nodeTableNames.contains(dstTable)) {
        throw new IllegalArgumentException(
            String.format(
                "One or both of the referenced node tables (%s, %s) of edge table %s are filtered"
                    + " out. Existing node tables: %s.",
                srcTable, dstTable, t.name, nodeTableNames));
      }
    }
  }

  private StructType fieldsToStruct(List<StructField> fields) {
    StructType result = new StructType();
    for (StructField field : fields) {
      result = result.add(field);
    }
    return result;
  }
}
