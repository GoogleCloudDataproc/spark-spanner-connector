package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spark.spanner.graph.PropertyGraph.GraphElementTable;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** User-supplied configs for exporting graphs in Spanner */
public class SpannerGraphConfigs {

  /** Do not export ID columns directly even when it can */
  public boolean disableDirectIdExport = false;

  /** Output individual node element key columns instead of one column that concatenate all keys */
  public boolean outputIndividualKeys = false;

  /** Labels and properties to fetch for nodes */
  public List<LabelConfig> nodeLabelConfigs = new ArrayList<>();

  /** Labels and properties to fetch for edges */
  public List<LabelConfig> edgeLabelConfigs = new ArrayList<>();

  /**
   * Same as <a
   * href="https://cloud.google.com/spanner/docs/reference/rest/v1/PartitionOptions">PartitionOptions</a>
   */
  public Long partitionSizeBytes = null;

  /** Extra headers added to requests when fetching partitions of the graph */
  public Map<String, List<String>> extraHeaders = null;

  public static SpannerGraphConfigs fromJson(String json) {
    return new Gson().fromJson(json, SpannerGraphConfigs.class);
  }

  public void validate(PropertyGraph graphSchema, boolean directGqlQuery) {
    if (directGqlQuery) {
      if (!nodeLabelConfigs.isEmpty() || !edgeLabelConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "nodeLabelConfigs and edgeLabelConfigs are invalid "
                + "options when using GQL queries are provided.");
      }
    }
    checkExclusiveAnyLabel(nodeLabelConfigs);
    checkExclusiveAnyLabel(edgeLabelConfigs);
    for (LabelConfig labelConfig : nodeLabelConfigs) {
      labelConfig.validate(graphSchema, /*node=*/ true);
    }
    for (LabelConfig labelConfig : edgeLabelConfigs) {
      labelConfig.validate(graphSchema, /*node=*/ false);
    }
    if (partitionSizeBytes != null && partitionSizeBytes <= 0) {
      throw new IllegalArgumentException("partitionSize must be greater than 0");
    }
  }

  private void checkExclusiveAnyLabel(List<LabelConfig> labelConfigs) {
    boolean hasAnyLabel = labelConfigs.stream().anyMatch(lc -> "*".equals(lc.label));
    if (!hasAnyLabel) {
      return;
    }
    if (labelConfigs.size() > 1) {
      throw new IllegalArgumentException(
          "Label wildcard (\"*\") cannot be specified together with other label filters.");
    }
  }

  public static class LabelConfig {

    @Nonnull public String label;
    @Nonnull public List<String> properties;
    @Nullable public String filter;

    public LabelConfig(
        @Nonnull String label, @Nullable List<String> properties, @Nullable String filter) {
      this.label = label;
      this.filter = filter;
      this.properties = properties != null ? properties : new ArrayList<>();
    }

    private void validate(PropertyGraph graphSchema, boolean node) {
      if (label == null) {
        throw new IllegalArgumentException("label must be specified");
      }

      // Ensure label and properties exist in the graph
      if (label.equals("*")) {
        List<GraphElementTable> elementTables =
            node ? graphSchema.nodeTables : graphSchema.edgeTables;
        Set<String> availableProperties = new LinkedHashSet<>();
        elementTables.stream()
            .flatMap(t -> t.propertyDefinitions.stream())
            .map(d -> d.propertyDeclarationName)
            .forEach(availableProperties::add);
        for (String property : properties) {
          if (!availableProperties.contains(property)) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot find %s property %s in the graph schema. Existing properties: %s",
                    node ? "node" : "edge", property, availableProperties));
          }
        }
      } else {
        Set<String> availableProperties =
            new HashSet<>(
                graphSchema.labels.stream()
                    .filter(l -> l.name.equalsIgnoreCase(label))
                    .findFirst()
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                String.format("Cannot find label %s in the graph schema.", label)))
                    .propertyDeclarationNames);
        for (String property : properties) {
          if (!availableProperties.contains(property)) {
            throw new IllegalArgumentException(
                String.format("Cannot find property %s in label %s", property, label));
          }
        }
      }
    }
  }
}
