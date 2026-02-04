package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.sql.DataFrameReader;
import org.junit.Assert;
import org.junit.Test;

<<<<<<<< HEAD:spark-3.1-spanner-lib/src/test/java/com/google/cloud/spark/spanner/graph/GraphErrorHandlingIntegrationTestBase.java
public abstract class GraphErrorHandlingIntegrationTestBase extends GraphReadIntegrationTestBase {
========
public class GraphErrorHandlingIntegrationTest extends GraphReadIntegrationTestBase {
>>>>>>>> main:spark-3.1-spanner-lib/src/test/java/com/google/cloud/spark/spanner/graph/GraphErrorHandlingIntegrationTest.java

  @Test
  public void testDirectQueryNonRootPartitionable() {
    String nodeQuery =
        "SELECT * FROM GRAPH_TABLE (MusicGraph MATCH (n:SINGER|ALBUM) RETURN n.id AS id)";
    DataFrameReader reader =
        musicGraphReader(null).option("graphQuery", nodeQuery).option("type", "node");
    Exception e = Assert.assertThrows(Exception.class, reader::load);
    Assert.assertTrue(e.getMessage().contains("root-partitionable"));
  }

  @Test
  public void testDirectQueryNoId() {
    String nodeQuery =
        "SELECT * FROM GRAPH_TABLE (MusicGraph MATCH (n:SINGER) RETURN n.id AS no_id)";
    DataFrameReader reader =
        musicGraphReader(null).option("graphQuery", nodeQuery).option("type", "node");
    Exception e = Assert.assertThrows(IllegalArgumentException.class, reader::load);
    Assert.assertTrue(e.getMessage().contains("id missing"));
  }

  @Test
  public void testWildcardLabelMixedWithOtherLabel() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(new LabelConfig("*", Collections.emptyList(), null));
    configs.nodeLabelConfigs.add(new LabelConfig("SINGER", Collections.emptyList(), null));
    configs.edgeLabelConfigs.add(new LabelConfig("*", Collections.emptyList(), null));
    configs.edgeLabelConfigs.add(new LabelConfig("KNOWN", Collections.emptyList(), null));

    DataFrameReader reader = musicGraphReader(null).option("configs", new Gson().toJson(configs));

    Assert.assertThrows(IllegalArgumentException.class, () -> reader.option("type", "node").load());
    Assert.assertThrows(IllegalArgumentException.class, () -> reader.option("type", "edge").load());
  }

  @Test
  public void testEdgeReferencingFilteredOutNodes() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(new LabelConfig("SINGER", Collections.emptyList(), null));
    Assert.assertThrows(IllegalArgumentException.class, () -> readEdges(musicGraphReader(configs)));
  }

  private void testNonExistentProperties(Collection<LabelConfig> labels, boolean node) {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    if (node) {
      configs.nodeLabelConfigs.addAll(labels);
    } else {
      configs.edgeLabelConfigs.addAll(labels);
    }
    Exception e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> musicGraphReader(configs).option("type", node ? "node" : "edge").load());
    Assert.assertTrue(e.getMessage().contains("property"));
  }

  @Test
  public void testNonExistentProperties() {
    testNonExistentProperties(
        ImmutableList.of(new LabelConfig("*", ImmutableList.of("FriendId"), null)), true);
    testNonExistentProperties(
        ImmutableList.of(new LabelConfig("*", ImmutableList.of("AlbumTitle"), null)), false);
    testNonExistentProperties(
        ImmutableList.of(new LabelConfig("SINGER", ImmutableList.of("album_id"), null)), true);
  }
}
