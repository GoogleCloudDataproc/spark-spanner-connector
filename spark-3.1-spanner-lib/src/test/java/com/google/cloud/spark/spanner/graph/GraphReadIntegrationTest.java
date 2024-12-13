package com.google.cloud.spark.spanner.graph;

import com.google.cloud.spark.spanner.graph.SpannerGraphConfigs.LabelConfig;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

public class GraphReadIntegrationTest extends GraphReadIntegrationTestBase {

  @Test
  public void testFlexibleGraphConfigs() {
    Dataset<Row> nodesDf = readNodes(flexibleGraphReader(null));
    Dataset<Row> edgesDf = readEdges(flexibleGraphReader(null));
    Assert.assertArrayEquals(new String[] {"id"}, nodesDf.columns());
    Assert.assertArrayEquals(new String[] {"src", "dst"}, edgesDf.columns());

    String nodesString = nodesDf.orderBy("id").collectAsList().toString();
    Assert.assertEquals("[[1], [2], [3], [7], [16], [20], [100], [101]]", nodesString);

    String edgesString = edgesDf.orderBy("src", "dst").collectAsList().toString();
    Assert.assertEquals(
        "[[1,7], [2,20], [3,16], [7,16], [7,16], [16,20], [20,7], [20,16], [100,101]]",
        edgesString);
  }

  @Test
  public void testMusicGraphDirectQueries() {
    String nodeQuery = "SELECT * FROM GRAPH_TABLE (MusicGraph MATCH (n:SINGER) RETURN n.id AS id)";
    String edgeQuery =
        "SELECT * FROM GRAPH_TABLE (MusicGraph MATCH -[e:KNOWS]-> RETURN e.SingerId AS src, e.FriendId AS dst)";
    Dataset<Row> nodesDf = readNodes(musicGraphReader(null).option("graphQuery", nodeQuery));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(null).option("graphQuery", edgeQuery));
    Assert.assertArrayEquals(new String[] {"id"}, nodesDf.columns());
    Assert.assertArrayEquals(new String[] {"src", "dst"}, edgesDf.columns());

    String nodesString = nodesDf.orderBy("id").collectAsList().toString();
    Assert.assertEquals("[[1], [2], [3], [4], [5]]", nodesString);

    String edgesString = edgesDf.orderBy("src", "dst").collectAsList().toString();
    Assert.assertEquals(
        "[[1,2], [1,3], [2,1], [2,4], [2,5], [3,1], [3,5], [4,2], [4,5], [5,2], [5,3], [5,4]]",
        edgesString);
  }

  @Test
  public void testMusicGraph() {
    Dataset<Row> nodesDf = readNodes(musicGraphReader(new SpannerGraphConfigs()));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(new SpannerGraphConfigs()));
    Assert.assertArrayEquals(new String[] {"id"}, nodesDf.columns());
    Assert.assertArrayEquals(new String[] {"src", "dst"}, edgesDf.columns());

    String expectedNodes =
        "[[1@1|1], [1@2|1], [1@3|2], [1@4|2], [1@5|3], "
            + "[1@6|4], [1@7|5], [2@1], [2@2], [2@3], [2@4], "
            + "[2@5], [3@1], [3@2], [3@3], [3@4], [3@5]]";

    Assert.assertEquals(expectedNodes, nodesDf.orderBy("id").collectAsList().toString());

    String expectedEdges =
        "[[2@1,1@1|1], [2@1,1@2|1], [2@2,1@3|2], "
            + "[2@3,1@4|2], [2@3,1@5|3], [2@4,1@6|4], "
            + "[2@5,1@7|5], [3@1,1@1|1], [3@1,1@2|1], "
            + "[3@1,2@4], [3@1,3@2], [3@1,3@3], "
            + "[3@2,1@3|2], [3@2,1@4|2], [3@2,2@2], "
            + "[3@2,3@1], [3@2,3@4], [3@2,3@5], "
            + "[3@3,1@5|3], [3@3,2@5], [3@3,3@1], "
            + "[3@3,3@5], [3@4,1@6|4], [3@4,2@1], "
            + "[3@4,3@2], [3@4,3@5], [3@5,1@7|5], "
            + "[3@5,2@3], [3@5,3@2], [3@5,3@3], "
            + "[3@5,3@4]]";
    Assert.assertEquals(expectedEdges, edgesDf.orderBy("src", "dst").collectAsList().toString());
  }

  @Test
  public void testMusicGraphWithLabelFilterUpperCase() {
    testMusicGraphWithLabelFilter(true);
  }

  @Test
  public void testMusicGraphWithLabelFilterLowerCase() {
    testMusicGraphWithLabelFilter(false);
  }

  public void testMusicGraphWithLabelFilter(boolean upperCase) {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(new LabelConfig(upperCase ? "SINGER" : "singer", null, null));
    configs.edgeLabelConfigs.add(new LabelConfig(upperCase ? "KNOWS" : "knows", null, null));
    Dataset<Row> nodesDf = readNodes(musicGraphReader(configs));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(configs));
    Assert.assertArrayEquals(new String[] {"id"}, nodesDf.columns());
    Assert.assertArrayEquals(new String[] {"src", "dst"}, edgesDf.columns());

    Assert.assertEquals(
        "[[1], [2], [3], [4], [5]]", nodesDf.orderBy("id").collectAsList().toString());

    String expectedEdges =
        "[[1,2], [1,3], [2,1], [2,4], [2,5], [3,1], [3,5], [4,2], [4,5], [5,2], " + "[5,3], [5,4]]";

    Assert.assertEquals(expectedEdges, edgesDf.orderBy("src", "dst").collectAsList().toString());
  }

  @Test
  public void testMusicGraphAnyLabelWithProperties() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(new LabelConfig("*", Collections.singletonList("name"), null));
    configs.edgeLabelConfigs.add(
        new LabelConfig("*", Collections.singletonList("release_date"), null));
    Dataset<Row> nodesDf = readNodes(musicGraphReader(configs));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(configs));
    Assert.assertArrayEquals(new String[] {"id", "property_name"}, nodesDf.columns());
    Assert.assertArrayEquals(
        new String[] {"src", "dst", "property_release_date"}, edgesDf.columns());

    String expectedNodes =
        "[[1@1|1,null], [1@2|1,null], [1@3|2,null], "
            + "[1@4|2,null], [1@5|3,null], [1@6|4,null], [1@7|5,null], "
            + "[2@1,Mellow Wave], [2@2,Rolling Stow], [2@3,Picky Penang], "
            + "[2@4,Ice Ice], [2@5,Oint Is Not An Ink], [3@1,Cruz Richards], "
            + "[3@2,Tristan Smith], [3@3,Izumi Trentor], [3@4,Ira Martin], "
            + "[3@5,Mahan Lomond]]";
    Assert.assertEquals(expectedNodes, nodesDf.orderBy("id").collectAsList().toString());

    String expectedEdges =
        "[[2@1,1@1|1,2014-03-02], [2@1,1@2|1,2011-02-09], "
            + "[2@2,1@3|2,2012-09-17], [2@3,1@4|2,2010-10-15], "
            + "[2@3,1@5|3,2008-06-07], [2@4,1@6|4,2014-04-29], "
            + "[2@5,1@7|5,2013-12-21], [3@1,1@1|1,2014-03-02], "
            + "[3@1,1@2|1,2011-02-09], [3@1,2@4,null], [3@1,3@2,null], "
            + "[3@1,3@3,null], [3@2,1@3|2,2012-09-17], "
            + "[3@2,1@4|2,2010-10-15], [3@2,2@2,null], [3@2,3@1,null], "
            + "[3@2,3@4,null], [3@2,3@5,null], [3@3,1@5|3,2008-06-07], "
            + "[3@3,2@5,null], [3@3,3@1,null], [3@3,3@5,null], "
            + "[3@4,1@6|4,2014-04-29], [3@4,2@1,null], [3@4,3@2,null], "
            + "[3@4,3@5,null], [3@5,1@7|5,2013-12-21], [3@5,2@3,null], "
            + "[3@5,3@2,null], [3@5,3@3,null], [3@5,3@4,null]]";
    Assert.assertEquals(expectedEdges, edgesDf.orderBy("src", "dst").collectAsList().toString());
  }

  @Test
  public void testMusicGraphExportKeyColumns() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.outputIndividualKeys = true;
    configs.nodeLabelConfigs.add(new LabelConfig("*", Collections.singletonList("name"), null));
    configs.edgeLabelConfigs.add(
        new LabelConfig("*", Collections.singletonList("release_date"), null));

    Dataset<Row> nodesDf = readNodes(musicGraphReader(configs));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(configs));

    String[] expectedNodesColumns =
        new String[] {
          "__spark_spanner_connector_internal_id_node_table",
          "__spark_spanner_connector_internal_id_key_column_AlbumId",
          "__spark_spanner_connector_internal_id_key_column_CompanyId",
          "__spark_spanner_connector_internal_id_key_column_SingerId",
          "property_name"
        };
    String[] expectedEdgesColumns =
        new String[] {
          "__spark_spanner_connector_internal_src_node_table",
          "__spark_spanner_connector_internal_src_key_column_CompanyId",
          "__spark_spanner_connector_internal_src_key_column_SingerId",
          "__spark_spanner_connector_internal_dst_node_table",
          "__spark_spanner_connector_internal_dst_key_column_AlbumId",
          "__spark_spanner_connector_internal_dst_key_column_CompanyId",
          "__spark_spanner_connector_internal_dst_key_column_SingerId",
          "property_release_date"
        };

    Assert.assertArrayEquals(expectedNodesColumns, nodesDf.columns());
    Assert.assertArrayEquals(expectedEdgesColumns, edgesDf.columns());

    String expectedNodes =
        "[[1,1,null,1,null], [1,2,null,1,null], [1,3,null,2,null], [1,4,null,2,null], "
            + "[1,5,null,3,null], [1,6,null,4,null], [1,7,null,5,null], "
            + "[2,null,1,null,Mellow Wave], [2,null,2,null,Rolling Stow], "
            + "[2,null,3,null,Picky Penang], [2,null,4,null,Ice Ice], "
            + "[2,null,5,null,Oint Is Not An Ink], [3,null,null,1,Cruz Richards], "
            + "[3,null,null,2,Tristan Smith], [3,null,null,3,Izumi Trentor], "
            + "[3,null,null,4,Ira Martin], [3,null,null,5,Mahan Lomond]]";
    Assert.assertEquals(expectedNodes, nodesDf.collectAsList().toString());

    String expectedEdges =
        "[[2,1,null,1,1,null,1,2014-03-02], [2,1,null,1,2,null,1,2011-02-09], "
            + "[2,2,null,1,3,null,2,2012-09-17], [2,3,null,1,4,null,2,2010-10-15], "
            + "[2,3,null,1,5,null,3,2008-06-07], [2,4,null,1,6,null,4,2014-04-29], "
            + "[2,5,null,1,7,null,5,2013-12-21], [3,null,1,1,1,null,1,2014-03-02], "
            + "[3,null,1,1,2,null,1,2011-02-09], [3,null,2,1,3,null,2,2012-09-17], "
            + "[3,null,2,1,4,null,2,2010-10-15], [3,null,3,1,5,null,3,2008-06-07], "
            + "[3,null,4,1,6,null,4,2014-04-29], [3,null,5,1,7,null,5,2013-12-21], "
            + "[3,null,1,3,null,null,2,null], [3,null,1,3,null,null,3,null], "
            + "[3,null,2,3,null,null,1,null], [3,null,2,3,null,null,4,null], "
            + "[3,null,2,3,null,null,5,null], [3,null,3,3,null,null,1,null], "
            + "[3,null,3,3,null,null,5,null], [3,null,4,3,null,null,2,null], "
            + "[3,null,4,3,null,null,5,null], [3,null,5,3,null,null,2,null], "
            + "[3,null,5,3,null,null,3,null], [3,null,5,3,null,null,4,null], "
            + "[3,null,1,2,null,4,null,null], [3,null,2,2,null,2,null,null], "
            + "[3,null,3,2,null,5,null,null], [3,null,4,2,null,1,null,null], "
            + "[3,null,5,2,null,3,null,null]]";
    Assert.assertEquals(expectedEdges, edgesDf.collectAsList().toString());
  }

  @Test
  public void testMusicGraphFriendsExportKeyColumns() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.outputIndividualKeys = true;
    configs.nodeLabelConfigs.add(
        new LabelConfig("SINGER", Collections.singletonList("singer_name"), null));
    configs.edgeLabelConfigs.add(new LabelConfig("KNOWS", null, null));

    Dataset<Row> nodesDf = readNodes(musicGraphReader(configs));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(configs));

    String[] expectedNodesColumns = new String[] {"id", "property_singer_name"};
    String[] expectedEdgesColumns = new String[] {"src", "dst"};

    Assert.assertArrayEquals(expectedNodesColumns, nodesDf.columns());
    Assert.assertArrayEquals(expectedEdgesColumns, edgesDf.columns());

    String expectedNodes =
        "[[1,Cruz Richards], [2,Tristan Smith], [3,Izumi Trentor], "
            + "[4,Ira Martin], [5,Mahan Lomond]]";
    Assert.assertEquals(expectedNodes, nodesDf.collectAsList().toString());

    String expectedEdges =
        "[[1,2], [1,3], [2,1], [2,4], [2,5], [3,1], [3,5], [4,2], [4,5], [5,2], " + "[5,3], [5,4]]";
    Assert.assertEquals(expectedEdges, edgesDf.collectAsList().toString());
  }

  @Test
  public void testMusicGraphNodeFilter() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.outputIndividualKeys = true;
    configs.nodeLabelConfigs.add(
        new LabelConfig("SINGER", ImmutableList.of("singer_name"), "birthday > '1990-01-01'"));
    Dataset<Row> nodesDf = readNodes(musicGraphReader(configs));

    String[] expectedNodesColumns = new String[] {"id", "property_singer_name"};
    Assert.assertArrayEquals(expectedNodesColumns, nodesDf.columns());

    String expectedNodes = "[[2,Tristan Smith], [3,Izumi Trentor], [4,Ira Martin]]";
    Assert.assertEquals(expectedNodes, nodesDf.collectAsList().toString());
  }

  @Test
  public void testMusicGraphEdgeFilter() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.outputIndividualKeys = true;
    configs.edgeLabelConfigs.add(
        new LabelConfig("CREATES_MUSIC", null, "release_date > '2011-01-01'"));
    Dataset<Row> edgesDf = readEdges(musicGraphReader(configs));

    String[] expectedEdgesColumns =
        new String[] {
          "__spark_spanner_connector_internal_src_node_table",
          "__spark_spanner_connector_internal_src_key_column_CompanyId",
          "__spark_spanner_connector_internal_src_key_column_SingerId",
          "__spark_spanner_connector_internal_dst_node_table",
          "__spark_spanner_connector_internal_dst_key_column_AlbumId",
          "__spark_spanner_connector_internal_dst_key_column_SingerId"
        };
    Assert.assertArrayEquals(expectedEdgesColumns, edgesDf.columns());

    String expectedEdges =
        "[[2,1,null,1,1,1], [2,1,null,1,2,1], [2,2,null,1,3,2], "
            + "[2,4,null,1,6,4], [2,5,null,1,7,5], [3,null,1,1,1,1], [3,null,1,1,2,1], "
            + "[3,null,2,1,3,2], [3,null,4,1,6,4], [3,null,5,1,7,5]]";
    Assert.assertEquals(expectedEdges, edgesDf.collectAsList().toString());
  }

  @Test
  public void testFlexibleGraphEdgeFilter() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.edgeLabelConfigs.add(
        new LabelConfig("*", ImmutableList.of("to_id"), "id < 100 AND to_id < 100"));
    Dataset<Row> edgesDf = readEdges(flexibleGraphReader(configs));

    String[] expectedEdgeColumns = new String[] {"src", "dst", "property_to_id"};
    Assert.assertArrayEquals(expectedEdgeColumns, edgesDf.columns());

    String expectedEdges =
        "[[1,7,7], [20,7,7], [3,16,16], [7,16,16], [7,16,16], [20,16,16], "
            + "[2,20,20], [16,20,20]]";
    Assert.assertEquals(expectedEdges, edgesDf.collectAsList().toString());
  }

  @Test
  public void testFlexibleGraphExportPropertyId() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(new LabelConfig("Node", ImmutableList.of("id"), null));
    Dataset<Row> nodesDf = readNodes(flexibleGraphReader(configs));

    String[] expectedNodeColumns = new String[] {"id", "property_id"};
    Assert.assertArrayEquals(expectedNodeColumns, nodesDf.columns());

    String expectedNodes = "[[7,7], [16,16], [20,20], [100,100], [101,101], [1,1], [2,2], [3,3]]";
    Assert.assertEquals(expectedNodes, nodesDf.collectAsList().toString());
  }

  @Test
  public void testMusicGraphPreservePropertyOrdersOne() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(
        new LabelConfig("SINGER", ImmutableList.of("birthday", "singer_name"), null));
    Assert.assertArrayEquals(
        new String[] {"id", "property_birthday", "property_singer_name"},
        readNodes(musicGraphReader(configs)).columns());
  }

  @Test
  public void testMusicGraphPreservePropertyOrdersTwo() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(
        new LabelConfig("SINGER", ImmutableList.of("singer_name", "birthday"), null));
    Assert.assertArrayEquals(
        new String[] {"id", "property_singer_name", "property_birthday"},
        readNodes(musicGraphReader(configs)).columns());
  }

  @Test
  public void testMusicGraphPreservePropertyOrdersThree() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(
        new LabelConfig(
            "*", ImmutableList.of("name", "country_origin", "birthday", "ReleaseDate"), null));
    Assert.assertArrayEquals(
        new String[] {
          "id",
          "property_name",
          "property_country_origin",
          "property_birthday",
          "property_ReleaseDate"
        },
        readNodes(musicGraphReader(configs)).columns());
  }

  @Test
  public void testMusicGraphPreservePropertyOrdersFour() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.edgeLabelConfigs.add(
        new LabelConfig("*", ImmutableList.of("SingerId", "release_date", "FriendId"), null));
    Assert.assertArrayEquals(
        new String[] {
          "src", "dst", "property_SingerId", "property_release_date", "property_FriendId"
        },
        readEdges(musicGraphReader(configs)).columns());
  }

  @Test
  public void testPruneColumnForDirectQuery() {
    String edgeQuery =
        "SELECT * FROM GRAPH_TABLE "
            + "(MusicGraph MATCH -[e:SIGNED_BY]-> RETURN e.CompanyId AS dst, e.SingerId AS src)";
    Dataset<Row> edgeDf = readEdges(musicGraphReader(null).option("graphQuery", edgeQuery));
    Assert.assertArrayEquals(new String[] {"dst", "src"}, edgeDf.columns());
    Dataset<Row> edgeDfPruned = edgeDf.select("src");
    Assert.assertEquals("[[1], [2], [3], [4], [5]]", edgeDfPruned.collectAsList().toString());
  }

  @Test
  public void testPruneColumnForNonDirectQuery() {
    SpannerGraphConfigs configs = new SpannerGraphConfigs();
    configs.nodeLabelConfigs.add(
        new LabelConfig("SINGER", ImmutableList.of("birthday", "singer_name"), null));
    Dataset<Row> nodeDf = readNodes(musicGraphReader(configs));
    Dataset<Row> nodeDfPruned = nodeDf.select("property_birthday");

    String expectedNodes = "[[1970-09-03], [1990-08-17], [1991-10-02], [1991-11-09], [1977-01-29]]";
    Assert.assertEquals(expectedNodes, nodeDfPruned.collectAsList().toString());
  }

  @Test
  public void testEmptyPruneColumnForNonDirectQuery() {
    Dataset<Row> nodesDf = readNodes(musicGraphReader(null));
    Dataset<Row> nodesDfNoColumn = nodesDf.select();

    String expectedNodes = "[[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []]";
    Assert.assertEquals(expectedNodes, nodesDfNoColumn.collectAsList().toString());
    Assert.assertEquals(17, nodesDfNoColumn.count());
  }

  @Test
  public void testEmptyPruneColumnForDirectQuery() {
    String nodeQuery = "SELECT * FROM GRAPH_TABLE (MusicGraph MATCH (n:SINGER) RETURN n.id AS id)";
    Dataset<Row> nodesDf = readNodes(musicGraphReader(null).option("graphQuery", nodeQuery));
    Dataset<Row> nodesDfNoColumn = nodesDf.select();

    Assert.assertEquals("[[], [], [], [], []]", nodesDfNoColumn.collectAsList().toString());
    Assert.assertEquals(5, nodesDfNoColumn.count());
  }
}
