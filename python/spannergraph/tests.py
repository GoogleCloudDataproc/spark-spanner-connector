import os
import tempfile
import unittest
import logging

from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as sf

from ._connector import SpannerGraphConnector

from .tests_gold import *


logging.basicConfig(level=logging.INFO)


def get_connector_jar() -> str:
    jar_path = os.path.abspath(
        "../spark-3.1-spanner/target/spark-3.1-spanner-0.0.1-SNAPSHOT.jar"
    )
    assert os.path.exists(jar_path), (
        f"Cannot find connector JAR at {jar_path}. "
        "Please build the connector JAR first."
    )
    return jar_path


def _as_pandas_str(df: DataFrame) -> str:
    df_pd = df.toPandas()
    return df_pd.sort_values(by=df_pd.columns.to_list()) \
        .reset_index(drop=True).to_string()


# These tests rely on the graphs named FlexibleGraph and MusicGraph
# in the databases named flexible-graph and music-graph, respectively.
# See the following files for the definitions of the graph:
#   spark-3.1-spanner-lib/src/test/resources/db/populate_ddl_graph.sql
#   spark-3.1-spanner-lib/src/test/resources/db/insert_data_graph.sql


class TestGraphConnector(unittest.TestCase):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.check_point_dir = tempfile.TemporaryDirectory()
        self.spark = (
            SparkSession.builder.appName("spanner-spark-connector-test")
            .config(
                "spark.jars.packages",
                "graphframes:graphframes:0.8.4-spark3.5-s_2.12",
            )
            .config("spark.jars", get_connector_jar())
            .getOrCreate()
        )
        self.spark.sparkContext.setCheckpointDir(self.check_point_dir.name)

    def test_flexible_graph_cc(self) -> None:
        connector = (
            SpannerGraphConnector()
            .spark(self.spark)
            .project(os.getenv("SPANNER_PROJECT_ID"))
            .instance(os.getenv("SPANNER_INSTANCE_ID"))
            .database("flexible-graph")
            .graph("FlexibleGraph")
            .data_boost()
            .node_label("*", properties=["id"])
            .edge_label("*", properties=["to_id"],
                        where="id < 100 AND to_id < 100")
            .partition_size_bytes(1)  # hint only
            .repartition(3)
            .read_timestamp(datetime.now() - timedelta(minutes=10))
        )

        g = connector.load_graph()

        vertices_str = self._df_to_str(g.vertices)
        edges_str = self._df_to_str(g.edges)
        self.assertEqual(
            vertices_str,
            "['id', 'property_id'] - "
            + "[[1, 1], [2, 2], [3, 3], [7, 7], [16, 16], "
            + "[20, 20], [100, 100], [101, 101]]",
        )
        self.assertEqual(
            edges_str,
            "['src', 'dst', 'property_to_id'] - "
            + "[[1, 7, 7], [2, 20, 20], [3, 16, 16], "
            + "[7, 16, 16], [7, 16, 16], [16, 20, 20], "
            + "[20, 7, 7], [20, 16, 16]]",
        )

        cc = g.connectedComponents()
        cc_str = self._df_to_str(cc)
        self.assertEqual(
            cc_str,
            "['id', 'property_id', 'component'] - "
            + "[[1, 1, 1], [2, 2, 1], [3, 3, 1], [7, 7, 1], "
            + "[16, 16, 1], [20, 20, 1], [100, 100, 100], "
            + "[101, 101, 101]]",
        )

        self.assertEqual(g.vertices.rdd.getNumPartitions(), 3)
        self.assertEqual(g.edges.rdd.getNumPartitions(), 3)

    def test_music_graph_cc(self) -> None:
        connector = (
            SpannerGraphConnector()
            .spark(self.spark)
            .project(os.getenv("SPANNER_PROJECT_ID"))
            .instance(os.getenv("SPANNER_INSTANCE_ID"))
            .database("music-graph")
            .graph("MusicGraph")
            .data_boost(True)
            .repartition(6)
            .node_label(
                "*",
                properties=[
                    "name", "country_origin", "birthday", "ReleaseDate"
                ]
            )
            .edge_label(
                "*",
                properties=["SingerId", "release_date", "FriendId"]
            )
        )

        df_nodes, df_edges, df_mapping = connector.load_dfs()
        df_nodes = df_nodes.join(df_mapping, "id").drop("id")
        df_edges_src = df_edges \
            .join(df_mapping, sf.expr("src <=> id")).drop("id", "src", "dst")
        df_edges_dst = df_edges \
            .join(df_mapping, sf.expr("dst <=> id")).drop("id", "src", "dst")

        vertices_str = _as_pandas_str(df_nodes)
        edges_str_src = _as_pandas_str(df_edges_src)
        edges_str_dst = _as_pandas_str(df_edges_dst)

        self.assertEqual(vertices_str, TEST_MUSIC_GRAPH_CC_VERTICES)
        self.assertEqual(edges_str_src, TEST_MUSIC_GRAPH_CC_EDGES_SRC)
        self.assertEqual(edges_str_dst, TEST_MUSIC_GRAPH_CC_EDGES_DST)

    def test_flexible_graph_undirected(self) -> None:
        connector = (
            SpannerGraphConnector()
            .spark(self.spark)
            .project(os.getenv("SPANNER_PROJECT_ID"))
            .instance(os.getenv("SPANNER_INSTANCE_ID"))
            .database("flexible-graph")
            .graph("FlexibleGraph")
            .symmetrize_graph()
            .edge_label("*", properties=["to_id"])
            .repartition(7)
        )

        g = connector.load_graph()

        vertices_str = self._df_to_str(g.vertices)
        edges_str = self._df_to_str(g.edges)
        self.assertEqual(
            vertices_str,
            "['id'] - [[1], [2], [3], [7], [16], [20], [100], [101]]",
        )
        self.assertEqual(
            edges_str,
            "['src', 'dst', 'property_to_id'] - "
            "[[1, 7, 7], [2, 20, 20], [3, 16, 16], [7, 1, 7], [7, 16, 16], "
            "[7, 20, 7], [16, 3, 16], [16, 7, 16], [16, 20, 16], "
            "[16, 20, 20], [20, 2, 20], [20, 7, 7], [20, 16, 16], "
            "[20, 16, 20], [100, 101, 101], [101, 100, 101]]",
        )

        self.assertEqual(g.vertices.rdd.getNumPartitions(), 7)
        self.assertEqual(g.edges.rdd.getNumPartitions(), 7)

    def test_music_graph_direct_queries(self) -> None:
        node_query = "SELECT * FROM GRAPH_TABLE " \
                     "(MusicGraph MATCH (n:SINGER) RETURN n.id AS id)"
        edge_query = "SELECT * FROM GRAPH_TABLE " \
                     "(MusicGraph MATCH -[e:KNOWS]-> " \
                     "RETURN e.SingerId AS src, e.FriendId AS dst)"

        connector = (
            SpannerGraphConnector()
            .spark(self.spark)
            .project(os.getenv("SPANNER_PROJECT_ID"))
            .instance(os.getenv("SPANNER_INSTANCE_ID"))
            .database("music-graph")
            .graph("MusicGraph")
            .node_query(node_query)
            .edge_query(edge_query)
            .data_boost()
        )

        g = connector.load_graph()

        vertices_str = self._df_to_str(g.vertices)
        edges_str = self._df_to_str(g.edges)
        self.assertEqual(
            vertices_str,
            "['id'] - [[1], [2], [3], [4], [5]]",
        )
        self.assertEqual(
            edges_str,
            "['src', 'dst'] - "
            "[[1, 2], [1, 3], [2, 1], [2, 4], [2, 5], [3, 1], [3, 5], "
            "[4, 2], [4, 5], [5, 2], [5, 3], [5, 4]]",
        )

    def _df_to_str(self, df: DataFrame) -> str:
        rows = sorted([list(r.asDict().values()) for r in df.collect()])
        return f"{df.columns} - {rows}"


if __name__ == "__main__":
    unittest.main()
