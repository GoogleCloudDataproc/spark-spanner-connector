# pylint: disable=no-member

import json
import importlib
import logging

from datetime import datetime
from typing import Optional, Tuple, NamedTuple, Iterable
from typing_extensions import Self  # Self is not available until Python 3.11

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as sf

_KEY_COLUMN_PREFIX = "__spark_spanner_connector_internal_"
_ID_PREFIX = _KEY_COLUMN_PREFIX + "id_"
_SRC_PREFIX = _KEY_COLUMN_PREFIX + "src_"
_DST_PREFIX = _KEY_COLUMN_PREFIX + "dst_"

_LOGGER = logging.getLogger(__name__)


class _Label(NamedTuple):
    label: str
    properties: Optional[Iterable[str]]
    filter: Optional[str]


class SpannerGraphConnector(NamedTuple):
    """Exports Spanner Graphs as GraphFrames graphs.

    An example exporting the entire graph named "FlexibleGraph",
    and running Connected Components on the export graph in Spark:

        spark = (
            SparkSession.builder.appName("spanner-spark-connector")
            .config(
                "spark.jars.packages",
                "graphframes:graphframes:0.8.4-spark3.5-s_2.12",
            )
            .config("spark.jars", path_to_connector_jar)
            .getOrCreate()
        )
        spark.sparkContext.addPyFile(path_to_connector_jar)

        from spannergraph import SpannerGraphConnector

        connector = (
            SpannerGraphConnector()
            .spark(spark)
            .project("google-cloud-project-id")
            .instance("spanner-instance-id")
            .database("spanner-database-id")
            .graph("FlexibleGraph")
            .data_boost(True)
            .repartition(128)
            .read_timestamp(datetime.now() - timedelta(minutes=10))
        )

        g = connector.load()

        g.connectedComponents().show()
    """
    spark_: Optional[SparkSession] = None
    project_: Optional[str] = None
    instance_: Optional[str] = None
    database_: Optional[str] = None
    graph_: Optional[str] = None
    node_labels: Tuple[_Label] = ()
    edge_labels: Tuple[_Label] = ()
    enable_data_boost: bool = False
    partition_size_bytes_: Optional[int] = None
    repartition_count: Optional[int] = None
    read_timestamp_: Optional[datetime] = None
    symmetrize_graph_: bool = False
    node_query_: Optional[str] = None
    edge_query_: Optional[str] = None
    export_string_ids_: bool = False
    disable_direct_id_export_: bool = False  # For debugging only

    def spark(self, spark_session: SparkSession) -> Self:
        """Sets the Spark session"""
        return self._replace(spark_=spark_session)

    def project(self, project_id: str) -> Self:
        """Sets the Google Cloud project ID"""
        return self._replace(project_=project_id)

    def database(self, database_id: str) -> Self:
        """Sets the Spanner database ID"""
        return self._replace(database_=database_id)

    def instance(self, instance_id: str) -> Self:
        """Sets the Spanner instance ID"""
        return self._replace(instance_=instance_id)

    def graph(self, graph_name: str) -> Self:
        """Sets the name of the graph as defined in the database schema"""
        return self._replace(graph_=graph_name)

    def node_label(
        self,
        label: str,
        properties: Iterable[str] = (),
        where: Optional[str] = None,
    ) -> Self:
        """Includes nodes with the specified label, optionally specifies
        what element properties attached to the label should be exported,
        and optionally specifies an element-wise filter (a WHERE clause)
        for further filtering based on element properties.

        "*" can be used to match any labels. Cannot have other node label
        filters if a `"*"` edge node filter is provided.

        Example:
            connector = connector.node_label(
                "SINGER",
                properties=["birthday", "singer_name"],
                where="SingerId = 2"
            )
        """
        return self._replace(
            node_labels=self.node_labels
            + (_Label(label=label, properties=properties, filter=where),)
        )

    def edge_label(
        self,
        label: str,
        properties: Iterable[str] = (),
        where: Optional[str] = None,
    ) -> Self:
        """Includes edges with the specified label, optionally specifies
        what element properties attached to the label should be exported,
        and optionally specifies an element-wise filter (a WHERE clause)
        for further filtering based on element properties.

        "*" can be used to match any labels. Cannot have other edge label
        filters if a `"*"` edge label filter is provided.

        Example:
            connector = connector.edge_label(
                "SIGNED_BY",
                properties=["Date"],
                where="Date > '1990-01-01'"
            )
        """
        return self._replace(
            edge_labels=self.edge_labels
            + (_Label(label=label, properties=properties, filter=where),)
        )

    def data_boost(self, enable: bool = True) -> Self:
        """Sets if Data Boost should be enabled. For an overview of Data Boost,
        see https://cloud.google.com/spanner/docs/databoost/databoost-overview.
        """
        return self._replace(enable_data_boost=enable)

    def partition_size_bytes(self, partition_size_bytes_hint: int) -> Self:
        """Sets the partitionSizeBytes hint for Spanner. For more information
        on this option, see
        https://cloud.google.com/spanner/docs/reference/rest/v1/PartitionOptions.
        """
        assert partition_size_bytes_hint > 0, "partition size must be positive"
        return self._replace(partition_size_bytes_=partition_size_bytes_hint)

    def repartition(self, num_partitions: int) -> Self:
        """Repartitions node and edge DataFrames before constructing a graph
        with them."""
        assert num_partitions > 0, "num_partitions must be positive"
        return self._replace(repartition_count=num_partitions)

    def read_timestamp(self, snapshot_timestamp: datetime) -> Self:
        """Sets the timestamp of the snapshot to read from. By
        default the connector reads the snapshot at the time
        when load() is called."""
        return self._replace(read_timestamp_=snapshot_timestamp)

    def symmetrize_graph(self, enable: bool = True) -> Self:
        """Symmetries the output graph by adding reverse edges.
        Note that this will also remove any duplicated edges."""
        return self._replace(symmetrize_graph_=enable)

    def export_string_ids(self, enable: bool = True) -> Self:
        """When nodes require multiple columns to identify,
        exports node IDs as concatenations of the columns instead of
        condensing the columns to integers."""
        return self._replace(export_string_ids_=enable)

    def node_query(self, query: str) -> Self:
        """Specifies the query for fetching the node
        table. The query must be root-partitionable and have an
        "id" column for node IDs."""
        return self._replace(node_query_=query)

    def edge_query(self, query: str) -> Self:
        """Specifies the query for fetching the edge table. The query
        must be root-partitionable and have an "src" column for IDs
        of the source nodes and a "dst" column for the IDs of the
        destination nodes."""
        return self._replace(edge_query_=query)

    def load_dfs(self) -> \
            Tuple[DataFrame, DataFrame, Optional[DataFrame]]:
        """Exports the Spanner Graph as DataFrames.

        Returns:
            (1) a DataFrame for nodes,
            (2) a DataFrame for edges, and
            (3) a DataFrame used to map key columns and node table
            IDs to condensed long IDs, or None if such mapping was
            not needed.
        """
        df_nodes, df_edges = self._load_raw_dfs()

        df_nodes, df_edges, df_id_map = self._map_ids(df_nodes, df_edges)

        if self.repartition_count:
            df_nodes = df_nodes.repartition(self.repartition_count, "id")
            df_edges = df_edges.repartition(self.repartition_count, "src")
            if df_id_map:
                df_id_map.repartition(self.repartition_count, "id")

        if self.symmetrize_graph_:
            df_edges = self._symmetrize_graph(df_edges)

        return df_nodes, df_edges, df_id_map

    def load_graph_and_mapping(self) -> \
            Tuple["graphframes.GraphFrame", Optional[DataFrame]]:
        """Exports the Spanner Graph as a GraphFrames graph.

        Returns:
            graph_and_df_id_map: This method returns
            (1) a GraphFrame that represent the specified graph
            in Spanner, and
            (2) a DataFrame used to map key columns and node table
            IDs to condensed long IDs, or None if such mapping was
            not needed.
        """
        try:
            graphframes = importlib.import_module("graphframes")
        except ModuleNotFoundError as e:
            _LOGGER.fatal(
                "Cannot find package \"graphframes\". "
                "Is this package added to Spark?"
            )
            raise e
        df_nodes, df_edges, df_id_map = self.load_dfs()
        return graphframes.GraphFrame(df_nodes, df_edges), df_id_map

    def load_graph(self) -> "graphframes.GraphFrame":  # type: ignore
        """Exports the Spanner Graph as a GraphFrames graph."""
        return self.load_graph_and_mapping()[0]

    def _load_raw_dfs(self) -> Tuple[DataFrame, DataFrame]:
        self._validate()

        data_boost = "true" if self.enable_data_boost else "false"
        read_timestamp = self.read_timestamp_ \
            if self.read_timestamp_ else datetime.now()

        reader = (
            self.spark_.read.format("cloud-spanner")
            .option("projectId", self.project_)
            .option("instanceId", self.instance_)
            .option("databaseId", self.database_)
            .option("graph", self.graph_)
            .option("enableDataBoost", data_boost)
            .option("configs", self._config_str())
            .option("timestamp", read_timestamp.isoformat())
        )

        if self.node_query_:
            reader = reader.option("graphQuery", self.node_query_)
        df_nodes = reader.option("type", "node").load()

        if self.edge_query_:
            reader = reader.option("graphQuery", self.edge_query_)
        df_edges = reader.option("type", "edge").load()

        return df_nodes, df_edges

    def _config_str(self) -> str:
        configs = {
            "outputIndividualKeys": not self.export_string_ids_,
            "disableDirectIdExport": self.disable_direct_id_export_,
            "nodeLabelConfigs": [nl._asdict() for nl in self.node_labels],
            "edgeLabelConfigs": [el._asdict() for el in self.edge_labels],
        }
        if self.partition_size_bytes_:
            configs["partitionSizeBytes"] = self.partition_size_bytes_
        return json.dumps(configs)

    def _validate(self) -> None:
        for f in ["spark_", "project_", "instance_", "database_", "graph_"]:
            assert getattr(self, f), f"missing {f[0:-1]}"
        if self.node_query_ or self.edge_query_:
            assert self.node_query_ and self.edge_query_, \
                "node_query and edge_query must be set at the same time"
            assert not self.node_labels and not self.edge_labels, \
                "direct queries and label filters cannot be used " \
                "at the same time"

    def _map_ids(self, df_nodes: DataFrame, df_edges: DataFrame) -> \
            Tuple[DataFrame, DataFrame, Optional[DataFrame]]:
        if "id" in df_nodes.columns:
            _LOGGER.info("Found id column in df_nodes. "
                         "No need to map key columns to id.")
            return df_nodes, df_edges, None
        df_nodes_mapped, df_mapping = self._map_df_nodes(df_nodes)
        df_edges_mapped = self._map_df_edges(df_edges, df_mapping)
        return df_nodes_mapped, df_edges_mapped, df_mapping

    def _map_df_nodes(self, df_nodes: DataFrame) -> \
            Tuple[DataFrame, DataFrame]:
        _LOGGER.info("Mapping the node DataFrame.")
        key_columns, other_columns = [], []
        for c in df_nodes.columns:
            if c.startswith(_ID_PREFIX):
                key_columns.append(c)
            else:
                other_columns.append(c)

        df_nodes_with_id = df_nodes.withColumn(
            "id", sf.monotonically_increasing_id())
        df_mapping = df_nodes_with_id.select(["id"] + key_columns)
        df_nodes_mapped = df_nodes_with_id.select(["id"] + other_columns)
        return df_nodes_mapped, df_mapping

    def _map_df_edges(self, df_edges: DataFrame, df_mapping: DataFrame) -> \
            DataFrame:
        _LOGGER.info("Mapping the edge DataFrame.")
        src_key_columns, dst_key_columns, other_columns = [], [], []
        for c in df_edges.columns:
            if c.startswith(_SRC_PREFIX):
                src_key_columns.append(c)
            elif c.startswith(_DST_PREFIX):
                dst_key_columns.append(c)
            else:
                other_columns.append(c)

        src_join_exp = [sf.expr(f"{c} <=> {_ID_PREFIX + c[len(_SRC_PREFIX):]}")
                        for c in src_key_columns]
        dst_join_exp = [sf.expr(f"{c} <=> {_ID_PREFIX + c[len(_DST_PREFIX):]}")
                        for c in dst_key_columns]

        return df_edges \
            .join(df_mapping, src_join_exp, "left") \
            .withColumnRenamed("id", "src") \
            .drop(*(src_key_columns + df_mapping.columns)) \
            .join(df_mapping, dst_join_exp, "left") \
            .withColumnRenamed("id", "dst") \
            .drop(*(dst_key_columns + df_mapping.columns)) \
            .select(["src", "dst"] + other_columns)

    def _symmetrize_graph(self, df_edges: DataFrame) -> DataFrame:
        other_columns = [
            c for c in df_edges.columns if c != "src" and c != "dst"
        ]
        df_edges_reversed = df_edges.selectExpr(
            "dst as src", "src as dst", *other_columns
        )
        undirected = df_edges \
            .unionByName(df_edges_reversed) \
            .distinct()
        if self.repartition_count:
            undirected = undirected.repartition(self.repartition_count, "src")
        return undirected
