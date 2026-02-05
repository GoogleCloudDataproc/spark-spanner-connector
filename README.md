# Apache Spark SQL Connector for Google Cloud Spanner

The connector supports reading
[Google Cloud Spanner](https://cloud.google.com/spanner) tables and
[graphs](https://cloud.google.com/spanner/docs/graph/overview) into Spark
[DataFrames](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
and
[GraphFrames](https://graphframes.github.io/graphframes/docs/_site/user-guide.html).

## Requirements

### Enable the Cloud Spanner API
Follow the [instructions](https://cloud.google.com/spanner/docs/create-query-database-console) to create a project or Spanner table if you don't have an existing one.

### Create a Google Cloud Dataproc cluster (Optional)

If you do not have an Apache Spark environment you can create a Cloud Dataproc cluster with pre-configured auth. The following examples assume you are using Cloud Dataproc, but you can use `spark-submit` on any cluster.

Any Dataproc cluster using the API needs the 'Spanner' or 'cloud-platform' [scopes](https://developers.google.com/identity/protocols/oauth2/scopes#spanner). Dataproc clusters don't have the 'spanner' scope by default, but you can create a cluster with the scope. For example:

```
MY_CLUSTER=...
gcloud dataproc clusters create "$MY_CLUSTER" --scopes https://www.googleapis.com/auth/cloud-platform
```

### Permission

If you run a Spark job on the Dataproc cluster, you'll have to assign corresponding [Spanner permission](https://cloud.google.com/spanner/docs/iam#permissions) to the [Dataproc VM service account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#dataproc_service_accounts_2). If you choose to use Dataproc Serverless, you'll have to make sure the [Serverless service account](https://cloud.google.com/dataproc-serverless/docs/concepts/service-account#console) has the permission.

## Downloading and Using the Connector

You can find the released jar file from the Releases tag on right of the github page. The name pattern is spark-3.1-spanner-x.x.x.jar. The 3.1 indicates the driver depends on the Spark 3.1 and x.x.x is the Spark Spanner connector version. The alternative way is to use `gs://spark-lib/spanner/spark-3.1-spanner-1.2.2.jar` directly.

### Connector to Spark Compatibility Matrix
| Connector \ Spark | 2.3     | 2.4<br>(Scala 2.11) | 2.4<br>(Scala 2.12) | 3.0     | 3.1     | 3.2     | 3.3     | 3.4     | 3.5     |
|-------------------|---------|---------------------|---------------------|---------|---------|---------|---------|---------|---------|
| spark-3.1-spanner |         |                     |                     |         | &check; | &check; | &check; | &check; | &check; |
| spark-3.2-spanner |         |                     |                     |         |         | &check; | &check; | &check; | &check; |
| spark-3.3-spanner |         |                     |                     |         |         |         | &check; | &check; | &check; |
| spark-3.5-spanner |         |                     |                     |         |         |         |         |         | &check; |

### Connector to Dataproc Image Compatibility Matrix
| Connector \ Dataproc Image | 1.3     | 1.4     | 1.5     | 2.0     | 2.1     | 2.2     | Serverless<br>Image 1.1 | Serverless<br>Image 1.2 | Serverless<br>Image 2.0 | Serverless<br>Image 2.1 | Serverless<br>Image 2.2 |
|----------------------------|---------|---------|---------|---------|---------|---------|-------------------------|-------------------------|-------------------------|-------------------------|-------------------------|
| spark-3.1-spanner          |         |         |         | &check; | &check; | &check; | &check;                 | Note 1                  | &check;                 | &check;                 | Note 1                  |
| spark-3.2-spanner          |         |         |         | &check; | &check; | &check; | &check;                 | Note 1                  | &check;                 | &check;                 | Note 1                  |
| spark-3.3-spanner          |         |         |         | &check; | &check; | &check; | &check;                 | Note 1                  | &check;                 | &check;                 | Note 1                  |
| spark-3.5-spanner          |         |         |         |         |         | &check; |                         | &check;                 |                         |                         | &check;                 |

Note 1: Dataproc compatibility to be tested.

### Maven / Ivy Package

The connector is also available from the
[Maven Central](https://repo1.maven.org/maven2/com/google/cloud/spark/spanner/)
repository. It can be used using the `--packages` option or the
`spark.jars.packages` configuration property. Use the following value

| version    | Connector Artifact                                                                 |
|------------|------------------------------------------------------------------------------------|
| Spark 3.5  | `com.google.cloud.spark.spanner:spark-3.5-spanner:1.2.2`                    |
| Spark 3.3  | `com.google.cloud.spark.spanner:spark-3.3-spanner:1.2.2`                    |
| Spark 3.2  | `com.google.cloud.spark.spanner:spark-3.2-spanner:1.2.2`                    |
| Spark 3.1  | `com.google.cloud.spark.spanner:spark-3.1-spanner:1.2.2`                    |

### Specifying the Spark Spanner connector version in a Dataproc cluster

You can use the standard `--jars` or `--packages` (or alternatively, the `spark.jars`/`spark.jars.packages` configuration) to specify the Spark Spanner connector. For example:

```shell
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
    --jars=gs://spark-lib/spanner/spark-3.1-spanner-1.2.2.jar \
    --region us-central1 examples/SpannerSpark.py
```
## Usage

The connector supports exporting both tables and graphs from Spanner, and importing to Spanner (Preview).
It uses the cross language
[Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-data-sources.html)
to communicate with the
[Spanner Java library](https://github.com/googleapis/java-spanner).

### Exporting Spanner Tables
This is an example of using Python code to connect to a Spanner table. You can find more examples or documentations on the [usage](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html).

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spanner Connect App').getOrCreate()
df = spark.read.format('cloud-spanner') \
   .option("projectId", "$YourProjectId") \
   .option("instanceId", "$YourInstanceId") \
   .option("databaseId", "$YourDatabaseId") \
   .option("table", "$YourTable") \
   .load()
df.show()
```

For support of other languages, you can refer to
[Scala](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html),
[Java](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/Dataset.html),
and
[R](https://spark.apache.org/docs/latest/api/R/reference/SparkDataFrame.html).
You can also refer to
[Scala, Java](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark),
and
[R](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark-r)
about how to submit a job for other languages.

#### Table Connector Options

Here are the options supported in the Spark Spanner connector for reading
tables.

Variable|Validation|Comments
---|---|---
projectId|String|The projectID containing the Cloud Spanner database
instanceId|String|The instanceID of the Cloud Spanner database
databaseId|String|The databaseID of the Cloud Spanner database
table|String|The Table of the Cloud Spanner database that you are reading from
enableDataboost|Boolean|Enable the [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview), which provides independent compute resources to query Spanner with near-zero impact to existing workloads. Note the option may trigger [extra charge](https://cloud.google.com/spanner/pricing#spanner-data-boost-pricing).

### Writing to Spanner Tables (Preview)
> Note: Write support is a preview feature. Only "append" save mode is supported.

Here is an example of using Python to write to a Spanner table.
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spanner Write App').getOrCreate()

columns = ['id', 'name', 'email']
data = [(1, 'John Doe', 'john.doe@example.com'), (2, 'Jane Doe', 'jane.doe@example.com')]
df = spark.createDataFrame(data, columns)

df.write.format('cloud-spanner') \
   .option("projectId", "$YourProjectId") \
   .option("instanceId", "$YourInstanceId") \
   .option("databaseId", "$YourDatabaseId") \
   .option("table", "$YourTable") \
   .mode("append") \
   .save()
```

These are the options supported in the Spark Spanner connector for writing
tables.

Variable| Validation |Comments
---|------------|---
projectId| String     |The projectID containing the Cloud Spanner database
instanceId| String     |The instanceID of the Cloud Spanner database
databaseId| String     |The databaseID of the Cloud Spanner database
table| String     |The name of the destination Cloud Spanner table
mutationsPerTransaction| Integer    |The number of mutations to send in a single transaction. Default: 1000
bytesPerTransaction | Long |Maximum size of each transaction. Default: 1048576 (1MB)
numWriteThreads| Integer    |The number of threads to use for writing per Spark worker.  Default: 8
assumeIdempotentRows| Boolean    |When `true`, the connector uses a higher-throughput 'at-least-once' write mode. See [Spanner documentation](https://docs.cloud.google.com/spanner/docs/batch-write) for use cases and limitations. Default: `false`
maxPendingTransactions| Integer    |The maximum number of concurrent batches that can be in-flight. This is used to control backpressure. Default: 20

`mutationsPerTransaction` and `bytesPerTransaction` are both used when building a transaction to send to spanner.


#### Data Types
The connector supports writing the following Spark data types to Spanner.

##### GoogleSQL
Spark Data Type|Spanner GoogleSql Type
---|---
`LongType`|`INT64`
`StringType`|`STRING`
`BooleanType`|`BOOL`
`DoubleType`|`FLOAT64`
`BinaryType`|`BYTES`
`TimestampType`|`TIMESTAMP`
`DateType`|`DATE`
`DecimalType`|`NUMERIC`

##### PostgreSQL
Spark Data Type|Spanner PostgreSql Type
---|---
`LongType`|`bigint`/`int8`
`StringType`|`varchar`/`text`/`character varying`
`BooleanType`|`bool`/`boolean`
`DoubleType`|`double precision`/`float8`
`BinaryType`|`bytea`
`TimestampType`|`timestamptz`/`timestamp with time zone`
`DateType`|`date`
`DecimalType`|`numeric`/`decimal`

> Note: `ArrayType`, and `StructType` are not currently supported and pre-existing Google Spanner limitations apply. Specifically:
> - Column value size is limited to 10MB,
> - In GoogleSQL, `NUMERIC` type is limited to 9 digits of scale, Spark supports up to 38.



### Exporting Spanner Graphs

To export [Spanner Graphs](https://cloud.google.com/spanner/docs/graph/overview),
please use the Python class `SpannerGraphConnector` included in the jar.

The connector supports exporting the graph into separate node and edge
DataFrames, and exporting the graph into
[GraphFrames](https://graphframes.github.io/graphframes/docs/_site/user-guide.html)
directly.

This is an example of exporting a graph from Spanner as a GraphFrame:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("spanner-graphframe-graphx-example")
         .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
         .config("spark.jars", path_to_connector_jar)
         .getOrCreate())

spark.sparkContext.addPyFile(path_to_connector_jar)
from spannergraph import SpannerGraphConnector

connector = (SpannerGraphConnector()
             .spark(spark)
             .project("$YourProjectId")
             .instance("$YourInstanceId")
             .database("$YourDatabaseId")
             .graph("$YourGraphId"))

g = connector.load_graph()
g.vertices.show()
g.edges.show()
```

To export node and edge DataFrames instead of GraphFrames, please use
`load_dfs` instead:

```python
df_vertices, df_edges, df_id_map = connector.load_dfs()
```

#### Node ID Mapping

While Spanner Graph allows nodes to be identified with more than one element
key, many libraries for processing graphs, including GraphFrames, expect only
one ID field, ideally integers.

When node IDs are not integers, the connector assigns a unique integer ID to
each row in node tables and maps node keys in edge tables to integer IDs with
DataFrame joins by default. Please use `load_graph_and_mapping` or `load_dfs`
to retrieve the mapping when loading a graph:

```python
g, df_id_map = connector.load_graph_and_mapping()
```

or

```python
df_vertices, df_edges, df_id_map = connector.load_dfs()
```

If you do not want to let the connector perform this mapping, please specify
`.export_string_ids(True)` to let the connector output string concatenations of
table IDs (generated by the connector based on the graph schema) and element
keys directly. The format of the concatenated strings is
`{table_id}@{key_1}|{key_2}|{key_3}|...`, where element keys joined with `|` as
the separator, and `\ ` being used as the escape character. For example, the
string ID of a node with table ID `1` and keys `(a, b|b, c\c)` will be
`1@a|b\|b|c\\c`.

#### Graph Connector Options

Here is a summary of the options supported by the graph connector.
Please refer to the API documentation of
[`SpannerGraphConnector`](python/spannergraph/_connector.py) for details.

##### Required

| Option                  | Summary of Purpose                                                                                                    |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------|
| spark                   | The spark session to read graph to                                                                                    |
| project                 | ID of the Google Cloud project containing the graph                                                                   |
| instance                | ID of the Spanner instance containing the graph                                                                       |
| database                | ID of the Spanner database containing the graph                                                                       |
| graph                   | Name of the graph as defined in the database schema                                                                   |

##### Optional

| Option                  | Summary of Purpose                                                                                                    | Default                                            |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| data_boost              | Enable [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview)                               | Disabled                                           |
| partition_size_bytes    | The [partitionSizeBytes](https://cloud.google.com/spanner/docs/reference/rest/v1/PartitionOptions) hint for Spanner   | No hint provided                                   |
| repartition             | Enable repartitioning of node and edge DataFrames and set the target number of partitions                             | No repartitioning                                  |
| read_timestamp          | The timestamp of the snapshot to read from                                                                            | Read the snapshot at the time when load is called  |
| symmetrize_graph        | Symmetrizes the output graph by adding reverse edges                                                                  | No symmetrization                                  |
| export_string_ids       | Output string concatenations of the element keys instead of assigning integer IDs and performing joins                | Output integer IDs                                 |
| node_label / edge_label | Specify label element filters, additional properties to fetch, and element-wise property filters (details below)      | Export all nodes and edges and no element property |
| node_query / edge_query | Overwrite the queries used to fetch nodes and edges (details below)                                                   | Use queries generated by the connector             |

#### Filters and Element Properties

You can choose to include only graph elements with specific labels by providing
`node_label` and/or `edge_label` options. `node_label` and `edge_label` can also
be used to specify element properties to include in the output and additional
element-wise filters (i.e., WHERE clauses). The columns for the returned
properties will be prefixed with "property_" to avoid naming conflicts (e.g.,
when fetching a property named "id").

To fetch additional properties or specify an element-wise filter without
performing any filtering by label, please use `"*"` to match any label. Other
label filters of the same type (node/edge) cannot be used if a `"*"` label
filter is specified for that type.

This example fetches all nodes with their "name" property, all "KNOWS" edges
with their "SingerId" and "FriendId" properties, and all "CREATES_MUSIC" edges
with a release date after 1900-01-01:

```python
connector = (connector
             .node_label("*", properties=["name"])
             .edge_label("KNOWS", properties=["SingerId", "FriendId"])
             .edge_label("CREATES_MUSIC", where="release_date > '1900-01-01'"))
```

#### Direct Queries

In addition to letting the connector generate queries to read nodes and edges
from Spanner, you can provide your own GQL queries with `node_query` and
`edge_query` to fetch the node and edge tables, with some restrictions:

- The queries must be
  [root-partitionable](https://cloud.google.com/spanner/docs/reads#read_data_in_parallel).
- The output columns must meet the following conditions:
    - A column in the node DataFrame is named "id".
      This column will be used to identify nodes.
    - A column in the edge DataFrame is named "src".
      This column will be used to identify source nodes.
    - A column in the edge DataFrame is named "dst".
      This column will be used to identify destination nodes.

This example provides custom GQL queries to fetch the node and edge tables of
the graph:

```python
node_query = "SELECT * FROM GRAPH_TABLE " \
             "(MusicGraph MATCH (n:SINGER) RETURN n.id AS id)"
edge_query = "SELECT * FROM GRAPH_TABLE " \
             "(MusicGraph MATCH -[e:KNOWS]-> " \
             "RETURN e.SingerId AS src, e.FriendId AS dst)"
connector = (connector
             .node_query(node_query)
             .edge_query(edge_query))
```

#### Source and Destination Key Limitation

Currently, the graph connector expects source_key and destination_key of an Edge
to match the node_element_key of the referenced source and destination Node
respectively
([Element Definition](https://cloud.google.com/spanner/docs/reference/standard-sql/graph-schema-statements#element_definition)).
For example, if an edge table *E* references a node table *N* as source nodes,
and *N* has a 2-part compound [node_c1, node_c2] as its node_element_key, the
source_key of *E* must also be a 2-part compound [edge_c1, edge_c2]. A partial
match, e.g. source_key = [edge_c1], can logically form a hypergraph and is not
supported.

### Data Types

Here are the mappings for supported Spanner data types.

Spanner GoogleSql Type|Spark Data Type|Notes
---|---|---
ARRAY    |ArrayType    | Nested ARRAY is not supported, e.g. ARRAY<ARRAY<BOOL>>.
BOOL     |BooleanType  |
BYTES    |BinaryType   |
DATE     |DateType     | The date range is [1700-01-01, 9999-12-31].
FLOAT64  |DoubleType   |
INT64    |LongType     | The supported integer range is [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
JSON     |StringType   | Spark has no JSON type. The values are read as String.
NUMERIC  |DecimalType  | The NUMERIC will be converted to DecimalType with 38 precision and 9 scale, which is the same as the Spanner definition.
STRING   |StringType   |
TIMESTAMP|TimestampType| Only microseconds will be converted to Spark timestamp type. The range of timestamp is  [0001-01-01 00:00:00, 9999-12-31 23:59:59.999999]

### Filter Pushdown

The connector automatically computes column and pushdown filters the DataFrame's `SELECT` statement e.g.

```
df.select("word")
  .where("word = 'Hamlet' or word = 'Claudius'")
  .collect()
```

filters to the column `word`  and pushed down the predicate filter `word = 'hamlet' or word = 'Claudius'`. Note filters containing ArrayType column is not pushed down.

Filter pushdown is currently not supported when exporting graphs.

### Monitoring

When Data Boost is enabled, the usage can be monitored by using Cloud Monitoring. The [page]([url](https://cloud.google.com/spanner/docs/databoost/databoost-monitor#use_to_track_usage)) explains how to do that step by step. The usage cannot be grouped by the Spark job id though.

### Debugging

Dataproc [web interface]([url](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces)) can be used to debug especially to tune the performance. On the `YARN Application Timeline` page, it displays the execution timeline details for the executors and other functions. You can assign more workers if there are many tasks assigned to a same executor.

### Root-partitionable Query

When DataBoost is enabled, all queries that are fed into Cloud Spanner must be root-partionable. Please see [`Read data in parallel`](https://cloud.google.com/spanner/docs/reads#read_data_in_parallel) for more details. If you encounter an issue related to partitioning when using this connector, it is probably that the table being read from is not supported.

### PostgreSQL

The connector supports the Spanner [PostgreSQL interface-enabled databases](https://cloud.google.com/spanner/docs/postgresql-interface#postgresql-components).

#### Data types

Spanner PostgreSql Type|Spark Data Type|Notes
---|---|---
array                                |ArrayType    | Nested array is not supported.
bool / boolean                       |BooleanType  |
bytea                                |BinaryType   |
date                                 |DateType     | The date range is [1700-01-01, 9999-12-31].
double precision / float8            |DoubleType   |
int8 / bigint                        |LongType     | The supported integer range is [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
jsonb                                |StringType   | Spark has no JSON type. The values are read as String.
numeric / decimal                    |DecimalType  | The NUMERIC will be converted to DecimalType with 38 precision and 9 scale, which is the same as the Spanner definition.
varchar / text / character varying   |StringType   |
timestamptz/timestamp with time zone |TimestampType| Only microseconds will be converted to Spark timestamp type. The range of timestamp is  [0001-01-01 00:00:00, 9999-12-31 23:59:59.999999]

#### Filter Pushdown

Since jsonb is converted to StringType in Spark, a filter containing jsonb column can only be pushed down as a string filter. For the jsonb column, `IN` filter is not pushdown to Cloud Spanner.

Filters containing array column will not be pushed down.
