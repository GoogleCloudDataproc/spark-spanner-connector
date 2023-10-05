# Apache Spark SQL connector for Google Cloud Spanner

The connector supports reading [Google Cloud Spanner](https://cloud.google.com/spanner) tables into Spark's DataFrames. This is done by using the [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to communicate with [Spanner Java library](https://github.com/googleapis/java-spanner).

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

## Downloading and Using the Connector

You can find the released jar file from the Releases tag on right of the github page. The name pattern is spark-3.1-spanner-x.x.x.jar. The 3.1 indicates the driver depends on the Spark 3.1 and x.x.x is the Spark Spanner connector version.

### Connector to Spark Compatibility Matrix
| Connector \ Spark                     | 2.3     | 2.4<br>(Scala 2.11) | 2.4<br>(Scala 2.12) | 3.0     | 3.1     | 3.2     | 3.3     |
|---------------------------------------|---------|---------------------|---------------------|---------|---------|---------|---------|
| spark-3.1-spanner                     |         |                     |                     |         | &check; | &check; | &check; |

### Connector to Dataproc Image Compatibility Matrix
| Connector \ Dataproc Image            | 1.3     | 1.4     | 1.5     | 2.0     | 2.1     | Serverless<br>Image 1.0 | Serverless<br>Image 2.0 |
|---------------------------------------|---------|---------|---------|---------|---------|-------------------------|-------------------------|
| spark-3.1-spanner                     |         |         |         | &check; | &check; | &check;                 | &check;                 |

### Maven / Ivy Package

The connector is not available on the Maven Central yet.

### Specifying the Spark Spanner connector version in a Dataproc cluster

You can use the standard `--jars` or `--packages` (or alternatively, the `spark.jars`/`spark.jars.packages` configuration) to specify the Spark Spanner connector. For example:

```shell
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
    --jars=./path/to/spark-3.1-spanner-x.x.x.jar \
    --region us-central1 examples/SpannerSpark.py
```
## Usage

The connector uses the cross language [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources):

### Reading data from a Spanner table
This is an example of using Python code to connect to a Spanner table. You can find more examples or documentations on the [usage](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html).

```
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
For the other languages support, you can refer to [Scala](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html), [Java](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/Dataset.html), and [R](https://spark.apache.org/docs/latest/api/R/reference/SparkDataFrame.html). You can also refer [Scala, Java](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark), [R](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark-r) about how to submit a job for other languages.

### Properties

Here are the options supported in the Spark Spanner connector.

Variable|Validation|Comments
---|---|---
projectId|String|The projectID containing the Cloud Spanner database
instanceId|String|The instanceID of the Cloud Spanner database
databaseId|String|The databaseID of the Cloud Spanner database
table|String|The Table of the Cloud Spanner database that you are reading from
enableDataboost|Boolean|Enable the [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview), which provides independent compute resources to query Spanner with near-zero impact to existing workloads. Note the option may trigger [extra charge](https://cloud.google.com/spanner/pricing#spanner-data-boost-pricing).

### Data types

Here are the mappings for supported Spanner data types.

Spanner Data Type|Spark Data Type|Notes
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

### Filtering

The connector automatically computes column and pushdown filters the DataFrame's `SELECT` statement e.g.

```
df.select("word")
  .where("word = 'Hamlet' or word = 'Claudius'")
  .collect()
```

filters to the column `word`  and pushed down the predicate filter `word = 'hamlet' or word = 'Claudius'`.

### PostgreSQL

The connector doesn't support Spanner [PostgreSQL interface-enabled databases](https://cloud.google.com/spanner/docs/postgresql-interface#postgresql-dialect-support)https://cloud.google.com/spanner/docs/postgresql-interface#postgresql-dialect-support.
