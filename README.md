# Cloud Spanner Connector for Apache Spark™

Cloud Spanner Connector for Apache Spark™ is a library to support Apache Spark to access
[Cloud Spanner](https://cloud.google.com/spanner/) as an external data source or sink.

## Status

This library is currently work-in-progress and is likely to get backwards-incompatible updates.

Consult [Issues](https://github.com/GoogleCloudPlatform/cloud-spanner-spark-connector/issues) to know the missing features.

## Disclaimer

This is not an officially supported Google product.

## Supported Operations

Cloud Spanner Connector for Apache Spark™ supports the following:

* [Loading Data from Cloud Spanner](#loading-data-from-cloud-spanner)
* [Saving Dataset to Spanner](#saving-dataset-to-spanner)
* [Inserting (or Overwriting) Dataset to Spanner](#inserting-or-overwriting-dataset-to-spanner)
* [Filter Pushdown](#filter-pushdown)
* [Type Inference](#type-inference)
* [Human-Readable Representation (web UI and Dataset.explain)](#human-readable-representation-web-ui-and-datasetexplain)
* [Logging using log4j](#configuring-logging)

## Unsupported Operations

If you think that the Cloud Spanner Connector for Apache Spark™ should support a feature, please file an issue in the [connector's repository](https://github.com/GoogleCloudPlatform/cloud-spanner-spark-connector/issues).

## Using Cloud Spanner Connector

As there are no official releases yet, you can use the Cloud Spanner Connector only after you publish the project locally first.

The project uses [sbt](https://www.scala-sbt.org/) to manage build artifacts.

### Publishing Locally

In order to publish locally you should use `sbt publishLocal`.

**TIP** Remove `~/.ivy2/local/com.google.cloud.spark/` and `~/.ivy2/cache/com.google.cloud.spark/` directories to allow for rebuilding the connector and make sure that you use the latest version (not a cached one!)

### "Installing" Connector

The final step is to "install" the connector while submitting your Spark SQL application for execution (i.e. making sure that the connector jar is on the CLASSPATH of the driver and executors).

---

**TIP** Due to the shared dependencies (`com.google.common` and `com.google.protobuf`) with different versions for Apache Spark and Google Cloud Spanner you may face the following issue:

```
java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;II)V
  at com.google.cloud.spanner.SpannerOptions.createChannels(SpannerOptions.java:252)
  at com.google.cloud.spanner.SpannerOptions.<init>(SpannerOptions.java:92)
  at com.google.cloud.spanner.SpannerOptions.<init>(SpannerOptions.java:45)
  at com.google.cloud.spanner.SpannerOptions$Builder.build(SpannerOptions.java:195)
  at com.google.cloud.spark.spanner.SpannerRelation.<init>(SpannerRelation.scala:36)
  at com.google.cloud.spark.spanner.SpannerRelationProvider.createRelation(SpannerRelationProvider.scala:39)
  at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:340)
  at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:239)
  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:227)
  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:174)
  ... 54 elided
```  

A fix is to shade the dependencies to allow for such mixed-version dependencies to co-exist. Read the following article to get a better understanding of the issue and how to resolve it:

* [Managing Java dependencies for Apache Spark applications on Cloud Dataproc](https://cloud.google.com/blog/products/data-analytics/managing-java-dependencies-apache-spark-applications-cloud-dataproc)

In short, you have to use the following in `build.sbt` and use `spark-submit --jars .../cloud-spanner-spark-connector/target/scala-2.11/spark-cloud-spanner-assembly-0.1.0-alpha-SNAPSHOT.jar`.

```
test in assembly := {}
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename(
    "com.google.common.**" -> "repackaged.com.google.common.@1",
    "com.google.protobuf.**" -> "repackaged.com.google.protobuf.@1"
  ).inAll
)
``` 

---

Use `spark-submit` (or `spark-shell`) with `--packages` command-line option with the fully-qualified dependency name of the connector (and the other dependencies in their correct versions, i.e. Google Guava and Google Protobuf).

```
$ ./bin/spark-shell --packages com.google.cloud.spark:spark-cloud-spanner_2.11:0.1.0-alpha-SNAPSHOT \
    --exclude-packages com.google.guava:guava \
    --driver-class-path /Users/jacek/.m2/repository/com/google/guava/guava/20.0/guava-20.0.jar:/Users/jacek/.ivy2/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-3.6.0.jar
```

**TIP** Use https://github.com/GoogleCloudPlatform/google-cloud-java/blob/master/google-cloud-clients/pom.xml to know the exact versions of the dependencies.

If everything went fine, you could copy the above Spark SQL snippet and then work with the `Account` dataset.

```
scala> :pa
// Entering paste mode (ctrl-D to finish)

val opts = Map(
  "instanceId" -> "dev-instance",
  "databaseId" -> "demo"
)
val table = "Account"

val accounts = spark
  .read
  .format("cloud-spanner") // <-- here
  .options(opts)
  .load(table)

// Exiting paste mode, now interpreting.
...
opts: scala.collection.immutable.Map[String,String] = Map(instanceId -> dev-instance, databaseId -> demo)
table: String = Account
accounts: org.apache.spark.sql.DataFrame = [AccountId: string, Name: string ... 12 more fields]

scala> accounts.printSchema
root
 |-- AccountId: string (nullable = false)
 |-- Name: string (nullable = false)
 |-- EMail: string (nullable = false)
 |-- bool: boolean (nullable = true)
 |-- bytes_max: byte (nullable = true)
 |-- bytes_1: byte (nullable = true)
 |-- date: date (nullable = true)
 |-- float64: double (nullable = true)
 |-- int64: long (nullable = true)
 |-- string_max: string (nullable = true)
 |-- string_2621440: string (nullable = true)
 |-- timestamp_allow_commit_timestamp: timestamp (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- array_bool: array (nullable = true)
 |    |-- element: boolean (containsNull = true)

scala> accounts.show
// rows shown here
```

**NOTE** Don't forget to `export GOOGLE_APPLICATION_CREDENTIALS` or authenticate in another way.

## Running Project

Use `sbt` and execute `Test / runMain com.google.cloud.spark.spanner.SparkApp` to run a Spark demo application that uses the connector.

```
sbt 'Test / runMain com.google.cloud.spark.spanner.SparkApp'
```

**NOTE**: You may want to [setup authentication](https://cloud.google.com/docs/authentication/getting-started) using a service account
and export `GOOGLE_APPLICATION_CREDENTIALS` environment variable with the service account credentials in JSON format.

```
$ sbt 'Test / runMain com.google.cloud.spark.spanner.SparkApp'
...
[info] Running com.google.cloud.spark.spanner.SparkApp
Running Spark 2.3.1
GOOGLE_APPLICATION_CREDENTIALS: (redacted)
...
```

## Loading Data from Cloud Spanner

The connector supports loading data from a Google Cloud Spanner table and is registered under `cloud-spanner` name as the external data source format.

Simply, use `cloud-spanner` format to let Spark SQL to use the connector.

| Option  | Description |
| :---: | :---: |
| `table` | The name of the table to write rows to |
| `instanceId` | Spanner Instance ID |
| `databaseId` | Spanner Database ID |
| `maxPartitions` | Desired maximum number of partitions. Default: `1` |
| `partitionSizeBytes` | Data size of the partitions. Default: `1` |

In the following example, the connector loads data from the `Account` table in `demo` database in `dev-instance` Cloud Spanner instance.

```
val opts = Map(
  "instanceId" -> "dev-instance",
  "databaseId" -> "demo",
  "table" -> "Account"
)

val accounts = spark
  .read
  .format("cloud-spanner") // <-- here
  .options(opts)
  .option("maxPartitions", 5)
  .load
```

## Filter Pushdown

Cloud Spanner Connector for Apache Spark™ supports **filter pushdown optimization** for all available filter predicates in Apache Spark:

* `And`
* `EqualNullSafe`
* `EqualTo`
* `GreaterThan`
* `GreaterThanOrEqual`
* `In`
* `IsNotNull`
* `IsNull`
* `LessThan`
* `LessThanOrEqual`
* `Not`
* `Or`
* `StringContains`
* `StringEndsWith`
* `StringStartsWith`

That means that the filter predicates are executed by Cloud Spanner engine itself while data is loaded by a Apache Spark application and before the data lands on Spark executors.

Use `Dataset.explain` or web UI to see the physical plan of a structured query and learn what filters were pushed down.

All the filters handled by the connector itself (and hence Cloud Spanner database engine) are listed as `PushedFilters` prefixed with the star (`*`).

```
scala> accounts.explain
== Physical Plan ==
*(1) Scan Spanner(ID: dev-instance, demo, Account) [AccountId#0,Name#1,EMail#2,bool#3,bytes_max#4,bytes_1#5,date#6,float64#7,int64#8L,string_max#9,string_2621440#10,timestamp_allow_commit_timestamp#11,timestamp#12,array_bool#13] PushedFilters: [], ReadSchema: struct<AccountId:string,Name:string,EMail:string,bool:boolean,bytes_max:tinyint,bytes_1:tinyint,d...
```

## Type Inference

Cloud Spanner Connector for Apache Spark™ uses [INFORMATION_SCHEMA.COLUMNS](https://cloud.google.com/spanner/docs/information-schema#tables) table to query for the columns and their types of a table.

The Spanner-specific schema is converted to a Spark SQL schema per the type conversion rules.

| Spanner Type  | Catalyst Type |
| :---: | :---: |
| STRING(*)     | StringType    |
| BOOL          | BooleanType   |
| INT64 | LongType
| FLOAT64 | DoubleType
| BYTES(*)     | ByteType    |
| DATE | DateType
| TIMESTAMP | TimestampType
| ARRAY(T) | ArrayType(T)

The Cloud Spanner connector records the Spanner type of a column as a comment of a Spark SQL `StructField`. Use `Dataset.schema` to access the fields and then `StructField.getComment` to access the comment with the Spanner type.

**TIP:** Read the official documentation about [Data Types](https://cloud.google.com/spanner/docs/data-types).

## Human-Readable Representation (web UI and Dataset.explain)

Cloud Spanner Connector for Apache Spark™ displays itself in the following format:

```
Spanner(ID: [instanceId], [databaseId], [table])
```

You can use web UI or `Dataset.explain` to review query plans and Spanner-specific relations.

```
scala> accounts.explain
== Physical Plan ==
*(1) Scan Spanner(ID: dev-instance, demo, Account)...
```

## Configuring Logging

Cloud Spanner Connector for Apache Spark™ uses `org.apache.spark.internal.Logging` internally for logging. It works as described in the official documentation of Apache Spark™ in [Configuring Logging](http://spark.apache.org/docs/latest/configuration.html#configuring-logging).

Simply, add the following to `log4j.properties` to enable `DEBUG` logging level for the connector.

```
log4j.logger.com.google.cloud.spark.spanner=DEBUG
```

## Testing Connector with Spark (and ScalaTest)

Use [com.google.cloud.spark.spanner.BaseSpec](src/test/scala/com/google/cloud/spark/spanner/BaseSpec.scala) as the test base for tests. It automatically checks whether `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set before executing a test specification and defines `withSparkSession` that creates and closes a `SparkSession`. 

Use [com.google.cloud.spark.spanner.SpannerSpec](src/test/scala/com/google/cloud/spark/spanner/SpannerSpec.scala) as an example.

## Saving Dataset to Spanner

The connector supports saving data (as a `DataFrame`) to a table in Google Cloud Spanner.

Simply, use `cloud-spanner` format to let Spark SQL to use the connector (with other write options).

| Option  | Description |
| :---: | :---: |
| `table` | The name of the table to write rows to |
| `instanceId` | Spanner Instance ID |
| `databaseId` | Spanner Database ID |
| `writeSchema` | Custom write schema |
| `primaryKey` | Primary key (that a Spanner table requires for `CREATE TABLE` SQL statement) |

In the following example, the connector saves a DataFrame to `Verified_Accounts` table in `demo` database in `dev-instance` Cloud Spanner instance.

```
val writeOpts = Map(
  SpannerOptions.INSTANCE_ID -> "dev-instance",
  SpannerOptions.DATABASE_ID -> "demo",
  SpannerOptions.TABLE -> "Verified_Accounts",
  SpannerOptions.PRIMARY_KEY -> "id"
)

val accounts = spark
  .write
  .format("cloud-spanner")
  .options(writeOpts)
  .mode(SaveMode.Append) // <-- save mode
  .save
```

The connector supports all save modes and a custom write schema (e.g. when different types in a Spanner table are required to match the DataFrame's).

## Inserting (or Overwriting) Dataset to Spanner

The connector supports inserting (or overwriting) data (as a `DataFrame`) to a table in Google Cloud Spanner.

`Append` or `Overwrite` save modes are supported.

Simply, use `cloud-spanner` format to let Spark SQL to use the connector (with other write options).

```
val instance = "dev-instance"
val database = "demo"
val table = s"scalatest_insert_${System.currentTimeMillis()}"
val primaryKey = "id"
val writeOpts = Map(
  SpannerOptions.INSTANCE_ID -> instance,
  SpannerOptions.DATABASE_ID -> database,
  SpannerOptions.TABLE -> table,
  SpannerOptions.PRIMARY_KEY -> primaryKey
)

spark.range(10)
  .write
  .format("cloud-spanner")
  .options(writeOpts)
  .mode(SaveMode.ErrorIfExists)
  .saveAsTable(table)

spark.range(10, 20, 1)
  .write
  .format("cloud-spanner")
  .options(writeOpts)
  .mode(SaveMode.Append) // or Overwrite
  .insertInto(table)
```

## References

* [google-cloud](https://googlecloudplatform.github.io/google-cloud-java/google-cloud-clients/index.html)
    * [google-cloud-java](https://github.com/GoogleCloudPlatform/google-cloud-java/) on GitHub
    * [StackOverflow / google-cloud-spanner](https://stackoverflow.com/questions/tagged/google-cloud-spanner)
    * [StackOverflow / google-cloud-platform+java](https://stackoverflow.com/questions/tagged/google-cloud-platform+java)
* [Getting Started with Cloud Spanner in Java](https://cloud.google.com/spanner/docs/getting-started/java/)
* [Google Cloud Client Library for Java](https://github.com/GoogleCloudPlatform/google-cloud-java)
* [Google Cloud Java Client for Spanner](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-clients/google-cloud-spanner)
* [Package com.google.cloud.spanner](https://googlecloudplatform.github.io/google-cloud-java/google-cloud-clients/apidocs/index.html?com/google/cloud/spanner/package-summary.html) 
* [Google Cloud Spanner](https://cloud.google.com/spanner/) - the official web site
* [Cloud Spanner Documentation](https://cloud.google.com/spanner/docs/)
