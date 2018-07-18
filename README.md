# Cloud Spanner Connector for Apache Spark™

Cloud Spanner Connector for Apache Spark™ is a library to support Apache Spark to access
[Cloud Spanner](https://cloud.google.com/spanner/) as an external data source or sink.

## Status

This library is currently work-in-progress and is likely to get backwards-incompatible updates.

Consult [Issues](https://github.com/GoogleCloudPlatform/spanner-spark-connector/issues) to know the missing features.

## Disclaimer

This is not an officially supported Google product

## Running Project

Use `sbt` and execute `Test / runMain cloud.spark.SparkApp` to run a Spark application that uses the connector.

**NOTE**: You may want to [setup authentication](https://cloud.google.com/docs/authentication/getting-started) using a service account
and export `GOOGLE_APPLICATION_CREDENTIALS` environment variable with the service account credentials in JSON format.

```
sbt:cloudspanner-spark-connector> Test / runMain cloud.spark.SparkApp
...
[info] Running cloud.spark.SparkApp
Running Spark 2.3.1
GOOGLE_APPLICATION_CREDENTIALS: (redacted)
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

Dump schema types (Spanner types in round brackets):
 |-- AccountId: string (spanner: STRING(32))
 |-- Name: string (spanner: STRING(256))
 |-- EMail: string (spanner: STRING(256))
 |-- bool: boolean (spanner: BOOL)
 |-- bytes_max: byte (spanner: BYTES(MAX))
 |-- bytes_1: byte (spanner: BYTES(1))
 |-- date: date (spanner: DATE)
 |-- float64: double (spanner: FLOAT64)
 |-- int64: long (spanner: INT64)
 |-- string_max: string (spanner: STRING(MAX))
 |-- string_2621440: string (spanner: STRING(2621440))
 |-- timestamp_allow_commit_timestamp: timestamp (spanner: TIMESTAMP)
 |-- timestamp: timestamp (spanner: TIMESTAMP)
 |-- array_bool: array (spanner: ARRAY<BOOL>)

...
```

## Building Project

The project uses [sbt](https://www.scala-sbt.org/) to manage build artifacts.

Execute `sbt clean package` to build the executable.

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

Use `Dataset.explain` or web UI to see the physical plan of a structured query and learn what filters were pushed down. They are listed under `PushedFilters` section with the star (`*`).

```
== Physical Plan ==
*(1) Scan SpannerRelation(org.apache.spark.sql.SparkSession@389a1e34,spanner.spark.SpannerOptions@204b0f07) [AccountId#0,Name#1,EMail#2] PushedFilters: [*StringStartsWith(Name,A)], ReadSchema: struct<AccountId:string,Name:string,EMail:string>
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

**TIP:** Read the official documentation about <a href="https://cloud.google.com/spanner/docs/data-types">Data Types</a>.

## Human-Readable Representation (web UI and Dataset.explain)

Cloud Spanner Connector for Apache Spark™ displays itself in the following format:

```
Spanner(ID: [instanceId], [databaseId], [table])
```

You can use web UI or `Dataset.explain` to review query plans and Spanner-specific relations.

```
val opts = Map(
  "instanceId" -> "dev-instance",
  "databaseId" -> "demo"
)
val table = "Account"

val accounts = spark
  .read
  .format("spanner")
  .options(opts)
  .load(table)

scala> accounts.explain
== Physical Plan ==
*(1) Scan Spanner(ID: dev-instance, demo, Account)...

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
