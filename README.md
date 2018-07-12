# Cloud Spanner Connector for Apache Spark™

Cloud Spanner Connector for Apache Spark™ is a library to support Apache Spark to access
[Cloud Spanner](https://cloud.google.com/spanner/) as an external data source or sink.

## Status

This library is currently work-in-progress and is likely to get backwards-incompatible updates.

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

buildScan: requiredColumns = WrappedArray(AccountId, Name, EMail)
buildScan: filters = WrappedArray() <-- FIXME Use it
+----------+------+---------------+
|AccountId |Name  |EMail          |
+----------+------+---------------+
|0xCAFEBABE|A Name|hello@world.com|
+----------+------+---------------+
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

Use `Dataset.explain` or web UI to see the physical plan of a structured query and learn what filters were pushed down. They are listed under `PushedFilters` section.

```
== Physical Plan ==
*(1) Scan SpannerRelation(org.apache.spark.sql.SparkSession@389a1e34,spanner.spark.SpannerOptions@204b0f07) [AccountId#0,Name#1,EMail#2] PushedFilters: [*StringStartsWith(Name,A)], ReadSchema: struct<AccountId:string,Name:string,EMail:string>
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
* [Information Schema](https://cloud.google.com/spanner/docs/information-schema) for creating Spark's table schema (`StructType`)
