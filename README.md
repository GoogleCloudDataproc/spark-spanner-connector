# Spark Connector for Cloud Spanner

The Apache Spark - Cloud Spanner Connector is a library to support Spark accessing
[Cloud Spanner]((https://cloud.google.com/spanner/) as an external data source or sink.
The project uses [sbt](https://www.scala-sbt.org/) to manage build artifacts.

# Status
This library is currently work-in-progress and is likely to get backwards-incompatible updates.

## Running Project

Use `sbt Test / runMain cloud.spark.SparkApp` to run a sample Spark application that uses the connector.

[Setup](https://cloud.google.com/docs/authentication/getting-started) authentication using a service account
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

Execute `sbt clean package` to build the executable.

## Disclaimer

This is not an officially supported Google product

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
