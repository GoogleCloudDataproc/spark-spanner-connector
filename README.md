# spark-spanner-connector

## Pre-requisites
- [ ] Ensure that [Databoost is enabled for your Cloud Spanner database](https://cloud.google.com/spanner/docs/databoost/databoost-applications#before_you_begin)

## Running tests locally
Please run the [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) locally.

Before running `mvn`, please set the variable `SPANNER_EMULATOR_HOST=localhost:9090`

## Compiling the JAR
To compile it against Spark 3.1, Please run
```shell
./mvnw install -P3.1
```

## Submitting the job to Google Dataproc
```shell
gcloud dataproc jobs submit pyspark --cluster "spanner-spark-cluster" \
    --jars=./spark-3.1-spanner/target/spark-3.1-spanner-0.0.1-SNAPSHOT.jar \
    --region us-central1 examples/SpannerSpark.py
```

## Submitting the Spark job locally
```shell
./bin/spark-shell --jars \
        local:/spark-spanner-connector/spark-3.1-spanner-lib/target/spark-3.1-spanner-lib-0.0.1-SNAPSHOT.jar
```
which will pull up a spark shell and you can run it by passing in the options
to create the connection to Cloud Spanner.

### Variables needed

Variable|Comments
---|---
projectId|The projectID containing the Cloud Spanner database
instanceId|The instanceID of the Cloud Spanner database
databaseId|The databaseID of the Cloud Spanner database
table|The Table of the Cloud Spanner database that you are reading from


### Spark shell example
```shell
var df = spark.read.format("cloud-spanner").option("table", "ATable").option("projectId", PROJECT_ID).option("instanceId", INSTANCE_ID).option("databaseId", DATABASE_ID).load()
df.show()
df.printSchema()
```
