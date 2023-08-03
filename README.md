# spark-spanner-connector

## Running tests
Please run the [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) locally.

Before running `mvn`, please set the variable `SPANNER_EMULATOR_HOST=localhost:9090`

## Compiling the JAR
To compile it against Spark 3.1, Please run
```shell
./mvnw install -P3.1
```
