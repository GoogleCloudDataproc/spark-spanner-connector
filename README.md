# spark-spanner-connector

## Roadmap
- [ ] Retrieve table
- [ ] DataFrame.printSchema
- [ ] DataFrame.show
- [ ] Retrieving data from a Cloud Spanner table
- [ ] Performance tests
- [ ] Create table in Cloud Spanner
- [ ] DataFrameWriter -- writing back data to Cloud Spanner

## Unsupported features and limitations
- [ ] Cloud Spanner types ["STRUCT"](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#struct_type) and ["ARRAY"](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#array_type) have no equivalent [type mapping in Apache Spark](https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/types/DataTypes.html)
