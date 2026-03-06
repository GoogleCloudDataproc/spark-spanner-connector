// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spark.spanner.examples;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SpannerSpark {
    public static void main(String[] args) {
        String projectId = System.getenv("SPANNER_PROJECT_ID");
        String instanceId = System.getenv("SPANNER_INSTANCE_ID");
        String databaseId = System.getenv("SPANNER_DATABASE_ID");

        // Configure a Spark session with a Spanner catalog named "spanner".
        // This allows reading and writing tables with SQL and DataFrame APIs
        // without specifying connection options on every operation.
        SparkSession spark = SparkSession
            .builder()
            .appName("SpannerSpark Example")
            .config("spark.sql.catalog.spanner",
                "com.google.cloud.spark.spanner.SpannerCatalog")
            .config("spark.sql.catalog.spanner.projectId", projectId)
            .config("spark.sql.catalog.spanner.instanceId", instanceId)
            .config("spark.sql.catalog.spanner.databaseId", databaseId)
            .getOrCreate();

        // --- Read via the catalog ---
        // Tables are referenced as <catalog>.<table>.
        Dataset<Row> df = spark.sql("SELECT * FROM spanner.people");
        df.show();
        df.printSchema();

        // --- Read via the DataSource API (format) ---
        // This approach still works and does not require catalog configuration.
        Dataset<Row> df2 = spark.read()
            .format("cloud-spanner")
            .option("projectId", projectId)
            .option("instanceId", instanceId)
            .option("databaseId", databaseId)
            .option("table", "people")
            .load();
        df2.show();

        // --- Write via the DataSource API ---
        StructType schema = new StructType()
            .add("id", DataTypes.LongType, false)
            .add("name", DataTypes.StringType, true)
            .add("email", DataTypes.StringType, true);

        Dataset<Row> writeDf = spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "John Doe", "john@example.com"),
                RowFactory.create(2L, "Jane Doe", "jane@example.com")),
            schema);

        writeDf.write()
            .format("cloud-spanner")
            .option("projectId", projectId)
            .option("instanceId", instanceId)
            .option("databaseId", databaseId)
            .option("table", "people")
            .mode(SaveMode.Append)
            .save();
    }
}
