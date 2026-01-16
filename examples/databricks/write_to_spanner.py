# Databricks notebook source
# MAGIC %md
# MAGIC # Write to Spanner from Databricks
# MAGIC
# MAGIC This notebook reads data from a Delta Lake table and writes it to a Google Cloud Spanner table using the Spark Spanner Connector.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1.  A Spanner database and table have been created.
# MAGIC 2.  The Databricks cluster has been configured with:
# MAGIC     *   The Spark Spanner Connector JAR attached as a library.
# MAGIC     *   An init script (`setup_gcp_credentials.sh`) to handle GCP authentication.
# MAGIC     *   The `GCP_CREDENTIALS` environment variable pointing to a Databricks secret.
# MAGIC 3.  The `create_delta_lake_table` notebook has been run to create the source table `sample_data_for_spanner`.

# COMMAND ----------

import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Spanner Connection Properties
# MAGIC
# MAGIC These connection details are read from the cluster's environment variables, which should be configured before running this notebook.

# COMMAND ----------

# Spanner connection properties are read from environment variables
project_id = os.environ.get("GCP_PROJECT_ID")
instance_id = os.environ.get("SPANNER_INSTANCE_ID")
database_id = "spark_spanner_db_example"
spanner_table = "databricks_to_spanner_example"

# Delta Lake source table
delta_table_name = "sample_data_for_spanner"

if not project_id or not instance_id:
    raise Exception("The environment variables 'GCP_PROJECT_ID' and 'SPANNER_INSTANCE_ID' must be set on the cluster.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Data from Delta Lake
# MAGIC
# MAGIC Load the data from the `sample_data_for_spanner` table into a DataFrame.

# COMMAND ----------

df = spark.read.table(delta_table_name)

print("Successfully read data from Delta Lake table:")
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Data to Spanner
# MAGIC
# MAGIC Use the Spark Spanner Connector to write the DataFrame to the Spanner table. The connector will automatically map the DataFrame columns to the Spanner table columns by name.

# COMMAND ----------

try:
  df.write.format("cloud-spanner") \
    .option("projectId", project_id) \
    .option("instanceId", instance_id) \
    .option("databaseId", database_id) \
    .option("table", spanner_table) \
    .mode("append") \
    .save()
  
  print(f"Successfully wrote data to Spanner table: {spanner_table}")

except Exception as e:
  print(f"An error occurred while writing to Spanner: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify the Write (Optional)
# MAGIC
# MAGIC You can read the data back from Spanner to verify that the write was successful.

# COMMAND ----------

try:
  # Read data back from Spanner
  spanner_df = spark.read.format("cloud-spanner") \
      .option("projectId", project_id) \
      .option("instanceId", instance_id) \
      .option("databaseId", database_id) \
      .option("table", spanner_table) \
      .load()

  print(f"Successfully read data from Spanner table '{spanner_table}':")
  spanner_df.show()

except Exception as e:
    print(f"An error occurred while reading from Spanner: {e}")
