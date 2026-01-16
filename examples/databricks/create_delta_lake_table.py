# Databricks notebook source
# MAGIC %md
# MAGIC # Create Sample Delta Lake Table
# MAGIC
# MAGIC This notebook creates a simple Delta Lake table with sample data that can be used to demonstrate writing to Google Cloud Spanner.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Define the schema for our sample data
schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True)
])

# Create some sample data
data = [
    (1, "record_1", 11.1),
    (2, "record_2", 22.2),
    (3, "record_3", 33.3)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Add a timestamp column
df_with_ts = df.withColumn("update_ts", current_timestamp())

# Display the data
df_with_ts.show()
df_with_ts.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the data to a Delta Lake table
# MAGIC
# MAGIC We will save this data to a table named `sample_data_for_spanner`. This table will be read by the `write_to_spanner` notebook.

# COMMAND ----------

# Define the table name
delta_table_name = "sample_data_for_spanner"

# Write the DataFrame to a Delta Lake table
df_with_ts.write.format("delta").mode("overwrite").saveAsTable(delta_table_name)

print(f"Successfully created Delta Lake table: {delta_table_name}")

# You can now query this table using SQL
# %sql
# SELECT * FROM sample_data_for_spanner;
