CREATE TABLE databricks_to_spanner_example (
  id INT64,
  name STRING(MAX),
  value FLOAT64,
  update_ts TIMESTAMP,
) PRIMARY KEY(id);
