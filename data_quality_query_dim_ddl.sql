DROP TABLE IF EXISTS ${HiveDb}.data_quality_query_dim;
CREATE EXTERNAL table ${HiveDb}.data_quality_query_dim
(
  query_id string,
  workflow_name string,
  query_SQL string,
  query_description string,
  active_indicator string
)
STORED AS PARQUET;
