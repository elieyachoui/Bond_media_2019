DROP TABLE IF EXISTS ${HiveDb}.data_quality_result;
CREATE EXTERNAL table ${HiveDb}.data_quality_result
(
  query_id string,
  result_name string,
  result_value string,
  query_description string,
  execution_timestamp string
)
STORED AS PARQUET;
