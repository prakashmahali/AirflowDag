SELECT
  referenced_table.project_id,
  referenced_table.dataset_id,
  referenced_table.table_id,
  SUM(j.total_slot_ms) / 1000 AS total_slot_seconds,
  COUNT(*) AS query_count
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
  UNNEST(referenced_tables) AS referenced_table
WHERE
  j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND j.job_type = "QUERY"
  AND j.state = "DONE"
GROUP BY
  referenced_table.project_id,
  referenced_table.dataset_id,
  referenced_table.table_id
ORDER BY
  total_slot_seconds DESC;
