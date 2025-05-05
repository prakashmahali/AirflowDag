SELECT
  job_type,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', creation_time) AS hour,
  SUM(total_slot_ms) / 1000 AS total_slot_seconds,
  COUNT(*) AS job_count
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT j,
  UNNEST(referenced_tables) AS t
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND state = "DONE"
  AND t.project_id = "your-project-id"
  AND t.dataset_id = "your_dataset"
  AND t.table_id = "your_table"
GROUP BY
  job_type, hour
ORDER BY
  hour DESC, job_type;
