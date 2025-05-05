SELECT
  creation_time,
  job_id,
  user_email,
  total_slot_ms / 1000 AS slot_seconds,
  total_bytes_processed,
  total_bytes_billed,
  labels
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND state = "DONE"
  AND job_type = "QUERY"
  AND labels.dag_id = "your_dag_id_here"
ORDER BY
  creation_time DESC;
