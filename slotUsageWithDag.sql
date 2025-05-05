SELECT
  label.value AS dag_id,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', j.creation_time) AS hour,
  SUM(j.total_slot_ms) / 1000 AS total_slot_seconds,
  COUNT(*) AS job_count
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT AS j,
  UNNEST(j.labels) AS label
WHERE
  j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND j.state = 'DONE'
  AND j.job_type = 'QUERY'
  AND label.key = 'airflow_dag'
GROUP BY
  dag_id, hour
ORDER BY
  hour DESC, dag_id;
