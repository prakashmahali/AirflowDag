SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', creation_time) AS hour,
  CASE
    WHEN LOWER(query) LIKE 'insert%' THEN 'INSERT'
    WHEN LOWER(query) LIKE 'export%' THEN 'EXPORT'
    WHEN LOWER(query) LIKE 'copy%' THEN 'COPY'
    WHEN LOWER(query) LIKE 'create table%' THEN 'CREATE_TABLE'
    WHEN LOWER(query) LIKE 'select%' THEN 'SELECT'
    WHEN LOWER(query) LIKE 'merge%' THEN 'MERGE'
    WHEN LOWER(query) LIKE 'update%' THEN 'UPDATE'
    WHEN LOWER(query) LIKE 'delete%' THEN 'DELETE'
    ELSE 'OTHER'
  END AS query_operation,
  SUM(total_slot_ms) / 1000 AS total_slot_seconds,
  COUNT(*) AS query_count
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND state = "DONE"
  AND job_type = "QUERY"
  AND LOWER(query) LIKE '%your_table%' -- Narrow down to your target table
GROUP BY
  hour, query_operation
ORDER BY
  hour DESC, query_operation;
