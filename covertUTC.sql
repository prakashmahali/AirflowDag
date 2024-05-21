-- Creating example_table for demonstration
CREATE OR REPLACE TABLE example_table (
  id INT64,
  event_time STRING,
  timezone STRING
);

-- Inserting example data
INSERT INTO example_table (id, event_time, timezone)
VALUES
  (1, '2023-05-20 12:00:00', 'US/Eastern'),
  (2, '2023-05-20 11:00:00', 'US/Central'),
  (3, '2023-05-20 10:00:00', 'US/Mountain'),
  (4, '2023-05-20 09:00:00', 'US/Pacific');

-- Query to convert to UTC
SELECT 
  id, 
  event_time, 
  timezone,
  -- Parse the event_time string with the corresponding timezone
  PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %Z', event_time || ' ' || timezone) AS timestamp_with_timezone,
  -- Convert the parsed timestamp to UTC
  TIMESTAMP(timestamp_with_timezone AT TIME ZONE 'UTC') AS event_time_utc
FROM 
  example_table;

