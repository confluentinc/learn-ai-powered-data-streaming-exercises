-- Deduplication Query
--
-- Write an INSERT INTO ... SELECT statement that:
-- 1. Reads from `raw_weather_observations`
-- 2. Uses ROW_NUMBER() to identify duplicates
-- 3. PARTITION BY `properties.stationId` and `properties.timestamp`
-- 4. ORDER BY `properties.timestamp`
-- 5. Writes the deduplicated data to `deduplicated_weather_observations`
--
-- Your SELECT should include:
-- - `station_id`
-- - `properties`
-- - `CURRENT_TIMESTAMP AS processing_time`
--
-- Note: `timestamp` needs backticks because it's a reserved word
--
-- An example deduplication query:

INSERT INTO example_deduplicated_table
SELECT 
  `id`,
  data,
  CURRENT_TIMESTAMP AS processing_time
FROM (
  SELECT 
    `id`,
    data,
    ROW_NUMBER() OVER ( -- Assigns a sequential number to each row within its partition
      PARTITION BY data.customerId, data.`orderId` -- Creates groups for each unique combination of customerId and orderId
      ORDER BY data.`orderId` ASC -- Within each group, order by orderId
    ) AS row_num -- The sequential number assigned to each row
  FROM example_source_table
)
WHERE row_num = 1; -- Keep only the first record in each group

-- TODO: Replace the example above with your deduplication query
