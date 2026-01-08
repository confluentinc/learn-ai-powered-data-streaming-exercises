-- Create the `deduplicated_weather_observations` Table
--
-- This table will store our deduplicated weather data.
-- The schema should match the `raw_weather_observations` table.
--
-- Your table should include:
-- - A `station_id` field (STRING) for the message key
-- - A `properties` ROW containing all the nested NOAA observation fields:
--     - `station`, `stationId`, `timestamp`, `textDescription` (all STRING)
--     - Measurement fields: `temperature`, `relativeHumidity`, `windSpeed`, 
--       `windDirection`, `barometricPressure`, `dewpoint`, and `visibility`
--       Each measurement is a ROW with `value` (DOUBLE) and `unitCode` (STRING)
-- - A `processing_time` metadata field
--
-- Make sure to:
-- - Name the table `deduplicated_weather_observations`
-- - Distribute the table by `station_id` into 5 buckets
-- - Use 'json-registry' format for both key and value
--
-- An example of how to create a table with nested ROW types:

CREATE TABLE example_table (
  `id` STRING,
  `data` ROW<
    `name` STRING,
    `measurement` ROW<`value` DOUBLE, `unitCode` STRING>
  >,
  `processing_time` TIMESTAMP(3) METADATA FROM 'timestamp'
) DISTRIBUTED BY (`id`) INTO 5 BUCKETS WITH (
  'scan.startup.mode' = 'earliest-offset',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'kafka.retention.time' = '4 h'
);

-- TODO: Replace the example above with your CREATE TABLE statement
