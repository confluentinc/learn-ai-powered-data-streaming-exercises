-- Create the `simplified_weather_observations` Table
--
-- This table will store our transformed weather data with a flat structure.
-- Unlike the deduplicated table which preserves the nested NOAA format,
-- this table extracts values into simple, analytics-friendly columns.
--
-- Your table should include these fields:
-- - `station_id` (STRING NOT NULL) - The weather station identifier
-- - `observation_time` (TIMESTAMP_LTZ(3) NOT NULL) - When the observation was recorded (with timezone)
-- - `temperature_c` (DOUBLE) - Temperature in Celsius
-- - `humidity_pct` (DOUBLE) - Relative humidity percentage
-- - `wind_speed_kmh` (DOUBLE) - Wind speed in km/h
-- - `wind_direction_deg` (DOUBLE) - Wind direction in degrees
-- - `pressure_pa` (DOUBLE) - Barometric pressure in Pascals
-- - `dewpoint_c` (DOUBLE) - Dewpoint in Celsius
-- - `visibility_m` (DOUBLE) - Visibility in meters
-- - `description` (STRING) - Text description of conditions
-- - `processing_time` (TIMESTAMP(3) METADATA FROM 'timestamp') - Kafka timestamp
--
-- Make sure to:
-- - Name the table `simplified_weather_observations`
-- - Distribute the table by `station_id` into 5 buckets
--
-- An example of creating a flat table with scalar fields:

CREATE TABLE example_sensor_readings (
  `sensor_id` STRING NOT NULL,
  `reading_time` TIMESTAMP_LTZ(3) NOT NULL,
  `temperature` DOUBLE,
  `humidity` DOUBLE,
  `status` STRING,
  `processing_time` TIMESTAMP(3) METADATA FROM 'timestamp'
) DISTRIBUTED BY (`sensor_id`) INTO 5 BUCKETS WITH (
  'scan.startup.mode' = 'earliest-offset',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'kafka.retention.time' = '4 h'
);

-- TODO: Replace the example above with your CREATE TABLE statement
