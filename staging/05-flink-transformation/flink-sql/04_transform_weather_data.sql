-- Transformation Query
--
-- Write an INSERT INTO ... SELECT statement that:
-- 1. Reads from `deduplicated_weather_observations`
-- 2. Extracts nested property values into flat columns
-- 3. Parses the ISO timestamp string into a proper TIMESTAMP_LTZ type (preserving timezone)
-- 4. Writes the transformed data to `simplified_weather_observations`
--
-- Your SELECT should include these columns:
-- - `station_id` - Already a top-level field (use as-is)
-- - `observation_time` - Parse from properties.`timestamp` using TO_TIMESTAMP_LTZ
-- - `temperature_c` - Extract from properties.temperature.`value`
-- - `humidity_pct` - Extract from properties.relativeHumidity.`value`
-- - `wind_speed_kmh` - Extract from properties.windSpeed.`value`
-- - `wind_direction_deg` - Extract from properties.windDirection.`value`
-- - `pressure_pa` - Extract from properties.barometricPressure.`value`
-- - `dewpoint_c` - Extract from properties.dewpoint.`value`
-- - `visibility_m` - Extract from properties.visibility.`value`
-- - `description` - Extract from properties.textDescription
-- - `processing_time` - Use CURRENT_TIMESTAMP
--
-- Note: The timestamp field in NOAA data is ISO 8601 format like:
--       "2024-01-15T14:30:00+00:00"
--       Use TO_TIMESTAMP_LTZ with format 'yyyy-MM-dd''T''HH:mm:ssXXX'
--       The XXX pattern handles the timezone offset (+00:00)
--
-- Note: `timestamp` and `value` need backticks because they are reserved words
--
-- An example transformation query with timestamp parsing and nested field extraction:

INSERT INTO example_transformed_data
SELECT 
  `id`,
  TO_TIMESTAMP_LTZ(data.`timestamp`, 'yyyy-MM-dd''T''HH:mm:ssXXX') AS event_time,
  data.metrics.temperature.`value` AS temp_reading,
  data.metrics.humidity.`value` AS humidity_reading,
  data.label AS status_text,
  CURRENT_TIMESTAMP AS processing_time
FROM `example_source_table`;

-- TODO: Replace the example above with your transformation query
