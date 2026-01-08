-- Temperature Forecasting Query
--
-- Write a SELECT statement that:
-- 1. Reads from `simplified_weather_observations`
-- 2. Uses ML_FORECAST to predict temperature values
-- 3. PARTITION BY `station_id` to create per-station models
-- 4. ORDER BY `observation_time` to process in time order
-- 5. Extracts forecast results into columns
--
-- Your SELECT should include these columns:
-- - `station_id` - Already a top-level field (use as-is)
-- - `observation_time` - Already a top-level field (use as-is)
-- - `temperature_c` - Already a top-level field (use as-is)
-- - `forecast_value` - Extract from forecast[1].forecast_value
-- - `lower_bound` - Extract from forecast[1].lower_bound
-- - `upper_bound` - Extract from forecast[1].upper_bound
-- - `rmse` - Extract from forecast[1].rmse (model accuracy)
-- - `aic` - Extract from forecast[1].aic (model quality)
-- - `processing_time` - Use CURRENT_TIMESTAMP
--
-- You will be replacing the following fields in the example:
-- - `sensor_id` with `station_id`
-- - `reading_time` with `observation_time`
-- - `metric_value` with `temperature_c`
--
-- Note: Filter out rows where temperature_c IS NULL or observation_time IS NULL
--
-- Note: ML_FORECAST returns an ARRAY because it supports multi-step forecasting
--       (predicting multiple future time points). Each array element contains a
--       forecast for a different future timestamp. We use forecast[1] to get the
--       first (next) predicted value.
--
-- An example forecasting query with ML_FORECAST:

SELECT
  sensor_id,
  reading_time,
  metric_value,
  forecast[1].forecast_value AS forecast_value,
  forecast[1].lower_bound AS lower_bound,
  forecast[1].upper_bound AS upper_bound,
  forecast[1].rmse AS rmse,
  forecast[1].aic AS aic,
  CURRENT_TIMESTAMP AS processing_time
FROM (
  SELECT
    sensor_id,
    reading_time,
    metric_value,
    ML_FORECAST(
      metric_value,
      reading_time,
      JSON_OBJECT(
        'minTrainingSize' VALUE 8, -- start predicting after 8 observations
        'enableStl' VALUE true -- enable seasonal decomposition
      )
    ) OVER (
      PARTITION BY sensor_id
      ORDER BY reading_time -- process in time order
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forecast
  FROM example_sensor_data
  WHERE metric_value IS NOT NULL
    AND reading_time IS NOT NULL
);

-- TODO: Replace the example above with your temperature forecasting query

