-- Wind Anomaly Detection Query
--
-- Write a SELECT statement that:
-- 1. Reads from `simplified_weather_observations`
-- 2. Uses ML_DETECT_ANOMALIES to identify unusual wind speed values
-- 3. PARTITION BY `station_id` to create per-station models
-- 4. ORDER BY `observation_time` to process in time order
-- 5. Extracts anomaly detection results into columns
--
-- Your SELECT should include these columns:
-- - `station_id` - Already a top-level field (use as-is)
-- - `observation_time` - Already a top-level field (use as-is)
-- - `wind_speed_kmh` - Already a top-level field (use as-is)
-- - `is_anomaly` - Extract from anomaly.is_anomaly (TRUE if anomaly detected)
-- - `forecast_value` - Extract from anomaly.forecast_value (expected value)
-- - `lower_bound` - Extract from anomaly.lower_bound
-- - `upper_bound` - Extract from anomaly.upper_bound
-- - `rmse` - Extract from anomaly.rmse (model accuracy)
-- - `aic` - Extract from anomaly.aic (model quality)
--
-- How anomaly detection works:
-- 1. The model predicts what the wind speed *should* be
-- 2. Compares actual wind speed to the prediction
-- 3. If outside the confidence interval → is_anomaly = TRUE
--
-- You will be replacing the following fields in the example:
-- - `sensor_id` with `station_id`
-- - `reading_time` with `observation_time`
-- - `metric_value` with `wind_speed_kmh`
--
-- Note: Filter out rows where wind_speed_kmh IS NULL or observation_time IS NULL
--
-- Note: Unlike ML_FORECAST which returns an ARRAY for multi-step predictions,
--       ML_DETECT_ANOMALIES returns a single ROW since it only evaluates whether
--       the current observation is anomalous.
--
-- An example anomaly detection query with ML_DETECT_ANOMALIES:

SELECT 
  sensor_id,
  reading_time,
  metric_value,
  anomaly.is_anomaly AS is_anomaly,
  anomaly.forecast_value AS forecast_value,
  anomaly.lower_bound AS lower_bound,
  anomaly.upper_bound AS upper_bound,
  anomaly.rmse AS rmse,
  anomaly.aic AS aic
FROM (
  SELECT 
    sensor_id,
    reading_time,
    metric_value,
    ML_DETECT_ANOMALIES(
      metric_value, -- the metric to analyze
      reading_time, -- the time of the observation
      JSON_OBJECT(
        'minTrainingSize' VALUE 8, -- start detecting after 8 observations
        'enableStl' VALUE true -- enable seasonal decomposition
      )
    ) OVER (
      PARTITION BY sensor_id
      ORDER BY reading_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly
  FROM example_sensor_data
  WHERE metric_value IS NOT NULL
    AND reading_time IS NOT NULL
);

-- TODO: Replace the example above with your wind anomaly detection query

