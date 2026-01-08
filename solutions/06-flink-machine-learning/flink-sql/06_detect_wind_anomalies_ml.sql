SELECT 
  station_id,
  observation_time,
  wind_speed_kmh,
  anomaly.is_anomaly AS is_anomaly,
  anomaly.forecast_value AS forecast_value,
  anomaly.lower_bound AS lower_bound,
  anomaly.upper_bound AS upper_bound,
  anomaly.rmse AS rmse,
  anomaly.aic AS aic
FROM (
  SELECT 
    station_id,
    observation_time,
    wind_speed_kmh,
    ML_DETECT_ANOMALIES(
      wind_speed_kmh,
      observation_time,
      JSON_OBJECT(
        'minTrainingSize' VALUE 8,
        'enableStl' VALUE true
      )
    ) OVER (
      PARTITION BY station_id
      ORDER BY observation_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly
  FROM simplified_weather_observations
  WHERE wind_speed_kmh IS NOT NULL
    AND observation_time IS NOT NULL
);
