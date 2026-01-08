SELECT
  station_id,
  observation_time,
  temperature_c,
  forecast[1].forecast_value AS forecast_value,
  forecast[1].lower_bound AS lower_bound,
  forecast[1].upper_bound AS upper_bound,
  forecast[1].rmse AS rmse,
  forecast[1].aic AS aic
FROM (
  SELECT
    station_id,
    observation_time,
    temperature_c,
    ML_FORECAST(
      temperature_c,
      observation_time,
      JSON_OBJECT(
        'minTrainingSize' VALUE 8,
        'enableStl' VALUE true
      )
    ) OVER (
      PARTITION BY station_id
      ORDER BY observation_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forecast
  FROM simplified_weather_observations
  WHERE temperature_c IS NOT NULL
    AND observation_time IS NOT NULL
);

