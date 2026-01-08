INSERT INTO simplified_weather_observations
SELECT 
  `station_id`,
  TO_TIMESTAMP_LTZ(properties.`timestamp`, 'yyyy-MM-dd''T''HH:mm:ssXXX') AS observation_time,
  properties.temperature.`value` AS temperature_c,
  properties.relativeHumidity.`value` AS humidity_pct,
  properties.windSpeed.`value` AS wind_speed_kmh,
  properties.windDirection.`value` AS wind_direction_deg,
  properties.barometricPressure.`value` AS pressure_pa,
  properties.dewpoint.`value` AS dewpoint_c,
  properties.visibility.`value` AS visibility_m,
  properties.textDescription AS description
FROM deduplicated_weather_observations;

