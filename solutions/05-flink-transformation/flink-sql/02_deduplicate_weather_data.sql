INSERT INTO deduplicated_weather_observations
SELECT 
  `station_id`,
  properties,
  CURRENT_TIMESTAMP AS processing_time
FROM (
  SELECT 
    `station_id`,
    properties,
    ROW_NUMBER() OVER (
      PARTITION BY properties.stationId, properties.`timestamp`
      ORDER BY properties.`timestamp` ASC
    ) AS row_num
  FROM raw_weather_observations
)
WHERE row_num = 1;