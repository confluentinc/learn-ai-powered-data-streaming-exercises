CREATE TABLE deduplicated_weather_observations (
  `station_id` STRING,
  `properties` ROW<
    `station` STRING,
    `stationId` STRING,
    `timestamp` STRING,
    `textDescription` STRING,
    `temperature` ROW<`value` DOUBLE, `unitCode` STRING>,
    `relativeHumidity` ROW<`value` DOUBLE, `unitCode` STRING>,
    `windSpeed` ROW<`value` DOUBLE, `unitCode` STRING>,
    `windDirection` ROW<`value` DOUBLE, `unitCode` STRING>,
    `barometricPressure` ROW<`value` DOUBLE, `unitCode` STRING>,
    `dewpoint` ROW<`value` DOUBLE, `unitCode` STRING>,
    `visibility` ROW<`value` DOUBLE, `unitCode` STRING>
  >
) DISTRIBUTED BY (`station_id`) INTO 5 BUCKETS WITH (
  'scan.startup.mode' = 'earliest-offset',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'kafka.retention.time' = '4 h'
);

