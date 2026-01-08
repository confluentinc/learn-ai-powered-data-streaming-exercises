CREATE TABLE enriched_weather_observations (
  `station_id` STRING NOT NULL,
  `observation_time` TIMESTAMP_LTZ(3) NOT NULL,
  `temperature_c` DOUBLE,
  `humidity_pct` DOUBLE,
  `wind_speed_kmh` DOUBLE,
  `wind_direction_deg` DOUBLE,
  `pressure_pa` DOUBLE,
  `dewpoint_c` DOUBLE,
  `visibility_m` DOUBLE,
  `description` STRING,
  `weather_condition` STRING
) DISTRIBUTED BY (`station_id`) INTO 5 BUCKETS WITH (
  'scan.startup.mode' = 'earliest-offset',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'kafka.retention.time' = '4 h'
);

