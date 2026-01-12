ALTER TABLE raw_weather_observations 
MODIFY WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '1' MINUTE;

