"""
Weather Analytics Workshop - Consumer Application
"""

import json
from weather_analytics.kafka_consumer import KafkaConsumer


def main():
    """Consumer entry point - consumes processed weather data from Kafka"""
    print("=" * 60)
    print("Weather Analytics Workshop - Simplified Weather Consumer")
    print("=" * 60)
    print()

    consumer = KafkaConsumer(
        kafka_config_file="config/kafka-librdkafka.properties",
        schema_registry_config_file="config/schema-registry.properties",
        topic="simplified_weather_observations",
        group_id="weather-analytics-consumer"
    )

    try:
        for message in consumer.consume_stream():
            print(json.dumps(message))
    except KeyboardInterrupt:
        print("Consumer stopped.")


if __name__ == "__main__":
    main()

