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

    # TODO: Create a KafkaConsumer instance with:
    # - kafka_config_file: "config/kafka-librdkafka.properties"
    # - schema_registry_config_file: "config/schema-registry.properties"
    # - topic: "simplified_weather_observations"
    # - group_id: "weather-analytics-consumer"

    # TODO: Implement a try/except/finally block that:
    # - Loops over the consumer's consume_stream() method
    # - Prints each message as JSON using json.dumps()
    # - Catches KeyboardInterrupt and prints "Consumer stopped."
    # - Closes the consumer in the finally block
    ...


if __name__ == "__main__":
    main()
