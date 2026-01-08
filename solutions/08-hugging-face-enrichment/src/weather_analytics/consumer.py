"""
Weather Analytics Workshop - Consumer Application
"""

from weather_analytics.kafka_consumer import KafkaConsumer
from weather_analytics.kafka_producer import KafkaProducer
from weather_analytics.weather_enrichment_service import WeatherEnrichmentService


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

    enrichment_service = WeatherEnrichmentService()

    producer = KafkaProducer(
        config_file="config/kafka-librdkafka.properties",
        topic="enriched_weather_observations"
    )

    try:
        enriched_stream = enrichment_service.enrich(consumer.consume_stream())
        producer.produce_stream(
            enriched_stream,
            key_extractor=lambda msg: msg["station_id"]
        )
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
