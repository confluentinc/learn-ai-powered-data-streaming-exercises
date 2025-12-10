"""
Weather Analytics Workshop - Producer Application
"""

from weather_analytics.data_ingestion_service import DataIngestionService
from weather_analytics.kafka_producer import KafkaProducer


def main():
    """Producer entry point - streams weather data to Kafka"""
    print("=" * 60)
    print("Weather Analytics Workshop - AI-Powered Streaming Pipeline")
    print("=" * 60)
    print()

    producer = KafkaProducer(
        config_file="config/kafka-librdkafka.properties",
        topic="raw_weather_observations",
        client_id="RawWeatherProducer"
    )

    ingestion_service = DataIngestionService(poll_interval=60)

    try:
        producer.produce_stream(
            ingestion_service.ingest(),
            lambda message: message.get("station_id", "unknown")
        )
    except KeyboardInterrupt:
        print("Producer stopped.")


if __name__ == "__main__":
    main()

