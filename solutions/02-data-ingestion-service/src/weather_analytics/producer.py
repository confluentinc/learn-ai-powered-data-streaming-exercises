"""
Weather Analytics Workshop - Producer Application
"""

from weather_analytics.data_ingestion_service import DataIngestionService


def main():
    """Producer entry point - will eventually stream weather data to Kafka"""
    print("=" * 60)
    print("Weather Analytics Workshop - AI-Powered Streaming Pipeline")
    print("=" * 60)
    print()

    ingestion_service = DataIngestionService(poll_interval=60)

    for observation in ingestion_service.ingest():
        print(observation)


if __name__ == "__main__":
    main()

