from unittest.mock import patch, Mock
import json
import pytest

from weather_analytics.consumer import main


class TestConsumer:
    """Test suite for consumer application with Hugging Face enrichment"""

    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_consumer_correctly(self, mock_consumer_class, mock_enrichment_class):
        """Test that main() creates KafkaConsumer with correct parameters"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_consumer_class.assert_called_once_with(
            kafka_config_file="config/kafka-librdkafka.properties",
            schema_registry_config_file="config/schema-registry.properties",
            topic="simplified_weather_observations",
            group_id="weather-analytics-consumer"
        )

    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_enrichment_service(self, mock_consumer_class, mock_enrichment_class):
        """Test that main() creates WeatherEnrichmentService"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_enrichment_class.assert_called_once()

    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_passes_consumer_stream_to_enrichment_service(self, mock_consumer_class, mock_enrichment_class):
        """Test that main() passes consumer stream to enrichment service"""
        mock_consumer_instance = Mock()
        mock_stream = iter([{"station_id": "KJFK"}])
        mock_consumer_instance.consume_stream.return_value = mock_stream
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_enrichment_instance.enrich.assert_called_once_with(mock_stream)

    @patch("weather_analytics.consumer.print")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_prints_enriched_messages_as_json(self, mock_consumer_class, mock_enrichment_class, mock_print):
        """Test that main() prints each enriched message as JSON"""
        test_messages = [
            {"station_id": "KJFK", "temperature_c": 15.0, "weather_condition": "Sunny"},
            {"station_id": "KLAX", "temperature_c": 22.5, "weather_condition": "Cloudy"},
        ]

        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter(test_messages)
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        print_calls = [call[0][0] for call in mock_print.call_args_list if call[0]]

        for message in test_messages:
            assert json.dumps(message) in print_calls

