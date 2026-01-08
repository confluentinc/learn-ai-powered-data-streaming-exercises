from unittest.mock import patch, Mock, call
import json
import pytest

from weather_analytics.consumer import main


class TestConsumer:
    """Test suite for consumer application with Hugging Face enrichment"""

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_consumer_correctly(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
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

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_enrichment_service(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
        """Test that main() creates WeatherEnrichmentService"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_enrichment_class.assert_called_once()

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_producer_correctly(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
        """Test that main() creates KafkaProducer with correct parameters"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_producer_class.assert_called_once_with(
            config_file="config/kafka-librdkafka.properties",
            topic="enriched_weather_observations"
        )

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_passes_consumer_stream_to_enrichment_service(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
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

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_produces_enriched_messages(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
        """Test that main() sends enriched messages to the producer"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        enriched_stream = iter([
            {"station_id": "KJFK", "weather_condition": "Sunny"},
            {"station_id": "KLAX", "weather_condition": "Cloudy"},
        ])
        mock_enrichment_instance.enrich.return_value = enriched_stream
        mock_enrichment_class.return_value = mock_enrichment_instance

        mock_producer_instance = Mock()
        mock_producer_class.return_value = mock_producer_instance

        main()

        mock_producer_instance.produce_stream.assert_called_once()
        # Verify the enriched stream was passed to produce_stream
        call_args = mock_producer_instance.produce_stream.call_args
        assert call_args[1]["key_extractor"] is not None

    @patch("weather_analytics.consumer.KafkaProducer")
    @patch("weather_analytics.consumer.WeatherEnrichmentService")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_closes_consumer_in_finally_block(self, mock_consumer_class, mock_enrichment_class, mock_producer_class):
        """Test that main() closes the consumer in the finally block"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        mock_enrichment_instance = Mock()
        mock_enrichment_instance.enrich.return_value = iter([])
        mock_enrichment_class.return_value = mock_enrichment_instance

        main()

        mock_consumer_instance.close.assert_called_once()
