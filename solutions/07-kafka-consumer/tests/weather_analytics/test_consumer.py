from unittest.mock import patch, Mock
import json
import pytest

from weather_analytics.consumer import main


class TestConsumer:
    """Test suite for consumer application"""

    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_initializes_consumer_correctly(self, mock_consumer_class):
        """Test that main() creates KafkaConsumer with correct parameters"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        main()

        mock_consumer_class.assert_called_once_with(
            kafka_config_file="config/kafka-librdkafka.properties",
            schema_registry_config_file="config/schema-registry.properties",
            topic="simplified_weather_observations",
            group_id="weather-analytics-consumer"
        )

    @patch("weather_analytics.consumer.print")
    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_prints_messages_as_json(self, mock_consumer_class, mock_print):
        """Test that main() prints each message as JSON"""
        test_messages = [
            {"station_id": "KJFK", "temperature_c": 15.0},
            {"station_id": "KLAX", "temperature_c": 22.5},
        ]

        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter(test_messages)
        mock_consumer_class.return_value = mock_consumer_instance

        main()

        mock_consumer_instance.consume_stream.assert_called_once()

        print_calls = [call[0][0] for call in mock_print.call_args_list if call[0]]

        for message in test_messages:
            assert json.dumps(message) in print_calls

    @patch("weather_analytics.consumer.KafkaConsumer")
    def test_main_closes_consumer_in_finally_block(self, mock_consumer_class):
        """Test that main() closes the consumer in the finally block"""
        mock_consumer_instance = Mock()
        mock_consumer_instance.consume_stream.return_value = iter([])
        mock_consumer_class.return_value = mock_consumer_instance

        main()

        mock_consumer_instance.close.assert_called_once()
