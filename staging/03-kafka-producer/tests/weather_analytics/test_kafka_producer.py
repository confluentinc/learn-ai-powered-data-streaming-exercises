"""
Tests for Kafka Producer
"""

import json
from unittest.mock import patch, mock_open
from weather_analytics.kafka_producer import KafkaProducer


class TestKafkaProducer:
    """Test suite for KafkaProducer"""

    SAMPLE_CONFIG = """# Kafka Configuration
bootstrap.servers=localhost:9092

security.protocol=PLAINTEXT
"""

    @patch("builtins.open", new=mock_open(read_data=SAMPLE_CONFIG))
    def test_load_config_should_return_config_while_ignoring_comments_and_empty_lines(self):
        """Test loading configuration from properties file while ignoring comments and empty lines"""

        config = KafkaProducer.load_config("some_file.properties")

        assert len(config) == 2
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["security.protocol"] == "PLAINTEXT"

    @patch("weather_analytics.kafka_producer.Producer")
    @patch("builtins.open", new=mock_open(read_data=SAMPLE_CONFIG))
    def test_initialization_should_initialize_producer(self, mock_producer_class):
        """Test producer initialization with configuration file"""
        producer = KafkaProducer(
            config_file="kafka-librdkafka.properties", topic="test-topic", client_id="my-client"
        )

        # Verify Producer was called with config from file
        call_args = mock_producer_class.call_args[0][0]
        assert call_args["bootstrap.servers"] == "localhost:9092"
        assert call_args["client.id"] == "my-client"
        assert producer.topic == "test-topic"

    @patch("weather_analytics.kafka_producer.Producer")
    @patch("builtins.open", new=mock_open(read_data=SAMPLE_CONFIG))
    def test_produce_stream_should_produce_messages_to_kafka(self, mock_producer_class):
        """Test producing messages to Kafka"""
        producer = KafkaProducer("kafka.properties", "test-topic")

        messages = [
            {"station": "KJFK", "temp": 15.0},
            {"station": "KLAX", "temp": 20.0},
        ]

        def message_generator():
            for msg in messages:
                yield msg

        producer.produce_stream(message_generator(), lambda msg: msg["station"])

        assert mock_producer_class.return_value.produce.call_count == 2
        
        calls = mock_producer_class.return_value.produce.call_args_list
        assert calls[0][1]["key"] == b'{"station_id": "KJFK"}'
        assert calls[1][1]["key"] == b'{"station_id": "KLAX"}'
        assert calls[0][1]["value"] == json.dumps(messages[0]).encode("utf-8")
        assert calls[1][1]["value"] == json.dumps(messages[1]).encode("utf-8")
        assert callable(calls[0][1]["callback"])

        mock_producer_class.return_value.flush.assert_called_once_with(30.0)