import json
import pytest
from itertools import islice
from unittest.mock import patch, mock_open, Mock
from confluent_kafka.schema_registry import Schema
from weather_analytics.kafka_consumer import KafkaConsumer


KAFKA_CONFIG = """# Kafka Configuration
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT
"""

SR_CONFIG = """# Schema Registry Configuration
url=http://localhost:8081
"""

class TestKafkaConsumer:
    """Test suite for KafkaConsumer"""

    @staticmethod
    def mock_message(key: dict, value: dict) -> Mock:
        """
        Create a mock Kafka message with Schema Registry wire format
        
        Args:
            key: Key data as dictionary
            value: Value data as dictionary
            
        Returns:
            Mock Message object with properly formatted key/value
        """
        # Schema Registry wire format: magic byte (0x00) + 4-byte schema ID
        schema_header = b"\x00\x00\x00\x00\x01"
        
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.key.return_value = schema_header + json.dumps(key).encode("utf-8")
        mock_msg.value.return_value = schema_header + json.dumps(value).encode("utf-8")
        
        return mock_msg

    def create_consumer(self, mock_sr_client_class):
        """Helper to create a KafkaConsumer with all necessary mocks"""

        schema = Schema(
            schema_str='{"type": "object"}',
            schema_type="JSON"
        ) 

        mock_sr_instance = Mock()
        mock_sr_instance.get_schema.return_value = schema
        mock_sr_client_class.return_value = mock_sr_instance

        def mock_open_files(filename, *args, **kwargs):
            if "kafka" in filename:
                return mock_open(read_data=KAFKA_CONFIG)()
            else:
                return mock_open(read_data=SR_CONFIG)()
        
        with patch("builtins.open", mock_open_files):
            return KafkaConsumer(
                kafka_config_file="kafka.properties",
                schema_registry_config_file="schema-registry.properties",
                topic="test-topic",
                group_id="test-group",
            )

    @patch("builtins.open", new=mock_open(read_data=KAFKA_CONFIG))
    def test_load_config_should_return_config_while_ignoring_comments_and_empty_lines(
        self,
    ):
        """Test loading configuration from properties file while ignoring comments and empty lines"""

        config = KafkaConsumer.load_config("some_file.properties")

        assert len(config) == 2
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["security.protocol"] == "PLAINTEXT"

    @patch("weather_analytics.kafka_consumer.SchemaRegistryClient")
    @patch("weather_analytics.kafka_consumer.Consumer")
    def test_initialization_should_initialize_consumer(
        self, mock_consumer_class, mock_sr_client
    ):
        """Test consumer initialization with configuration files"""
        consumer = self.create_consumer(mock_sr_client)

        # Verify Consumer was called with config from file
        call_args = mock_consumer_class.call_args[0][0]
        assert call_args["bootstrap.servers"] == "localhost:9092"
        assert call_args["group.id"] == "test-group"
        assert call_args["auto.offset.reset"] == "earliest"
        assert consumer.topic == "test-topic"

        # Verify subscribe was called
        mock_consumer_class.return_value.subscribe.assert_called_once_with(
            ["test-topic"]
        )

    @patch("weather_analytics.kafka_consumer.SchemaRegistryClient")
    @patch("weather_analytics.kafka_consumer.Consumer")
    def test_close_should_close_consumer(
        self, mock_consumer_class, mock_sr_client
    ):
        """Test closing the consumer"""
        consumer = self.create_consumer(mock_sr_client)

        consumer.close()

        mock_consumer_class.return_value.close.assert_called_once()

    @patch("weather_analytics.kafka_consumer.SchemaRegistryClient")
    @patch("weather_analytics.kafka_consumer.Consumer")
    def test_consume_stream_should_yield_messages(
        self, mock_consumer_class, mock_sr_client
    ):
        """Test consuming messages from Kafka with Schema Registry JSON format (key + value)"""
        consumer = self.create_consumer(mock_sr_client)

        mock_msg1 = self.mock_message({"station_id": "KJFK"}, {"temp": 15.0})
        mock_msg2 = self.mock_message({"station_id": "KLAX"}, {"temp": 20.0})

        mock_consumer_class.return_value.poll.side_effect = [mock_msg1, mock_msg2]

        messages = list(islice(consumer.consume_stream(), 2))

        assert len(messages) == 2
        assert messages[0]["station_id"] == "KJFK"
        assert messages[0]["temp"] == 15.0
        assert messages[1]["station_id"] == "KLAX"
        assert messages[1]["temp"] == 20.0

    @patch("weather_analytics.kafka_consumer.SchemaRegistryClient")
    @patch("weather_analytics.kafka_consumer.Consumer")
    def test_consume_stream_should_skip_none_messages(
        self, mock_consumer_class, mock_sr_client_class
    ):
        """Test that None messages are skipped"""
        consumer = self.create_consumer(mock_sr_client_class)

        mock_msg = self.mock_message({"station_id": "KJFK"}, {"temp": 15.0})

        mock_consumer_class.return_value.poll.side_effect = [None, mock_msg]

        messages = list(islice(consumer.consume_stream(), 1))

        assert len(messages) == 1
        assert messages[0]["temp"] == 15.0

    @patch("weather_analytics.kafka_consumer.SchemaRegistryClient")
    @patch("weather_analytics.kafka_consumer.Consumer")
    def test_consume_stream_should_skip_error_messages(
        self, mock_consumer_class, mock_sr_client_class
    ):
        """Test that error messages are skipped"""
        consumer = self.create_consumer(mock_sr_client_class)

        mock_error_msg = Mock()
        mock_error_msg.error.return_value = "Some error"

        mock_good_msg = self.mock_message({"station_id": "KJFK"}, {"temp": 15.0})

        mock_consumer_class.return_value.poll.side_effect = [mock_error_msg, mock_good_msg]

        messages = list(islice(consumer.consume_stream(), 1))

        assert len(messages) == 1
        assert messages[0]["temp"] == 15.0
