from typing import Generator
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


class KafkaConsumer:
    """Generic Kafka consumer for reading streaming data from topics"""

    @staticmethod
    def load_config(config_file: str) -> dict:
        """
        Load librdkafka configuration from properties file

        Args:
            config_file: Path to properties file (e.g., kafka-librdkafka.properties)

        Returns:
            dict: Configuration dictionary suitable for Consumer
        """
        config = {}
        with open(config_file, "r") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        config[key.strip()] = value.strip()
        return config

    def __init__(
        self,
        kafka_config_file: str,
        schema_registry_config_file: str,
        topic: str,
        group_id: str = "python-kafka-consumer"
    ):
        """
        Initialize the Kafka consumer with Schema Registry support

        Args:
            kafka_config_file: Path to kafka-librdkafka.properties file
            schema_registry_config_file: Path to schema-registry.properties file
            topic: Source Kafka topic
            group_id: Consumer group ID (defaults to 'python-kafka-consumer')
        """
        # TODO: Implement the initialization of the Kafka consumer
        ...

    def consume_stream(self) -> Generator[dict, None, None]:
        """
        Consume messages from Kafka as a generator

        This method runs indefinitely, consuming messages from the subscribed topic
        and yielding them as Python dictionaries.

        Uses Schema Registry deserializers to automatically handle schema-encoded
        messages (json-registry format). The deserializers automatically:
        - Strip the 5-byte Schema Registry header
        - Fetch the schema from the registry using the schema ID
        - Deserialize and validate the message

        Yields:
            dict: Deserialized message as dictionary
        """
        # TODO: Implement the consumption of the Kafka stream
        ...

    def close(self) -> None:
        """Close the consumer and clean up resources"""
        # TODO: Implement the closing of the Kafka consumer
        ...

