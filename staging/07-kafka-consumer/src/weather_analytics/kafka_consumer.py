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
        # TODO: Store the topic as an instance variable (self.topic)

        # TODO: Load the Schema Registry config and create a SchemaRegistryClient
        # - Use self.load_config() to load the schema_registry_config_file
        # - Create a SchemaRegistryClient with the loaded config

        # TODO: Create a JSONDeserializer with schema_str=None
        # - Pass schema_str=None to fetch schemas automatically from the registry
        # - Pass the schema_registry_client you created above

        # TODO: Load the Kafka config and configure the consumer
        # - Use self.load_config() to load the kafka_config_file
        # - Add "group.id" set to the group_id parameter
        # - Add "auto.offset.reset" set to "earliest"

        # TODO: Create a Consumer and subscribe to the topic
        # - Create a Consumer with the kafka_config
        # - Call subscribe() with the topic in a list
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
        # TODO: Implement a try/finally block with an infinite polling loop
        #
        # In the try block:
        # - Loop indefinitely with while True
        # - Poll for messages with self.consumer.poll(1.0)
        # - Skip messages that are None or have errors (msg.error())
        # - Deserialize the key using self.deserializer with SerializationContext
        #   and MessageField.KEY, then extract the "station_id" from the result
        # - Deserialize the value using self.deserializer with SerializationContext
        #   and MessageField.VALUE
        # - Add the station_id to the value data dictionary
        # - Yield the complete message
        #
        # In the finally block:
        # - Close the consumer with self.consumer.close()
        ...

    def close(self) -> None:
        """Close the consumer and clean up resources"""
        # TODO: Close the consumer by calling self.consumer.close()
        ...
