"""
Kafka Producer
Generic Kafka producer for streaming data to topics
"""

import json
from typing import Generator, Callable
from confluent_kafka import Producer


class KafkaProducer:
    """Generic Kafka producer for streaming data to topics"""

    @staticmethod
    def load_config(config_file: str) -> dict:
        """
        Load librdkafka configuration from properties file

        Args:
            config_file: Path to properties file (e.g., kafka-librdkafka.properties)

        Returns:
            dict: Configuration dictionary suitable for Producer/Consumer
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
        self, config_file: str, topic: str, client_id: str = "python-kafka-producer"
    ):
        """
        Initialize the Kafka producer

        Args:
            config_file: Path to kafka-librdkafka.properties file
            topic: Target Kafka topic
            client_id: Client ID (defaults to 'python-kafka-producer')
        """
        self.topic = topic

        config = self.load_config(config_file)

        config["client.id"] = client_id

        self.producer = Producer(config)

    def produce_stream(
        self,
        message_stream: Generator[dict, None, None],
        key_extractor: Callable[[dict], str],
    ) -> None:
        """
        Consume a message generator and produce all messages to Kafka

        This method runs indefinitely, consuming from the generator and producing
        messages. The key_extractor function is called for each message to determine
        its key.

        Args:
            message_stream: Generator yielding message dictionaries
            key_extractor: Function that extracts the key from each message
                          Example: lambda msg: msg['id']
        """
        try:
            for message in message_stream:
                # Format key as JSON object for Flink DISTRIBUTED BY
                key_value = key_extractor(message)
                key_dict = {"station_id": key_value}
                key = json.dumps(key_dict).encode("utf-8")
                
                # Serialize value as JSON
                value = json.dumps(message).encode("utf-8")

                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    callback=lambda err, msg: print(f"Produced to {msg.topic()} (error={err})")
                )
        finally:
            self.producer.flush(30.0)
