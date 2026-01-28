from unittest.mock import patch, Mock, ANY

from weather_analytics.producer import main


class TestProducer:
    """Test suite for producer application"""

    @patch("weather_analytics.producer.KafkaProducer")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_initializes_services_correctly(self, mock_service_class, mock_producer_class):
        """Test that main() creates DataIngestionService and KafkaProducer with correct parameters"""
        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = iter([])
        mock_service_class.return_value = mock_service_instance

        mock_producer_instance = Mock()
        mock_producer_class.return_value = mock_producer_instance

        main()

        mock_service_class.assert_called_once_with(poll_interval=60)
        mock_producer_class.assert_called_once_with(
            config_file="config/kafka-librdkafka.properties",
            topic="raw_weather_observations",
            client_id="RawWeatherProducer"
        )

    @patch("weather_analytics.producer.KafkaProducer")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_streams_observations_to_kafka(self, mock_service_class, mock_producer_class):
        """Test that main() passes the ingestion stream to the Kafka producer"""
        test_observations = [
            {"station_id": "KJFK", "temperature": 72},
            {"station_id": "KLAX", "temperature": 85},
            {"station_id": "KORD", "temperature": 65},
        ]

        mock_generator = iter(test_observations)

        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = mock_generator
        mock_service_class.return_value = mock_service_instance

        mock_producer_instance = Mock()
        mock_producer_class.return_value = mock_producer_instance

        main()

        mock_service_instance.ingest.assert_called_once()
        mock_producer_instance.produce_stream.assert_called_once_with(mock_generator, ANY)

    @patch("weather_analytics.producer.KafkaProducer")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_uses_correct_key_extractor(self, mock_service_class, mock_producer_class):
        """Test that main() provides a key extractor that extracts station_id from messages"""
        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = iter([])
        mock_service_class.return_value = mock_service_instance

        mock_producer_instance = Mock()
        mock_producer_class.return_value = mock_producer_instance

        main()

        call_args = mock_producer_instance.produce_stream.call_args
        key_extractor = call_args[0][1]

        assert key_extractor({"station_id": "KJFK"}) == "KJFK"
        assert key_extractor({"station_id": "KLAX"}) == "KLAX"
        assert key_extractor({"some_key": "some_value"}) == "unknown"

    @patch("weather_analytics.producer.KafkaProducer")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_handles_keyboard_interrupt_gracefully(self, mock_service_class, mock_producer_class):
        """Test that main() catches KeyboardInterrupt and exits gracefully"""
        def interrupt_generator():
            yield {"station_id": "KJFK", "temperature": 72}
            raise KeyboardInterrupt()

        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = interrupt_generator()
        mock_service_class.return_value = mock_service_instance

        mock_producer_instance = Mock()
        mock_producer_instance.produce_stream.side_effect = KeyboardInterrupt()
        mock_producer_class.return_value = mock_producer_instance

        # Should not raise an exception
        main()

