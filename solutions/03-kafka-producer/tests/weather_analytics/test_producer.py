from unittest.mock import patch, Mock

from weather_analytics.producer import main


class TestProducer:
    """Test suite for producer application"""

    @patch("weather_analytics.producer.print")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_prints_observations(self, mock_service_class, mock_print):
        """Test that main() prints each observation from the ingestion service"""
        test_observations = [
            {"station_id": "KJFK", "temperature": 72},
            {"station_id": "KLAX", "temperature": 85},
            {"station_id": "KORD", "temperature": 65},
        ]

        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = iter(test_observations)
        mock_service_class.return_value = mock_service_instance

        main()

        mock_service_class.assert_called_once_with(poll_interval=60)

        mock_service_instance.ingest.assert_called_once()

        print_calls = [call[0][0] for call in mock_print.call_args_list if call[0]]

        for observation in test_observations:
            assert observation in print_calls

    @patch("weather_analytics.producer.print")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_creates_service_with_60_second_interval(self, mock_service_class, mock_print):
        """Test that main() creates DataIngestionService with 60-second poll interval"""
        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = iter([])
        mock_service_class.return_value = mock_service_instance

        main()

        mock_service_class.assert_called_once_with(poll_interval=60)

    @patch("weather_analytics.producer.print")
    @patch("weather_analytics.producer.DataIngestionService")
    def test_main_handles_keyboard_interrupt_gracefully(self, mock_service_class, mock_print):
        """Test that main() catches KeyboardInterrupt and exits gracefully"""
        def interrupt_generator():
            yield {"station_id": "KJFK", "temperature": 72}
            raise KeyboardInterrupt()

        mock_service_instance = Mock()
        mock_service_instance.ingest.return_value = interrupt_generator()
        mock_service_class.return_value = mock_service_instance

        # Should not raise an exception
        main()

        # Verify "Producer stopped." was printed
        print_calls = [str(call) for call in mock_print.call_args_list]
        assert any("Producer stopped" in call for call in print_calls)

