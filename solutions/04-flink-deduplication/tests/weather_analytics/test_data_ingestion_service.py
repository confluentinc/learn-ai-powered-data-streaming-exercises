from unittest.mock import patch, Mock
import pytest
import requests
import itertools

from weather_analytics.data_ingestion_service import DataIngestionService


class TestDataIngestionService:
    """Test suite for DataIngestionService"""

    def setup_method(self):
        """Setup that runs before each test method"""
        self.service = DataIngestionService(poll_interval=0)

    def response(self, response_data, status_code=200):
        """Create a response object for requests.get

        Args:
            response_data: The JSON data to return
            status_code: HTTP status code (default: 200)
        """
        response = requests.Response()
        response.status_code = status_code
        response._content = bytes(str(response_data).replace("'", '"'), "utf-8")

        return response

    def test_STATIONS_contains_expected_stations(self):
        """Test that DataIngestionService.STATIONS contains the expected stations"""
        assert DataIngestionService.STATIONS == [
            "KJFK",  # New York JFK
            "KLAX",  # Los Angeles
            "KORD",  # Chicago O'Hare
            "KDFW",  # Dallas
            "KATL",  # Atlanta
            "KDEN",  # Denver
            "KSFO",  # San Francisco
            "KSEA",  # Seattle
            "KBOS",  # Boston
            "KMIA",  # Miami
        ]

    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_fetches_all_stations(self, mock_get):
        """Test that DataIngestionService fetches all stations"""
        mock_get.return_value = self.response({"data": "test"})

        observations = list(
            itertools.islice(self.service.ingest(), len(DataIngestionService.STATIONS))
        )

        assert mock_get.call_count == len(DataIngestionService.STATIONS)

        for i, station_id in enumerate(DataIngestionService.STATIONS):
            call_args = mock_get.call_args_list[i]
            actual_url = call_args[0][0]

            assert station_id in actual_url
            assert call_args[1]["timeout"] == 10

    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_yields_observations(self, mock_get):
        """Test that DataIngestionService yields unique observations for each station"""
        mock_responses = [
            self.response({"stationId": station_id})
            for station_id in DataIngestionService.STATIONS
        ]
        mock_get.side_effect = mock_responses

        observations = list(
            itertools.islice(self.service.ingest(), len(DataIngestionService.STATIONS))
        )

        for observation, station_id in zip(observations, DataIngestionService.STATIONS):
            assert observation["stationId"] == station_id

    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_adds_station_id_to_observation(self, mock_get):
        """Test that DataIngestionService adds station_id field to each observation"""
        mock_responses = [
            self.response({"temperature": 72})
            for station_id in DataIngestionService.STATIONS
        ]
        mock_get.side_effect = mock_responses

        observations = list(
            itertools.islice(self.service.ingest(), len(DataIngestionService.STATIONS))
        )

        for observation, expected_station_id in zip(observations, DataIngestionService.STATIONS):
            assert "station_id" in observation
            assert observation["station_id"] == expected_station_id

    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_handles_exceptions(self, mock_get):
        """Test that service continues when some stations fail"""
        responses = []
        expected_stations = []

        for i, station_id in enumerate(DataIngestionService.STATIONS):
            if i % 2 == 0:  # Alternate between success and failure
                responses.append(
                    self.response({"stationId": station_id}, status_code=200)
                )
                expected_stations.append(station_id)
            else:
                requests.exceptions.RequestException("Station error")

        mock_get.side_effect = responses

        observations = list(
            itertools.islice(self.service.ingest(), len(expected_stations))
        )

        for observation, station_id in zip(observations, expected_stations):
            assert observation["stationId"] == station_id

    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_handles_http_errors(self, mock_get):
        """Test that service handles HTTP errors (4xx, 5xx) gracefully with partial failures"""
        responses = []
        expected_stations = []

        for i, station_id in enumerate(DataIngestionService.STATIONS):
            if i % 2 == 0:  # Alternate between success and failure
                responses.append(
                    self.response({"stationId": station_id}, status_code=200)
                )
                expected_stations.append(station_id)
            else:
                responses.append(self.response({"error": "Not Found"}, status_code=404))

        mock_get.side_effect = responses

        observations = list(
            itertools.islice(self.service.ingest(), len(expected_stations))
        )

        for observation, station_id in zip(observations, expected_stations):
            assert observation["stationId"] == station_id

    @patch("weather_analytics.data_ingestion_service.time.sleep")
    @patch("weather_analytics.data_ingestion_service.requests.get")
    def test_ingest_sleeps_between_cycles(self, mock_get, mock_sleep):
        """Test that service sleeps for poll_interval between polling cycles"""
        poll_interval = 30
        service = DataIngestionService(poll_interval=poll_interval)
        
        mock_get.return_value = self.response({"stationId": "TEST"})
        
        # Get all stations from first cycle, plus one from second cycle
        # This proves it completed a cycle and slept before starting the next
        observations = list(
            itertools.islice(service.ingest(), len(DataIngestionService.STATIONS) + 1)
        )
        
        # Should have called sleep once after processing all stations
        mock_sleep.assert_called_once_with(poll_interval)
