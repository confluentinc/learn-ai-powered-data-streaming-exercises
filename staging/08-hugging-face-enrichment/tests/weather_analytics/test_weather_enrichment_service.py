import pytest
from unittest.mock import patch, Mock
from weather_analytics.weather_enrichment_service import WeatherEnrichmentService


class TestWeatherEnrichmentService:
    """Test suite for WeatherEnrichmentService"""

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_initialization_should_create_pipeline(self, mock_pipeline):
        """Test service initialization with zero-shot classification model"""
        service = WeatherEnrichmentService(model_name="facebook/bart-large-mnli")

        mock_pipeline.assert_called_once_with("zero-shot-classification", model="facebook/bart-large-mnli")
        assert service.classifier == mock_pipeline.return_value

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_enrich_should_add_weather_condition_to_messages(self, mock_pipeline):
        """Test enriching messages with AI-classified weather conditions"""
        mock_pipeline.return_value.return_value = {
            "labels": ["Sunny", "Cloudy"],
            "scores": [0.85, 0.15]
        }
        service = WeatherEnrichmentService()

        messages = [
            {
                "station_id": "KJFK",
                "temperature_c": 16.0,
                "wind_speed_kmh": 15.0,
                "humidity_pct": 50.0,
                "description": "Clear skies",
            }
        ]

        enriched = list(service.enrich(messages))

        assert len(enriched) == 1
        assert "weather_condition" in enriched[0]
        assert enriched[0]["weather_condition"] == "Sunny"
        assert enriched[0]["station_id"] == "KJFK"
        assert enriched[0]["temperature_c"] == 16.0
        
        mock_pipeline.return_value.assert_called_once_with("Clear skies", ["Sunny", "Cloudy", "Rainy", "Snowy", "Windy"])

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_enrich_should_handle_missing_description(self, mock_pipeline):
        """Test enrichment with missing description field uses default"""
        mock_pipeline.return_value.return_value = {
            "labels": ["Sunny", "Cloudy"],
            "scores": [0.9, 0.1]
        }
        service = WeatherEnrichmentService()

        messages = [{"station_id": "KJFK", "temperature_c": 16.0}]

        enriched = list(service.enrich(messages))

        assert len(enriched) == 1
        assert "weather_condition" in enriched[0]
        
        mock_pipeline.return_value.assert_called_once_with("clear", ["Sunny", "Cloudy", "Rainy", "Snowy", "Windy"])

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_enrich_should_handle_classification_error(self, mock_pipeline):
        """Test that classification errors result in 'Unknown' condition"""
        mock_classifier = Mock()
        mock_classifier.side_effect = Exception("Model error")
        mock_pipeline.return_value = mock_classifier

        service = WeatherEnrichmentService()

        messages = [{"station_id": "KJFK", "description": "Clear"}]

        enriched = list(service.enrich(messages))

        assert len(enriched) == 1
        assert enriched[0]["weather_condition"] == "Unknown"

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_enrich_should_preserve_original_fields(self, mock_pipeline):
        """Test that enrichment preserves all original fields"""
        mock_pipeline.return_value.return_value = {
            "labels": ["Cloudy", "Rainy"],
            "scores": [0.8, 0.2]
        }
        service = WeatherEnrichmentService()

        messages = [{
            "station_id": "KJFK",
            "temperature_c": 16.0,
            "wind_speed_kmh": 15.0,
            "observation_time": "2025-11-03T10:00:00Z",
            "description": "Overcast"
        }]

        enriched = list(service.enrich(messages))

        assert enriched[0]["station_id"] == "KJFK"
        assert enriched[0]["temperature_c"] == 16.0
        assert enriched[0]["wind_speed_kmh"] == 15.0
        assert enriched[0]["observation_time"] == "2025-11-03T10:00:00Z"
        assert enriched[0]["description"] == "Overcast"
        assert "weather_condition" in enriched[0]
        assert enriched[0]["weather_condition"] == "Cloudy"

    @patch("weather_analytics.weather_enrichment_service.pipeline")
    def test_enrich_should_process_multiple_messages(self, mock_pipeline):
        """Test processing stream of multiple messages"""
        mock_pipeline.return_value.return_value = {
            "labels": ["Sunny", "Cloudy"],
            "scores": [0.75, 0.25]
        }
        service = WeatherEnrichmentService()

        messages = [
            {"station_id": "KJFK", "temperature_c": 16.0, "description": "Clear"},
            {"station_id": "KLAX", "temperature_c": 22.0, "description": "Fair"},
            {"station_id": "KORD", "temperature_c": 12.0, "description": "Bright"},
        ]

        enriched = list(service.enrich(messages))

        assert len(enriched) == 3
        assert all("weather_condition" in msg for msg in enriched)
        assert all(msg["weather_condition"] == "Sunny" for msg in enriched)
        assert enriched[0]["station_id"] == "KJFK"
        assert enriched[1]["station_id"] == "KLAX"
        assert enriched[2]["station_id"] == "KORD"

