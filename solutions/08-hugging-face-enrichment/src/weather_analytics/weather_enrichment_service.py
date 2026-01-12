from typing import Generator
from transformers import pipeline


class WeatherEnrichmentService:
    """Service for enriching weather data using AI zero-shot classification"""

    def __init__(self):
        """
        Initialize the enrichment service with a zero-shot classification model
        """
        self.classifier = pipeline("zero-shot-classification", model="valhalla/distilbart-mnli-12-1")
       
    def enrich(
        self, message_stream: Generator[dict, None, None]
    ) -> Generator[dict, None, None]:
        """
        Enrich weather observations with AI-classified weather conditions

        Args:
            message_stream: Generator yielding weather observation dictionaries

        Yields:
            dict: Enriched message with weather_condition field added
        """
        categories = ["Sunny", "Cloudy", "Rainy", "Snowy", "Windy"]
        
        for message in message_stream:
            desc = message.get("description")
            condition = "Unknown"
            
            if desc:
                try:
                    result = self.classifier(desc, categories)
                    condition = result['labels'][0]
                except Exception as e:
                    print(f"Classification failed: {e}")
            
            message["weather_condition"] = condition
            
            print(f"{message.get('station_id')}: {desc} → {condition}")

            yield message
