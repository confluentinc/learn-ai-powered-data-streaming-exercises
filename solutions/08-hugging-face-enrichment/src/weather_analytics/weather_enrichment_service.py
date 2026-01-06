from typing import Generator
from transformers import pipeline


class WeatherEnrichmentService:
    """Service for enriching weather data using AI zero-shot classification"""

    def __init__(self, model_name: str = "facebook/bart-large-mnli"):
        """
        Initialize the enrichment service with a zero-shot classification model

        Args:
            model_name: Hugging Face model for zero-shot classification
                       Default: facebook/bart-large-mnli (robust, ~1.6GB)
        """
        self.classifier = pipeline("zero-shot-classification", model=model_name)
       
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
            enriched = message.copy()
            desc = message.get("description") or "clear"
            
            try:
                result = self.classifier(desc, categories)
                condition = result['labels'][0]
            except Exception as e:
                print(f"Classification failed: {e}")
                condition = "Unknown"
            
            enriched["weather_condition"] = condition
            
            print(f"{message.get('station_id')}: {desc} → {condition}")

            yield enriched

