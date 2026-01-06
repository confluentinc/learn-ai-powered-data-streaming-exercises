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
        # TODO: Initialize the zero-shot classification pipeline
        # - Use the pipeline() function from transformers
        # - Set the task to "zero-shot-classification"
        # - Pass the model_name parameter as the model
        # - Store the result as self.classifier
        ...
       
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
        
        # TODO: Implement the enrichment logic
        # - Iterate over messages from message_stream
        # - Get the "description" field from the message
        # - Create a condition variable set to "Unknown"
        # - If description exists, try to classify it using self.classifier
        #   with the categories, and extract result['labels'][0]
        # - If classification fails, print the error (condition stays "Unknown")
        # - Add the condition as "weather_condition" field to the message
        # - Yield the message
        ...
