"""
Data Ingestion Service
Handles fetching weather data from NOAA API and streaming to Kafka
"""

import time
import requests


class DataIngestionService:
    """Service for ingesting weather data from NOAA and producing to Kafka"""

    STATIONS = [
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

    def __init__(self, poll_interval=60):
        """
        Initialize the data ingestion service

        Args:
            poll_interval: Seconds to wait between polling cycles (default: 60)
        """
        self.poll_interval = poll_interval

    def ingest(self):
        """
        Continuously ingest weather data and yield observations as a stream

        This is an infinite generator that polls all stations, yields their
        observations, then waits before polling again.

        Yields:
            dict: Weather observation data for each station
        """
        while True:
            for station_id in self.STATIONS:
                print(f"Fetching observations for {station_id}...")
                url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
                
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    
                    data = response.json()
                    data['station_id'] = station_id
                    
                    yield data
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching data for {station_id}: {e}")

            time.sleep(self.poll_interval)
