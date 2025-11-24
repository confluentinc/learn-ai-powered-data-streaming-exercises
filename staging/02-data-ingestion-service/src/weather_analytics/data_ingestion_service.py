"""
Data Ingestion Service
Handles fetching weather data from NOAA API and streaming to Kafka
"""

import time
import requests


class DataIngestionService:
    """Service for ingesting weather data from NOAA and producing to Kafka"""

    # TODO: Define STATIONS as a class variable containing a list of 10 station IDs:
    # KJFK, KLAX, KORD, KDFW, KATL, KDEN, KSFO, KSEA, KBOS, KMIA
    STATIONS = []

    def __init__(self, poll_interval=60):
        """
        Initialize the data ingestion service

        Args:
            poll_interval: Seconds to wait between polling cycles (default: 60)
        """
        # TODO: Store the poll_interval as an instance variable
        pass

    def ingest(self):
        """
        Continuously ingest weather data and yield observations as a stream

        This is an infinite generator that polls all stations, yields their
        observations, then waits before polling again.

        Yields:
            dict: Weather observation data for each station
        """
        # TODO: Implement an infinite loop that:
        #   1. Loops through each station in STATIONS
        #   2. Fetches data from the NOAA API:
        #      URL: https://api.weather.gov/stations/{station_id}/observations/latest
        #   3. Adds the station_id to the response data
        #   4. Yields the data
        #   5. Handles exceptions gracefully (print error, continue to next station)
        #   6. Sleeps for poll_interval seconds after processing all stations
        pass

