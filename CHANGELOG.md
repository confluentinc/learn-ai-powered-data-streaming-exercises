# Change Log

## v0.2.0 - 2026-01-12

- Added Exercise 02: Data Ingestion Service
  - Added DataIngestionService class that fetches real-time weather data from NOAA API
  - Polls 10 major U.S. airport weather stations
  - Uses Python generators for streaming data
  - Includes configurable poll interval and graceful error handling
  - Updated producer.py to use the ingestion service with KeyboardInterrupt handling
  - Added comprehensive tests for both DataIngestionService and producer

## v0.1.0 - 2025-11-19

- Added Exercise 01: Setup Confluent Cloud
  - Added Makefile with setup, install, run, test, and clean targets
  - Added pyproject.toml with project dependencies
  - Added README with quick start guide and project structure
  - Added basic "Hello World" style application in main.py

## v0.0.1 - 2025-11-18

- Added `exercise.sh` script for navigating exercise workflow
