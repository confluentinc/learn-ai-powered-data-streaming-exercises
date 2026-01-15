.PHONY: help setup-confluent install clean test produce consume
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
VENV := venv
BIN := $(VENV)/bin
PYTHON_VENV := $(BIN)/python
PIP := $(BIN)/pip

help:
	@echo "Weather Analytics Workshop - Makefile Commands"
	@echo "================================================"
	@echo ""
	@echo "Setup Commands:"
	@echo "  make setup-confluent - Set up Confluent Cloud environment"
	@echo "  make install         - Install all dependencies"
	@echo "  make clean           - Remove virtual environment and cache files"
	@echo ""
	@echo "Development Commands:"
	@echo "  make produce         - Run the weather data producer"
	@echo "  make consume         - Run the weather data consumer"
	@echo "  make test            - Run all tests"
	@echo ""

setup-confluent:
	@echo "Creating Confluent Cloud environment..."
	confluent quickstart \
		--environment-name "weather_analytics" \
		--kafka-cluster-name "data_streams" \
		--compute-pool-name "stream_processing" \
		--create-kafka-key \
		--create-flink-key \
		--create-sr-key \
		--kafka-librdkafka-properties-file config/kafka-librdkafka.properties \
		--flink-properties-file config/flink.properties \
		--schema-registry-properties-file config/schema-registry.properties
	@echo "Confluent Cloud environment created successfully!"

install:
	@echo "Setting up virtual environment..."
	@$(PYTHON) -m venv $(VENV)
	@echo "Installing dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@echo "Dependencies installed successfully!"

clean:
	@echo "Cleaning up..."
	rm -rf $(VENV)
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf *.pyc
	rm -rf **/__pycache__
	rm -rf **/*.pyc
	rm -rf src/*.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Cleanup complete!"

test:
	@echo "Running tests..."
	$(PYTHON_VENV) -m pytest tests/ -v -s
	@echo "Tests complete!"

produce:
	@rm -rf **/__pycache__ **/*.pyc
	@echo "Starting weather data producer..."
	$(PYTHON_VENV) -m weather_analytics.producer

consume:
	@rm -rf **/__pycache__ **/*.pyc
	@echo "Starting weather data consumer..."
	$(PYTHON_VENV) -m weather_analytics.consumer
