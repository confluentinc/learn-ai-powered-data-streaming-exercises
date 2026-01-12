# Weather Analytics Workshop - Exercises

Real-Time Weather Analytics with AI using Confluent Kafka, Apache Flink, and Hugging Face.

## Quick Start

### 1. Setup Confluent Cloud Environment

```bash
make setup-confluent
```

This will create:
- Confluent Cloud environment named "weather_analytics"
- Kafka cluster named "data_streams"
- Flink compute pool named "stream_processing"
- API keys for Kafka, Flink, and Schema Registry
- Configuration files automatically populated in `config/` directory

**Note:** You'll need the [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) installed and logged in.

### 2. Install Dependencies

```bash
make install
```

This will create a Python virtual environment and install all required dependencies.

### 3. Run the Application

```bash
make run
```

### 4. Run Tests

```bash
make test
```

### 5. Clean the Local Environment

```bash
make clean
```

## Project Structure

```
exercises/
├── Makefile                         # Build and development commands
├── pyproject.toml                   # Python project configuration and dependencies
├── README.md                        # This file
├── config/                          # Configuration files (created by setup-confluent)
│   ├── kafka-librdkafka.properties  # Kafka connection settings
│   ├── flink.properties             # Flink configuration
│   └── schema-registry.properties   # Schema Registry settings
├── src/                             
│   └── weather_analytics/           # Application source code
└── tests/
    └── weather_analytics/           # Test files
```