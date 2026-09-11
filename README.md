# Nairobi Air Quality Airflow

A production-style Airflow pipeline for ingesting, validating, and storing Nairobi air quality measurements from the OpenAQ API into Postgres.

## Overview

This project collects air quality data for locations in Nairobi, normalizes the response payloads, filters invalid measurements, and loads the cleaned results into a warehouse table using an idempotent upsert pattern.

The pipeline is implemented in [dags/nairobi_air_quality.py](dags/nairobi_air_quality.py) and is designed around a simple ETL workflow:

1. Discover available locations in Nairobi.
2. Fetch sensors for each location.
3. Retrieve recent measurements for each sensor.
4. Flatten and validate the measurements.
5. Upsert the verified data into Postgres.

## Architecture

- Airflow DAG: [dags/nairobi_air_quality.py](dags/nairobi_air_quality.py)
- OpenAQ client: [src/nairobi_air_quality_airflow/api/openaq_client.py](src/nairobi_air_quality_airflow/api/openaq_client.py)
- Validation logic: [src/nairobi_air_quality_airflow/validation/validators.py](src/nairobi_air_quality_airflow/validation/validators.py)
- Postgres helper: [src/nairobi_air_quality_airflow/database/postgres.py](src/nairobi_air_quality_airflow/database/postgres.py)
- Database schema: [sql/create_tables.sql](sql/create_tables.sql)
- Tests: [tests](tests)

## Key Features

- API-driven ingestion from the OpenAQ service
- Dynamic task expansion for locations and sensors
- Validation of required metadata and numeric values
- Negative-value rejection and malformed record filtering
- Idempotent Postgres upsert keyed by sensor and timestamp
- Test coverage for validation, DAG structure, and Postgres behavior

## Tech Stack

- Python 3.13+
- Apache Airflow 3.3.1
- Postgres
- OpenAQ API
- Pytest
- uv for dependency management

## Project Structure

```text
.
├── dags/
│   └── nairobi_air_quality.py
├── src/
│   └── nairobi_air_quality_airflow/
│       ├── api/
│       ├── config/
│       ├── database/
│       ├── models/
│       ├── utils/
│       ├── validation/
│       └── __init__.py
├── sql/
│   └── create_tables.sql
├── tests/
│   ├── test_dag_pipeline.py
│   ├── test_openaq_client.py
│   ├── test_postgres_upsert.py
│   └── test_validators.py
├── docker-compose.yaml
├── pyproject.toml
├── README.md
└── .gitignore
```

## Prerequisites

Before running the project locally, ensure you have:

- Python 3.13 or newer
- uv installed
- Access to a Postgres instance
- An OpenAQ API key configured in Airflow connections

## Local Setup

### Option 1: Native Python Environment

1. Clone the repository.
2. Install dependencies:

```bash
uv sync --group dev
```

3. Ensure your Airflow environment has the required connections:

- `openaq_api` with the API host and `api_key`
- `postgres_default` for your Postgres database

4. Create the target warehouse table using the schema in [sql/create_tables.sql](sql/create_tables.sql).

### Option 2: Docker Compose

This project includes a containerized Airflow stack using Docker Compose.

1. Copy the sample environment file:

```bash
cp .env.example .env
```

2. Update the values in `.env` as needed.
3. Start the stack:

```bash
docker compose up --build
```

4. Open the Airflow UI in your browser:

```text
http://localhost:8080
```

5. Log in with the default admin credentials created by the stack:

- Username: `admin`
- Password: `admin`

The Docker stack includes:

- Postgres database
- Airflow webserver
- Airflow scheduler
- Mounted DAG and source directories for local development

> Docker is the recommended option for quickly running the full pipeline in a reproducible environment.

## Running the DAG

From the project root, initialize and start Airflow as needed for your environment, then trigger the DAG named `nairobi_air_quality`.

The DAG executes the pipeline defined in [dags/nairobi_air_quality.py](dags/nairobi_air_quality.py):

- `extract_locations`
- `extract_sensors`
- `flatten_sensors`
- `extract_measurements`
- `flatten_measurements`
- `validate_measurements`
- `load_measurements`

## Database Model

The warehouse table stores raw measurement data with a natural key based on sensor and measurement timestamp:

- `sensor_id`
- `measurement_timestamp`
- `location_id`
- `location_name`
- `parameter`
- `unit`
- `value`
- `loaded_timestamp`

This design keeps the ingestion idempotent and helps prevent duplicates when the pipeline is re-run.

## Validation Rules

Measurements are only retained when they contain all required fields and valid values:

- non-null `location_id`
- non-null `sensor_id`
- non-null `parameter`
- non-null `unit`
- non-null `measurement_timestamp`
- numeric `value`
- `value >= 0`

Invalid records are counted and discarded before the upsert stage.

## Testing

Run the test suite with:

```bash
uv run pytest -q
```

The project includes tests for:

- measurement validation rules
- API client behavior
- DAG metadata and structure
- Postgres upsert row generation and execution

## Notes

This project is structured for extension: new pollutants, additional geographies, or a broader warehouse model can be added without changing the ingestion pattern. The current implementation is intentionally simple and easy to operate in a local or staging Airflow deployment.

## License

This project is intended for internal or personal use unless otherwise specified by the repository owner.
