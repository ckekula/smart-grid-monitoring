# Smart Grid Energy Monitoring & Billing Data Pipeline

This pipeline provides real-time visibility into grid load and renewable
contribution from smart meters, reconciled daily against tariff and
billing data produced once a day.

# Getting Started

## Requirements
1. Python >= 3.12
2. uv
3. Docker

## Activate virtual environment
```bash
uv venv --python 3.12.12
# for Linux/macOS:
source .venv/bin/activate
# for Windows
.venv\Scripts\Activate.ps1
```

## Install Dependencies
```bash
uv sync
```

# Setting Up the Pipeline

## Run Docker compose
```bash
docker compose up -d
```

## Create the Kafka Topic
```bash
docker compose exec kafka kafka-topics --create \
  --topic smart-meter-readings \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1
```

> [!NOTE]
> For a fresh run, fully reset state before the run:

1. Delete Spark's streaming checkpoint
```bash
rm -rf spark-checkpoints/
```

2. Delete and recreate the Kafka topic
```bash
docker compose down
docker compose up -d
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic smart-meter-readings

docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic smart-meter-readings \
  --partitions 2 \
  --replication-factor 1
```

3. Restart spark-submit container
```bash
docker compose restart spark-submit
docker compose logs -f spark-submit
```

## Create the Postgres Sink
```bash
docker compose exec postgres psql -U airflow -d airflow
CREATE DATABASE smart_grid;
exit
docker compose exec postgres psql -U airflow -d smart_grid
CREATE SCHEMA IF NOT EXISTS smart_grid;
```

### Create the Metrics table
```bash
CREATE TABLE smart_grid.realtime_zone_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,

    grid_zone VARCHAR(20) NOT NULL,

    total_load_kw DOUBLE PRECISION NOT NULL,
    total_consumption_kwh DOUBLE PRECISION NOT NULL,

    solar_generation_kwh DOUBLE PRECISION NOT NULL,
    wind_generation_kwh DOUBLE PRECISION NOT NULL,
    renewable_generation_kwh DOUBLE PRECISION NOT NULL,

    grid_import_kwh DOUBLE PRECISION NOT NULL,
    grid_export_kwh DOUBLE PRECISION NOT NULL,

    renewable_contribution_pct DOUBLE PRECISION NOT NULL,

    meter_reading_count INTEGER NOT NULL,
    meter_count INTEGER NOT NULL,
    household_count INTEGER NOT NULL,

    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (window_start, grid_zone)
);
```

### Create Indexes
```bash
CREATE INDEX idx_realtime_zone_metrics_zone_time
ON smart_grid.realtime_zone_metrics (
    grid_zone,
    window_end DESC
);

CREATE INDEX idx_realtime_zone_metrics_window
ON smart_grid.realtime_zone_metrics (
    window_start DESC
);

exit
```

## Log Spark Structured Streaming Consumer outputs
```bash
docker compose logs -f spark-submit
```

## Run the producer
```bash
cd src
uv run python producer.py
```

View the Kafka UI at: `localhost:8080` and go to the messages tab.
You should see the number of messages increase.

## Create the batch table

### connect to the smart gird database

```bash
docker compose exec postgres psql -U airflow -d smart_grid
```

### create the daily_household_billing table:
```bash
CREATE TABLE smart_grid.daily_household_billing (
    billing_date DATE NOT NULL,
    household_id VARCHAR(50) NOT NULL,
    grid_zone VARCHAR(20) NOT NULL,

    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    solar_generation_kwh DOUBLE PRECISION NOT NULL,
    wind_generation_kwh DOUBLE PRECISION NOT NULL,
    renewable_generation_kwh DOUBLE PRECISION NOT NULL,

    grid_import_kwh DOUBLE PRECISION NOT NULL,
    grid_export_kwh DOUBLE PRECISION NOT NULL,

    average_tariff_usd_kwh DOUBLE PRECISION NOT NULL,
    energy_charge_usd DOUBLE PRECISION NOT NULL,

    meter_count INTEGER NOT NULL,
    reading_count INTEGER NOT NULL,

    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (billing_date, household_id)
);
```

### Create the Indexes
```bash
CREATE INDEX idx_daily_billing_date
ON smart_grid.daily_household_billing (
    billing_date DESC
);

CREATE INDEX idx_daily_billing_household
ON smart_grid.daily_household_billing (
    household_id,
    billing_date DESC
);

CREATE INDEX idx_daily_billing_zone_date
ON smart_grid.daily_household_billing (
    grid_zone,
    billing_date DESC
);
```

# Run the Application

```bash
uvicorn main:app --reload
```