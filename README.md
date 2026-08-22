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
.venv\Script\activate
```

## Install Dependencies
```bash
uv sync
```

# Run the Application

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
rm -rf src/smart-grid-checkpoint/
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
docker compose exec postgres psql -U smart_admin -d airflow
CREATE DATABASE smart_grid;
```


## Log Spark Structured Streaming Consumer outputs
```bash
docker compose logs -f spark-submit
```

## Run the producer
```bash
cd src
python producer.py
```

```bash
uvicorn main:app --reload
```