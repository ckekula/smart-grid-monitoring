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

For full instructions for first-time setup, refer the [Setup Guide](SETUP.md)

# Run the Application

## Run Docker compsoe
```bash
docker compose up -d
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

## Run the FastAPI Application

```bash
cd src
uvicorn main:app --reload
```

You can test the API with:
```bash
GET /api/v1/billing/current
GET /api/v1/billing/H001
GET /api/v1/billing/H001/history
GET /api/v1/billing/daily/2024-01-02
```

