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

## Create the Kafka Topic
```bash
kafka-topics --create \
  --topic smart-meter-readings \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1
```

## Add Winutils for Hadoop
1. Go to: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin
2. Download winutils.exe and hadoop.dll
3. Paste them in C:\hadoop\bin

Make sure you add C:\hadoop\bin to PATH

# Run the Application

## Run Docker compose
```bash
docker compose up -d
```

> [!NOTE]
> Fully reset state before each test run:
```bash
docker compose down
docker compose up -d
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic smart-meter-readings
docker compose restart spark-submit
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