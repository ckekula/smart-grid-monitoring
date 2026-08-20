# Smart Grid Energy Monitoring & Billing Data Pipeline

This pipeline provides real-time visibility into grid load and renewable
contribution from smart meters, reconciled daily against tariff and
billing data produced once a day.

# Getting Started

## Requirements
1. Python >= 3.12
2. uv
3. Docker

## Run Docker compose
```bash
docker compose up -d
```

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