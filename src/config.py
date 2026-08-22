import os
from pathlib import Path

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "smart-meter-readings")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", Path(__file__).resolve().parent / "smart-grid-checkpoint")
DATASET_PATH = os.environ.get("DATASET_PATH", Path(__file__).resolve().parent / "smart_grid_dataset_new.csv")

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/smart_grid")
POSTGRES_USER = os.getenv("POSTGRES_USER", "smart_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "smart_admin")
POSTGRES_DRIVER = "org.postgresql.Driver"
ZONE_METRICS_TABLE = "smart_grid.realtime_zone_metrics"
