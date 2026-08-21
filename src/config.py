import os
from pathlib import Path

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "smart-meter-readings")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", Path(__file__).resolve().parent / "smart-grid-checkpoint")
DATASET_PATH = os.environ.get("DATASET_PATH", Path(__file__).resolve().parent / "smart_grid_dataset_new.csv")
