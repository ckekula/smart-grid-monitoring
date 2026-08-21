from pathlib import Path

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "smart-meter-readings"
DATASET_PATH = Path(__file__).resolve().parent / "smart_grid_dataset_new.csv"
