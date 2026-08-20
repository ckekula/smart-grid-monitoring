KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "smart-meter-readings"

DATASET_PATH = "./smart_grid_dataset.csv"

NUM_METERS = 100 # Number of simulated households/meters.
NUM_ZONES = 3 # Number of grid zones.

# 1 simulated day = 1 minute (60 seconds)
SIMULATED_MINUTE_SECONDS = str(60 / 1440)

PRODUCER_CLIENT_ID = "smart-grid-meter-producer"
PRODUCER_ACKS = "all"

# Maximum number of records to send before waiting for delivery callbacks.
DELIVERY_POLL_INTERVAL = 0.1
