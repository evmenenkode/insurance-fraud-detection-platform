STREAM_NAME = "vehicle-sensor-events"
AWS_REGION = "us-east-1"

CATALOG = "claims_dev"
BRONZE_SCHEMA = "bronze"

TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.telematics"

CHECKPOINT_LOCATION = (
"dbfs:/checkpoints/vehicle_sensor_events"
)
