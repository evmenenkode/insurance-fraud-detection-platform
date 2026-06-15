CATALOG = "claims_dev"
BRONZE_SCHEMA = "bronze"

S3_BASE_PATH = "s3://smart-claims-lakehouse/landing"

IMAGE_METADATA_PATH = f"{S3_BASE_PATH}/metadata/"
ACCIDENT_IMAGES_PATH = f"{S3_BASE_PATH}/images/"
TRAINING_IMAGES_PATH = f"{S3_BASE_PATH}/training_imgs/"

IMAGE_METADATA_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.image_metadata"
ACCIDENT_IMAGES_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.accident_images"
TRAINING_IMAGES_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.training_images"