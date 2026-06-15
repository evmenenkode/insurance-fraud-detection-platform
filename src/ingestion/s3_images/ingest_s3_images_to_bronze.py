from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, regexp_extract
from configs.config_s3_to_bronze import (
    IMAGE_METADATA_PATH,
    ACCIDENT_IMAGES_PATH,
    TRAINING_IMAGES_PATH,
    IMAGE_METADATA_TABLE,
    ACCIDENT_IMAGES_TABLE,
    TRAINING_IMAGES_TABLE,
)


def read_metadata(spark: SparkSession):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(IMAGE_METADATA_PATH)
        .withColumn("ingested_at", current_timestamp())
    )


def read_images(spark: SparkSession, source_path: str):
    return (
        spark.read
        .format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(source_path)
        .withColumn("image_name", regexp_extract("path", r"([^/]+)$", 1))
        .withColumn("ingested_at", current_timestamp())
    )


def write_bronze(df, table_name: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )


def main():
    spark = SparkSession.builder.getOrCreate()

    image_metadata_df = read_metadata(spark)
    accident_images_df = read_images(spark, ACCIDENT_IMAGES_PATH)
    training_images_df = read_images(spark, TRAINING_IMAGES_PATH)

    write_bronze(image_metadata_df, IMAGE_METADATA_TABLE)
    write_bronze(accident_images_df, ACCIDENT_IMAGES_TABLE)
    write_bronze(training_images_df, TRAINING_IMAGES_TABLE)


if __name__ == "__main__":
    main()