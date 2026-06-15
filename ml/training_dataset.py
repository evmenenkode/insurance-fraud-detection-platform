from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

training_images = spark.table("claims_dev.silver.training_images")

training_dataset = (
    training_images
    .select(
        "image_name",
        "path",
        "content",
        "damage_label",
        "file_size_kb"
    )
    .where(F.col("damage_label").isNotNull())
    .withColumn("dataset_created_at", F.current_timestamp())
)

(
    training_dataset.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("claims_dev.gold.training_dataset")
)