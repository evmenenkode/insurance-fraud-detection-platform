from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE SCHEMA IF NOT EXISTS claims_dev.gold
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS claims_dev.gold.damage_predictions (
    image_name STRING,
    actual_label STRING,
    predicted_label STRING,
    confidence_score DOUBLE,
    prediction_ts TIMESTAMP
)
USING DELTA
""")