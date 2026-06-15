from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from configs.config_kenesis_to_bronze import (
    STREAM_NAME,
    AWS_REGION,
    TARGET_TABLE,
    CHECKPOINT_LOCATION,
)

spark = SparkSession.builder.getOrCreate()

vehicle_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("claim_no", StringType(), True),
    StructField("policy_no", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("acceleration", DoubleType(), True),
    StructField("brake_pressure", DoubleType(), True),
    StructField("impact_force", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

raw_stream = (
    spark.readStream
    .format("kinesis")
    .option("streamName", STREAM_NAME)
    .option("region", AWS_REGION)
    .option("initialPosition", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(data AS STRING) AS json_payload")
    .select(
        F.from_json(
            F.col("json_payload"),
            vehicle_event_schema,
        ).alias("event")
    )
    .select("event.*")
    .withColumn("ingested_at", F.current_timestamp())
)

(
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .toTable(TARGET_TABLE)
)