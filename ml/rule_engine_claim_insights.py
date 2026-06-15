from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

claims = spark.table("claims_dev.gold.claims_enriched")
image_metadata = spark.table("claims_dev.silver.image_metadata")
predictions = spark.table("claims_dev.gold.damage_predictions")

claims_with_images = (
    claims
    .join(
        image_metadata.select("claim_no", "image_name"),
        on="claim_no",
        how="left"
    )
)

predictions_clean = (
    predictions
    .withColumn(
        "predicted_damage_score",
        F.when(F.col("predicted_label") == "ok", 0)
        .when(F.col("predicted_label") == "minor", 20)
        .when(F.col("predicted_label") == "major", 40)
        .otherwise(0)
    )
    .withColumn(
        "low_ml_confidence_flag",
        F.when(F.col("confidence_score") < 0.50, True).otherwise(False)
    )
)

claim_insights = (
    claims_with_images
    .join(predictions_clean, on="image_name", how="left")
    .withColumn(
        "high_claim_amount_flag",
        F.when(F.col("total") >= 10000, True).otherwise(False)
    )
    .withColumn(
        "low_confidence_data_flag",
        F.when(F.col("telematics_event_count").isNull(), True).otherwise(False)
    )
    .withColumn(
        "damage_claim_mismatch_flag",
        F.when(
            (F.col("predicted_label") == "ok") & (F.col("total") >= 5000),
            True
        )
        .when(
            (F.col("predicted_label") == "minor") & (F.col("total") >= 15000),
            True
        )
        .otherwise(False)
    )
    .withColumn(
        "risk_score",
        (
            F.when(F.col("fraud_risk_flag"), 30)
            + F.when(F.col("high_speed_flag"), 20)
            + F.when(F.col("high_claim_amount_flag"), 20)
            + F.when(F.col("damage_claim_mismatch_flag"), 20)
            + F.when(F.col("low_ml_confidence_flag"), 10)
        )
    )
    .withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= 80, "High")
        .when(F.col("risk_score") >= 45, "Medium")
        .otherwise("Low")
    )
    .withColumn(
        "manual_review_flag",
        F.when(F.col("risk_level").isin("High", "Medium"), True).otherwise(False)
    )
    .withColumn(
        "recommended_action",
        F.when(F.col("risk_level") == "High", "Send to investigation team")
        .when(F.col("risk_level") == "Medium", "Manual adjuster review")
        .otherwise("Auto-approve / standard review")
    )
    .withColumn("insight_created_at", F.current_timestamp())
)

(
    claim_insights.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("claims_dev.gold.claim_insights")
)