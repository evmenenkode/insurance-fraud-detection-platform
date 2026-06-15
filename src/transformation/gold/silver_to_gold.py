from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "claims_dev"
SILVER = "silver"
GOLD = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")


def write_gold(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{GOLD}.{table_name}")
    )


# -------------------------
# 1. Aggregated Telematics
# -------------------------

telematics = spark.table(f"{CATALOG}.{SILVER}.telematics")

aggregated_telematics = (
    telematics
    .groupBy("chassis_no")
    .agg(
        F.avg("speed").alias("avg_speed"),
        F.max("speed").alias("max_speed"),
        F.avg("latitude").alias("avg_latitude"),
        F.avg("longitude").alias("avg_longitude"),
        F.count("*").alias("telematics_event_count")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

write_gold(aggregated_telematics, "aggregated_telematics")


# -------------------------
# 2. Customer + Policy + Claims
# -------------------------

customers = spark.table(f"{CATALOG}.{SILVER}.customers")
policy = spark.table(f"{CATALOG}.{SILVER}.policy")
claims = spark.table(f"{CATALOG}.{SILVER}.claims")

customer_policy_claims = (
    claims.alias("c")
    .join(
        policy.alias("p"),
        F.col("c.policy_no") == F.col("p.policy_no"),
        "left"
    )
    .join(
        customers.alias("cu"),
        F.col("p.cust_id") == F.col("cu.customer_id"),
        "left"
    )
    .select(
        F.col("c.claim_no"),
        F.col("c.policy_no"),
        F.col("p.cust_id").alias("customer_id"),
        F.col("cu.name").alias("customer_name"),
        F.col("cu.date_of_birth"),
        F.col("cu.borough"),
        F.col("cu.neighborhood"),
        F.col("cu.zip_code"),
        F.col("p.policytype"),
        F.col("p.make"),
        F.col("p.model"),
        F.col("p.model_year"),
        F.col("p.chassis_no"),
        F.col("p.use_of_vehicle"),
        F.col("p.product"),
        F.col("p.sum_insured"),
        F.col("p.premium"),
        F.col("p.deductible"),
        F.col("c.claim_date"),
        F.col("c.incident_date"),
        F.col("c.incident_hour"),
        F.col("c.collision_type"),
        F.col("c.severity"),
        F.col("c.injury"),
        F.col("c.property"),
        F.col("c.vehicle"),
        F.col("c.total"),
        F.col("c.suspicious_activity")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

write_gold(customer_policy_claims, "customer_policy_claims")


# -------------------------
# 3. Claims + Telematics
# -------------------------

claims_enriched = (
    customer_policy_claims.alias("cpc")
    .join(
        aggregated_telematics
        .drop("gold_updated_at")
        .withColumnRenamed("chassis_no", "telematics_chassis_no")
        .alias("t"),
        F.col("cpc.chassis_no") == F.col("t.telematics_chassis_no"),
        "left"
    )
    .drop("telematics_chassis_no")
    .withColumn(
        "claim_amount_bucket",
        F.when(F.col("total") < 1000, "Low")
        .when(F.col("total") < 5000, "Medium")
        .when(F.col("total") < 15000, "High")
        .otherwise("Severe")
    )
    .withColumn(
        "high_speed_flag",
        F.when(F.col("max_speed") >= 100, F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn(
        "fraud_risk_flag",
        F.when(
            F.col("suspicious_activity") | F.col("high_speed_flag"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

write_gold(claims_enriched, "claims_enriched")

# -------------------------
# 4. Dashboard Summary
# -------------------------

claims_dashboard_summary = (
    claims_enriched
    .groupBy(
        "borough",
        "severity",
        "claim_amount_bucket"
    )
    .agg(
        F.countDistinct("claim_no").alias("claim_count"),
        F.sum("total").alias("total_claim_amount"),
        F.avg("total").alias("avg_claim_amount"),
        F.avg("premium").alias("avg_premium"),
        F.sum(F.col("fraud_risk_flag").cast("int")).alias("fraud_risk_count")
    )
    .withColumn("gold_updated_at", F.current_timestamp())
)

write_gold(claims_dashboard_summary, "claims_dashboard_summary")