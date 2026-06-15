from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "claims_dev"
BRONZE = "bronze"
SILVER = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")


def drop_if_exists(df, columns):
    existing = [c for c in columns if c in df.columns]
    return df.drop(*existing)


def write_silver(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SILVER}.{table_name}")
    )


def clean_date(column_name):
    cleaned = F.nullif(
        F.trim(F.col(column_name).cast("string")),
        F.lit("")
    )

    return F.coalesce(
        F.try_to_date(cleaned, F.lit("yyyy-MM-dd")),
        F.try_to_date(cleaned, F.lit("MM/dd/yyyy")),
        F.try_to_date(cleaned, F.lit("dd/MM/yyyy")),
        F.try_to_date(cleaned, F.lit("dd-MM-yyyy")),
        F.try_to_date(cleaned, F.lit("M/d/yyyy")),
    )


# -------------------------
# Customers
# -------------------------

customers_bronze = spark.table(f"{CATALOG}.{BRONZE}.customers")

customers_silver = (
    customers_bronze
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("date_of_birth", clean_date("date_of_birth"))
    .withColumn("name", F.initcap(F.trim(F.col("name"))))
    .withColumn("borough", F.initcap(F.trim(F.col("borough"))))
    .withColumn("neighborhood", F.initcap(F.trim(F.col("neighborhood"))))
    .withColumn("zip_code", F.col("zip_code").cast("string"))
    .withColumn("silver_updated_at", F.current_timestamp())
)

customers_silver = drop_if_exists(customers_silver, ["_rescued_data"])

write_silver(customers_silver, "customers")


# -------------------------
# Policy
# -------------------------

policy_bronze = spark.table(f"{CATALOG}.{BRONZE}.policy")

policy_silver = (
    policy_bronze
    .withColumn("policy_no", F.trim(F.col("policy_no")))
    .withColumn("cust_id", F.col("cust_id").cast("int"))
    .withColumn("policytype", F.initcap(F.trim(F.col("policytype"))))
    .withColumn("make", F.initcap(F.trim(F.col("make"))))
    .withColumn("model", F.initcap(F.trim(F.col("model"))))
    .withColumn("sum_insured", F.abs(F.col("sum_insured").cast("double")))
    .withColumn("premium", F.abs(F.col("premium").cast("double")))
    .withColumn("deductible", F.abs(F.col("deductible").cast("double")))
    .withColumn("silver_updated_at", F.current_timestamp())
)

policy_silver = drop_if_exists(policy_silver, ["_rescued_data"])

write_silver(policy_silver, "policy")


# -------------------------
# Claims
# -------------------------

claims_bronze = spark.table(f"{CATALOG}.{BRONZE}.claims")

claims_silver = (
    claims_bronze
    .withColumn("claim_no", F.trim(F.col("claim_no")))
    .withColumn("policy_no", F.trim(F.col("policy_no")))
    .withColumn("claim_date", clean_date("claim_date"))
    .withColumn("incident_date", clean_date("incident_date"))
    .withColumn("licence_issue_date", clean_date("licence_issue_date"))
    .withColumn("injury", F.abs(F.col("injury").cast("double")))
    .withColumn("property", F.abs(F.col("property").cast("double")))
    .withColumn("vehicle", F.abs(F.col("vehicle").cast("double")))
    .withColumn("total", F.abs(F.col("total").cast("double")))
    .withColumn("incident_hour", F.col("incident_hour").cast("int"))
    .withColumn("collision_type", F.initcap(F.trim(F.col("collision_type"))))
    .withColumn("severity", F.initcap(F.trim(F.col("severity"))))
    .withColumn("silver_updated_at", F.current_timestamp())
)

claims_silver = drop_if_exists(claims_silver, ["_rescued_data"])

write_silver(claims_silver, "claims")


# -------------------------
# Telematics
# -------------------------

telematics_bronze = spark.table(f"{CATALOG}.{BRONZE}.telematics")

telematics_silver = (
    telematics_bronze
    .withColumn("silver_updated_at", F.current_timestamp())
)

telematics_silver = drop_if_exists(telematics_silver, ["_rescued_data"])

write_silver(telematics_silver, "telematics")


# -------------------------
# Accident Images
# -------------------------

accident_images_bronze = spark.table(f"{CATALOG}.{BRONZE}.accident_images")

accident_images_silver = (
    accident_images_bronze
    .withColumn(
        "image_name",
        F.regexp_extract(F.col("path"), r"([^/]+)$", 1)
    )
    .withColumn("file_size_kb", F.round(F.col("length") / 1024, 2))
    .withColumn("silver_updated_at", F.current_timestamp())
)

write_silver(accident_images_silver, "accident_images")


# -------------------------
# Training Images
# -------------------------

training_images_bronze = spark.table(f"{CATALOG}.{BRONZE}.training_images")

training_images_silver = (
    training_images_bronze
    .withColumn(
        "image_name",
        F.regexp_extract(F.col("path"), r"([^/]+)$", 1)
    )
    .withColumn(
        "damage_label",
        F.regexp_extract(F.col("path"), r"/([^/]+)/[^/]+$", 1)
    )
    .withColumn("file_size_kb", F.round(F.col("length") / 1024, 2))
    .withColumn("silver_updated_at", F.current_timestamp())
)

write_silver(training_images_silver, "training_images")


# -------------------------
# Image Metadata
# -------------------------

image_metadata_bronze = spark.table(f"{CATALOG}.{BRONZE}.image_metadata")

image_metadata_silver = (
    image_metadata_bronze
    .withColumn("silver_updated_at", F.current_timestamp())
)

image_metadata_silver = drop_if_exists(image_metadata_silver, ["_rescued_data"])

write_silver(image_metadata_silver, "image_metadata")