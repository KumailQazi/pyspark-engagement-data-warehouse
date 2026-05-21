"""
# ============================================================
# dim_user_scd2.py
# CDC Snapshot → SCD Type 2 Dimension
# Handles: plan changes, status changes, historical attribution
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, md5, concat_ws, lit, when, row_number, max as spark_max,
    current_timestamp, coalesce, lag, lead
)
from delta.tables import DeltaTable

spark = SparkSession.builder     .appName("StreamCorp-DimUserSCD2")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# --------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------
CDC_PATH = "abfss://raw@streamcorplake.dfs.core.windows.net/users/"
DIM_PATH = "abfss://curated@streamcorplake.dfs.core.windows.net/dim_user/"
PROCESS_DATE = "2024-01-15"

# --------------------------------------------------------------
# 1. READ CDC SNAPSHOT
# --------------------------------------------------------------
df_cdc = spark.read.parquet(CDC_PATH).filter(col("last_updated_ts") >= lit(f"{PROCESS_DATE}T00:00:00Z"))

# Compute hash of all type-2 tracked attributes
df_cdc = df_cdc.withColumn(
    "row_hash",
    md5(concat_ws("||",
        col("signup_source"), col("country_code"), col("current_plan"),
        col("plan_started_ts"), col("status")
    ))
)

# --------------------------------------------------------------
# 2. MERGE LOGIC
# --------------------------------------------------------------
if DeltaTable.isDeltaTable(spark, DIM_PATH):
    dim_table = DeltaTable.forPath(spark, DIM_PATH)

    # Read current dimension to find existing users
    df_current = dim_table.toDF().filter(col("is_current") == True)

    # Join CDC to current dimension on user_id
    df_joined = df_cdc.alias("cdc").join(
        df_current.alias("dim"),
        on="user_id",
        how="left"
    )

    # Identify changed rows: new user OR hash mismatch
    df_changes = df_joined.filter(
        (col("dim.user_id").isNull()) |  # New user
        (col("cdc.row_hash") != col("dim.row_hash"))  # Attribute changed
    ).select("cdc.*")

    if df_changes.count() > 0:
        # Close old records: set valid_to = now, is_current = False
        dim_table.alias("target").merge(
            df_changes.alias("source"),
            "target.user_id = source.user_id AND target.is_current = True"
        ).whenMatchedUpdate(set={
            "valid_to": current_timestamp(),
            "is_current": lit(False)
        }).execute()

        # Insert new records with valid_from = now, valid_to = 9999-12-31, is_current = True
        df_new_records = df_changes.withColumn("valid_from", current_timestamp())             .withColumn("valid_to", lit("9999-12-31").cast("timestamp"))             .withColumn("is_current", lit(True))

        df_new_records.write.format("delta").mode("append").save(DIM_PATH)

        print(f"SCD2 merge complete: {df_changes.count()} users changed/added")
    else:
        print("No changes detected in CDC snapshot")

else:
    # Bootstrap dimension on first run
    df_bootstrap = df_cdc.withColumn("valid_from", current_timestamp())         .withColumn("valid_to", lit("9999-12-31").cast("timestamp"))         .withColumn("is_current", lit(True))

    df_bootstrap.write.format("delta").mode("overwrite").save(DIM_PATH)
    print(f"Dimension bootstrapped with {df_bootstrap.count()} users")
