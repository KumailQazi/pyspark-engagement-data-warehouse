"""
# ============================================================
# bronze_to_silver.py
# Raw JSON → Staging Delta Tables
# Handles: deduplication, typing, late-arrival flagging
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_date, row_number, max as spark_max, lit, when,
    current_timestamp, datediff, abs as spark_abs
)
from delta.tables import DeltaTable

spark = SparkSession.builder     .appName("Tapmad-BronzeToSilver")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# --------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------
PROCESS_DATE = "2024-01-15"  # Parameterized in production via Airflow
LOOKBACK_DAYS = 2            # 48h late arrival window
RAW_BASE = "abfss://raw@tapmadlake.dfs.core.windows.net"
STAGING_BASE = "abfss://staging@tapmadlake.dfs.core.windows.net"

# --------------------------------------------------------------
# 1. PLAYBACK EVENTS — Deduplicate & Type
# --------------------------------------------------------------
def process_playback_events(process_date: str):
    """
    Idempotent merge of playback events into staging.
    Deduplicates on event_id (keep latest server_ts).
    Flags late arrivals (> 24h between event_ts and server_ts).
    Processes process_date + LOOKBACK_DAYS for late arrivals.
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    dates_to_process = [
        (process_dt - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(LOOKBACK_DAYS + 1)
    ]

    for event_date in dates_to_process:
        raw_path = f"{RAW_BASE}/playback_events/event_date={event_date}"

        # Read raw JSON
        df_raw = spark.read.json(raw_path)

        # Basic typing and derived columns
        df_typed = df_raw             .withColumn("event_date", to_date(col("event_ts")))             .withColumn("is_late_arrival", 
                when(spark_abs(datediff(to_date(col("server_ts")), to_date(col("event_ts")))) > 1, lit(True)
                ).otherwise(lit(False))             .withColumn("_ingest_ts", current_timestamp())

        # Deduplicate: keep latest server_ts per event_id
        win = Window.partitionBy("event_id").orderBy(col("server_ts").desc())
        df_dedup = df_typed.withColumn("rn", row_number().over(win)).filter(col("rn") == 1).drop("rn")

        # Idempotent MERGE into Delta
        staging_table = f"{STAGING_BASE}/stg_playback_events"

        if DeltaTable.isDeltaTable(spark, staging_table):
            delta_table = DeltaTable.forPath(spark, staging_table)

            delta_table.alias("target").merge(
                df_dedup.alias("source"),
                "target.event_id = source.event_id"
            ).whenMatchedUpdateAll(
                condition="source.server_ts > target.server_ts"
            ).whenNotMatchedInsertAll().execute()
        else:
            # First run — create table
            df_dedup.write                 .format("delta")                 .partitionBy("event_date")                 .mode("overwrite")                 .save(staging_table)

        print(f"Processed playback events for {event_date}: {df_dedup.count()} rows")

# --------------------------------------------------------------
# 2. APP SESSIONS — Pass-through with typing
# --------------------------------------------------------------
def process_app_sessions(process_date: str):
    """
    App sessions are already deduplicated upstream.
    We simply type and write to staging.
    """
    raw_path = f"{RAW_BASE}/app_sessions/session_date={process_date}"

    df_raw = spark.read.parquet(raw_path)

    df_typed = df_raw         .withColumn("session_date", to_date(col("session_start_ts")))         .withColumn("_ingest_ts", current_timestamp())

    staging_table = f"{STAGING_BASE}/stg_sessions"

    if DeltaTable.isDeltaTable(spark, staging_table):
        delta_table = DeltaTable.forPath(spark, staging_table)

        delta_table.alias("target").merge(
            df_typed.alias("source"),
            "target.session_id = source.session_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_typed.write             .format("delta")             .partitionBy("session_date")             .mode("overwrite")             .save(staging_table)

    print(f"Processed app sessions for {process_date}: {df_typed.count()} rows")

# --------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------
if __name__ == "__main__":
    process_playback_events(PROCESS_DATE)
    process_app_sessions(PROCESS_DATE)
