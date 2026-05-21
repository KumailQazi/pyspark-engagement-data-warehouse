"""
# ============================================================
# silver_to_curated.py
# Staging → Curated Facts (playback events + sessions)
# Handles: watch-time calculation, session reconstruction,
#          out-of-order event handling
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lead, lag, when, coalesce, sum as spark_sum, count as spark_count,
    countDistinct, max as spark_max, min as spark_min, datediff,
    unix_timestamp, from_unixtime, to_date, lit, row_number, abs as spark_abs
)
from delta.tables import DeltaTable

spark = SparkSession.builder     .appName("Tapmad-SilverToCurated")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# --------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------
PROCESS_DATE = "2024-01-15"
LOOKBACK_DAYS = 2
STAGING_BASE = "abfss://staging@tapmadlake.dfs.core.windows.net"
CURATED_BASE = "abfss://curated@tapmadlake.dfs.core.windows.net"

# --------------------------------------------------------------
# 1. PLAYBACK EVENTS — Watch Time Calculation
# --------------------------------------------------------------
def build_playback_facts(process_date: str):
    """
    Build fct_playback_events with watch_time_sec derived from
    heartbeat intervals. Handles out-of-order events via server_ts ordering.
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    dates_to_process = [
        (process_dt - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(LOOKBACK_DAYS + 1)
    ]

    for event_date in dates_to_process:
        # Read staging for this date
        df_stg = spark.read.format("delta")             .load(f"{STAGING_BASE}/stg_playback_events")             .filter(col("event_date") == event_date)

        if df_stg.count() == 0:
            continue

        # Window: within session+content, order by server_ts (authoritative)
        win = Window.partitionBy("session_id", "content_id").orderBy("server_ts")

        # Calculate time to next event in same session/content
        df_with_watch = df_stg             .withColumn("next_event_ts", lead("server_ts").over(win))             .withColumn("next_event_type", lead("event_type").over(win))             .withColumn("watch_time_sec",
                when(col("next_event_ts").isNull(), lit(30))  # Last event: assume 30s heartbeat
                .when(col("event_type").isin(["pause", "play_end", "error"]), lit(0))  # Terminal events
                .otherwise(
                    # Cap at 5 minutes to handle client going background
                    when(
                        (unix_timestamp(col("next_event_ts")) - unix_timestamp(col("server_ts"))) > 300,
                        lit(300)
                    ).otherwise(
                        unix_timestamp(col("next_event_ts")) - unix_timestamp(col("server_ts"))
                    )
                )
            )

        # Join to content dimension to cap VOD at duration
        df_content = spark.read.format("delta").load(f"{CURATED_BASE}/dim_content")

        df_with_duration = df_with_watch.join(
            df_content.select("content_id", "duration_sec", "content_type"),
            on="content_id",
            how="left"
        )

        # Cap watch time at content duration for VOD
        df_final = df_with_duration.withColumn(
            "watch_time_sec",
            when(
                (col("content_type").isin(["vod_movie", "vod_series_ep", "highlight"])) &
                (col("duration_sec").isNotNull()),
                when(col("watch_time_sec") > col("duration_sec"), col("duration_sec")).otherwise(col("watch_time_sec"))
            ).otherwise(col("watch_time_sec"))
        ).select(
            "event_id", "user_id", "device_id", "session_id", "content_id",
            "event_type", "event_ts", "server_ts", "position_sec",
            "platform", "country_code", "event_date", "is_late_arrival",
            "watch_time_sec"
        )

        # Idempotent MERGE into curated fact
        fact_path = f"{CURATED_BASE}/fct_playback_events"

        if DeltaTable.isDeltaTable(spark, fact_path):
            delta_table = DeltaTable.forPath(spark, fact_path)
            delta_table.alias("target").merge(
                df_final.alias("source"),
                "target.event_id = source.event_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_final.write.format("delta").partitionBy("event_date").mode("overwrite").save(fact_path)

        print(f"Built playback facts for {event_date}: {df_final.count()} events")

# --------------------------------------------------------------
# 2. SESSIONS — Aggregate from playback events
# --------------------------------------------------------------
def build_session_facts(process_date: str):
    """
    Build fct_sessions by aggregating playback events.
    Uses app_sessions as boundary if available; otherwise infers
    from 30-minute inactivity gaps.
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    dates_to_process = [
        (process_dt - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(LOOKBACK_DAYS + 1)
    ]

    for session_date in dates_to_process:
        # Read playback events for this date
        df_playback = spark.read.format("delta")             .load(f"{CURATED_BASE}/fct_playback_events")             .filter(col("event_date") == session_date)

        if df_playback.count() == 0:
            continue

        # Aggregate to session grain
        df_sessions = df_playback.groupBy("session_id", "user_id", "device_id", "platform").agg(
            spark_min("server_ts").alias("session_start_ts"),
            spark_max("server_ts").alias("session_end_ts"),
            spark_sum("watch_time_sec").alias("total_watch_time_sec"),
            countDistinct("content_id").alias("distinct_content_count"),
            spark_count("event_id").alias("event_count")
        ).withColumn("session_date", lit(session_date).cast("date"))

        # Merge into fct_sessions
        fact_path = f"{CURATED_BASE}/fct_sessions"

        if DeltaTable.isDeltaTable(spark, fact_path):
            delta_table = DeltaTable.forPath(spark, fact_path)
            delta_table.alias("target").merge(
                df_sessions.alias("source"),
                "target.session_id = source.session_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_sessions.write.format("delta").partitionBy("session_date").mode("overwrite").save(fact_path)

        print(f"Built session facts for {session_date}: {df_sessions.count()} sessions")

# --------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------
if __name__ == "__main__":
    build_playback_facts(PROCESS_DATE)
    build_session_facts(PROCESS_DATE)
