"""
# ============================================================
# curated_to_marts.py
# Curated Facts → User-Day Aggregates → Weekly Mart
# Handles: engagement score (0-100), WoW change, segmentation
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sum as spark_sum, count as spark_count, countDistinct,
    avg as spark_avg, max as spark_max, min as spark_min,
    when, lit, row_number, rank, percent_rank, round as spark_round,
    datediff, date_sub, date_add, trunc, current_date, coalesce,
    first, last, expr
)
from delta.tables import DeltaTable

spark = SparkSession.builder     .appName("StreamCorp-CuratedToMarts")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# --------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------
PROCESS_DATE = "2024-01-15"
CURATED_BASE = "abfss://curated@streamcorplake.dfs.core.windows.net"
MARTS_BASE = "abfss://marts@streamcorplake.dfs.core.windows.net"

# --------------------------------------------------------------
# 1. USER-DAY ENGREGATE
# --------------------------------------------------------------
def build_user_daily(process_date: str):
    """
    Aggregate session facts to user-day grain.
    Computes watch time, frequency, breadth, and primary platform.
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    dates_to_process = [
        (process_dt - timedelta(days=d)).strftime("%Y-%m-%d")
        for d in range(3)  # Process last 3 days for stability
    ]

    for activity_date in dates_to_process:
        df_sessions = spark.read.format("delta")             .load(f"{CURATED_BASE}/fct_sessions")             .filter(col("session_date") == activity_date)

        if df_sessions.count() == 0:
            continue

        # Join to content for genre/type breadth
        df_playback = spark.read.format("delta")             .load(f"{CURATED_BASE}/fct_playback_events")             .filter(col("event_date") == activity_date)             .select("user_id", "content_id", "platform")

        df_content = spark.read.format("delta").load(f"{CURATED_BASE}/dim_content")             .select("content_id", "genre", "content_type")

        df_playback_enriched = df_playback.join(df_content, on="content_id", how="left")

        # Primary platform: mode of platforms in day
        platform_counts = df_playback_enriched.groupBy("user_id", "platform").count()
        win_platform = Window.partitionBy("user_id").orderBy(col("count").desc())
        df_primary_platform = platform_counts.withColumn("rn", row_number().over(win_platform))             .filter(col("rn") == 1).select("user_id", col("platform").alias("primary_platform"))

        # User-day aggregates
        df_user_day = df_sessions.groupBy("user_id").agg(
            spark_sum("total_watch_time_sec").alias("total_watch_time_sec"),
            spark_count("session_id").alias("session_count"),
            countDistinct("session_id").alias("distinct_session_count")
        ).withColumn("activity_date", lit(activity_date).cast("date"))          .withColumn("total_watch_time_min", spark_round(col("total_watch_time_sec") / 60, 2))          .withColumn("avg_session_duration_min", spark_round(col("total_watch_time_sec") / col("distinct_session_count") / 60, 2))

        # Breadth metrics from playback
        df_breadth = df_playback_enriched.groupBy("user_id").agg(
            countDistinct("content_id").alias("distinct_content_count"),
            countDistinct("genre").alias("distinct_genre_count"),
            countDistinct("content_type").alias("distinct_type_count")
        )

        df_final = df_user_day.join(df_breadth, on="user_id", how="left")             .join(df_primary_platform, on="user_id", how="left")             .withColumn("date_key", expr("CAST(date_format(activity_date, 'yyyyMMdd') AS INT)"))

        # Write to fct_user_engagement_daily
        daily_path = f"{CURATED_BASE}/fct_user_engagement_daily"

        if DeltaTable.isDeltaTable(spark, daily_path):
            delta_table = DeltaTable.forPath(spark, daily_path)
            delta_table.alias("target").merge(
                df_final.alias("source"),
                "target.user_id = source.user_id AND target.activity_date = source.activity_date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df_final.write.format("delta").partitionBy("activity_date").mode("overwrite").save(daily_path)

        print(f"Built user-day aggregate for {activity_date}: {df_final.count()} users")

# --------------------------------------------------------------
# 2. ENGAGEMENT SCORE (0-100)
# --------------------------------------------------------------
def compute_engagement_score(process_date: str):
    """
    Compute percentile-based engagement score using rolling 4-week window.
    Score = (WatchTimePctile * 0.50) + (FrequencyPctile * 0.30) + (BreadthPctile * 0.20)
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    window_start = (process_dt - timedelta(days=27)).strftime("%Y-%m-%d")

    df_daily = spark.read.format("delta")         .load(f"{CURATED_BASE}/fct_user_engagement_daily")         .filter(col("activity_date") >= window_start)         .filter(col("activity_date") <= process_date)

    # Join to dim_user for plan segment (current plan at time of activity)
    df_user = spark.read.format("delta").load(f"{CURATED_BASE}/dim_user").filter(col("is_current") == True)         .select("user_id", "current_plan")

    df_with_plan = df_daily.join(df_user, on="user_id", how="left")

    # Compute percentiles within plan segment
    win_plan = Window.partitionBy("current_plan").orderBy("total_watch_time_min")
    win_freq = Window.partitionBy("current_plan").orderBy("session_count")
    win_breadth = Window.partitionBy("current_plan").orderBy("distinct_genre_count")

    df_scored = df_with_plan         .withColumn("watch_time_pctile", percent_rank().over(win_plan))         .withColumn("freq_pctile", percent_rank().over(win_freq))         .withColumn("breadth_pctile", percent_rank().over(win_breadth))         .withColumn("engagement_score",
            spark_round(
                (col("watch_time_pctile") * 50) +
                (col("freq_pctile") * 30) +
                (col("breadth_pctile") * 20)
            , 0).cast("int")
        ).select("user_id", "activity_date", "engagement_score")

    # Merge score back into daily fact
    daily_path = f"{CURATED_BASE}/fct_user_engagement_daily"
    delta_table = DeltaTable.forPath(spark, daily_path)

    delta_table.alias("target").merge(
        df_scored.alias("source"),
        "target.user_id = source.user_id AND target.activity_date = source.activity_date"
    ).whenMatchedUpdate(set={
        "engagement_score": col("source.engagement_score")
    }).execute()

    print(f"Computed engagement scores for {process_date}")

# --------------------------------------------------------------
# 3. WEEKLY MART
# --------------------------------------------------------------
def build_weekly_mart(process_date: str):
    """
    Build mart_user_engagement_weekly from user-day aggregates.
    Computes WoW change and engagement trend.
    """
    from datetime import datetime, timedelta

    process_dt = datetime.strptime(process_date, "%Y-%m-%d")

    # Determine week boundaries (week starts Monday)
    week_start = process_dt - timedelta(days=process_dt.weekday())
    week_end = week_start + timedelta(days=6)
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = prev_week_start + timedelta(days=6)

    # Read current week daily data
    df_current = spark.read.format("delta")         .load(f"{CURATED_BASE}/fct_user_engagement_daily")         .filter(col("activity_date") >= week_start.strftime("%Y-%m-%d"))         .filter(col("activity_date") <= week_end.strftime("%Y-%m-%d"))

    # Read previous week for WoW comparison
    df_prev = spark.read.format("delta")         .load(f"{CURATED_BASE}/fct_user_engagement_daily")         .filter(col("activity_date") >= prev_week_start.strftime("%Y-%m-%d"))         .filter(col("activity_date") <= prev_week_end.strftime("%Y-%m-%d"))         .groupBy("user_id").agg(
            spark_sum("total_watch_time_min").alias("prev_week_watch_time")
        )

    # Aggregate current week
    df_weekly = df_current.groupBy("user_id").agg(
        spark_sum("total_watch_time_min").alias("total_watch_time_min"),
        spark_sum("session_count").alias("session_count"),
        spark_max("distinct_content_count").alias("distinct_content_count"),
        spark_max("distinct_genre_count").alias("distinct_genre_count"),
        countDistinct("activity_date").alias("days_active"),
        spark_avg("engagement_score").alias("avg_engagement_score")
    ).withColumn("week_start_date", lit(week_start.strftime("%Y-%m-%d")).cast("date"))      .withColumn("week_end_date", lit(week_end.strftime("%Y-%m-%d")).cast("date"))

    # Join to dim_user for plan/status at week start
    df_user = spark.read.format("delta").load(f"{CURATED_BASE}/dim_user")         .filter(col("is_current") == True)         .select("user_id", "current_plan", "status")

    df_with_user = df_weekly.join(df_user, on="user_id", how="left")         .withColumnRenamed("current_plan", "user_plan_at_week_start")         .withColumnRenamed("status", "user_status_at_week_start")

    # WoW change
    df_wow = df_with_user.join(df_prev, on="user_id", how="left")         .withColumn("week_over_week_change_pct",
            when(col("prev_week_watch_time").isNull() | (col("prev_week_watch_time") == 0), lit(None))
            .otherwise(spark_round((col("total_watch_time_min") - col("prev_week_watch_time")) / col("prev_week_watch_time") * 100, 2))
        )

    # Engagement trend classification
    df_final = df_wow.withColumn("engagement_trend",
        when(col("week_over_week_change_pct") > 20, lit("Surging"))
        .when(col("week_over_week_change_pct") > 5, lit("Growing"))
        .when(col("week_over_week_change_pct") < -20, lit("At Risk"))
        .when(col("week_over_week_change_pct") < -5, lit("Declining"))
        .otherwise(lit("Stable"))
    ).withColumn("segment",
        when(col("days_active") >= 5, lit("Power User"))
        .when(col("days_active") >= 3, lit("Regular"))
        .when(col("days_active") >= 1, lit("Casual"))
        .otherwise(lit("Dormant"))
    )

    # Write mart
    mart_path = f"{MARTS_BASE}/mart_user_engagement_weekly"

    if DeltaTable.isDeltaTable(spark, mart_path):
        delta_table = DeltaTable.forPath(spark, mart_path)
        delta_table.alias("target").merge(
            df_final.alias("source"),
            "target.user_id = source.user_id AND target.week_start_date = source.week_start_date"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df_final.write.format("delta").partitionBy("week_start_date").mode("overwrite").save(mart_path)

    print(f"Built weekly mart for week starting {week_start.strftime('%Y-%m-%d')}: {df_final.count()} users")

# --------------------------------------------------------------
# ENTRY POINT
# --------------------------------------------------------------
if __name__ == "__main__":
    build_user_daily(PROCESS_DATE)
    compute_engagement_score(PROCESS_DATE)
    build_weekly_mart(PROCESS_DATE)
