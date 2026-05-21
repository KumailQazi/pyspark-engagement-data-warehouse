"""
data_quality.py
Great Expectations-style assertions (simplified for case study scope).
In production, these would be GE suites against the Delta tables.
"""

from pyspark.sql.functions import col, count, when, isnan

def validate_playback_events(df):
    """
    Run quality checks on fct_playback_events before downstream use.
    Returns (is_valid, list_of_failures).
    """
    checks = []
    
    # Check 1: No null event_id
    null_events = df.filter(col("event_id").isNull()).count()
    checks.append(("null_event_id", null_events, 0))
    
    # Check 2: No negative watch_time_sec
    negative_watch = df.filter(col("watch_time_sec") < 0).count()
    checks.append(("negative_watch_time", negative_watch, 0))
    
    # Check 3: watch_time_sec capped at 300s (our business rule)
    excessive_watch = df.filter(col("watch_time_sec") > 300).count()
    checks.append(("watch_time_over_300s", excessive_watch, 0.001 * df.count()))  # 0.1% tolerance
    
    # Check 4: event_date matches server_ts date (within 2 days)
    from pyspark.sql.functions import datediff, to_date
    date_mismatch = df.filter(
        datediff(to_date(col("server_ts")), col("event_date")) > 2
    ).count()
    checks.append(("event_date_lag_gt_2d", date_mismatch, 0.01 * df.count()))  # 1% tolerance
    
    failures = [name for name, actual, threshold in checks if actual > threshold]
    is_valid = len(failures) == 0
    
    return is_valid, checks

def validate_user_engagement_daily(df):
    """Quality checks for fct_user_engagement_daily."""
    checks = []
    
    # engagement_score must be 0-100
    out_of_bounds = df.filter(
        (col("engagement_score") < 0) | (col("engagement_score") > 100)
    ).count()
    checks.append(("engagement_score_bounds", out_of_bounds, 0))
    
    # total_watch_time_min must be non-negative
    negative_time = df.filter(col("total_watch_time_min") < 0).count()
    checks.append(("negative_watch_time", negative_time, 0))
    
    failures = [name for name, actual, threshold in checks if actual > threshold]
    return len(failures) == 0, checks
