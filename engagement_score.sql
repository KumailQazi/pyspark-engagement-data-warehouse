-- ============================================================
-- analytics/engagement_score.sql
-- 0-100 Engagement Score: Definition, Computation, Backfill
-- ============================================================

-- --------------------------------------------------------------
-- DEFINITION
-- --------------------------------------------------------------
/*
The Engagement Score is a composite metric (0-100) designed to rank
users by their holistic engagement with the StreamCorp platform.

Formula (per user, per day):
  Score = (WatchTimePercentile * 0.50) + (FrequencyPercentile * 0.30) + (BreadthPercentile * 0.20)

Where percentiles are computed WITHIN the user's current plan segment
to avoid penalizing basic-plan users relative to premium users.

Components:
  - Watch Time:   total_watch_time_min  (weight: 50%) — Captures depth
  - Frequency:    session_count         (weight: 30%) — Captures habit
  - Breadth:      distinct_genre_count  (weight: 20%) — Captures exploration

Rolling Window:
  - Percentiles are computed over a 28-day (4-week) rolling window.
  - This stabilizes variance while remaining responsive to trends.
  - Backfill of a single day requires only that day + 27 prior days.

Storage:
  - Stored in fct_user_engagement_daily.engagement_score
  - Recomputed nightly for the processing date and the prior 27 days.
*/

-- --------------------------------------------------------------
-- DAILY SCORE COMPUTATION (Run nightly)
-- --------------------------------------------------------------

WITH daily_metrics AS (
    -- Pull last 28 days of user-day aggregates
    SELECT 
        user_id,
        activity_date,
        total_watch_time_min,
        session_count,
        distinct_genre_count
    FROM curated.fct_user_engagement_daily
    WHERE activity_date >= DATE_SUB(CURRENT_DATE(), 28)
),

user_plans AS (
    -- Get current plan for each user (for segmenting percentiles)
    SELECT user_id, current_plan
    FROM curated.dim_user
    WHERE is_current = TRUE
),

metrics_with_plan AS (
    SELECT 
        d.*,
        COALESCE(p.current_plan, 'unknown') AS plan_segment
    FROM daily_metrics d
    LEFT JOIN user_plans p ON d.user_id = p.user_id
),

-- Compute percentiles within plan segment
percentiled AS (
    SELECT 
        user_id,
        activity_date,
        total_watch_time_min,
        session_count,
        distinct_genre_count,
        plan_segment,
        PERCENT_RANK() OVER (PARTITION BY plan_segment ORDER BY total_watch_time_min) AS watch_time_pctile,
        PERCENT_RANK() OVER (PARTITION BY plan_segment ORDER BY session_count) AS freq_pctile,
        PERCENT_RANK() OVER (PARTITION BY plan_segment ORDER BY distinct_genre_count) AS breadth_pctile
    FROM metrics_with_plan
),

scored AS (
    SELECT 
        user_id,
        activity_date,
        plan_segment,
        total_watch_time_min,
        session_count,
        distinct_genre_count,
        watch_time_pctile,
        freq_pctile,
        breadth_pctile,
        CAST(ROUND(
            (watch_time_pctile * 50.0) + 
            (freq_pctile * 30.0) + 
            (breadth_pctile * 20.0)
        , 0) AS INT) AS engagement_score
    FROM percentiled
)

-- Merge back into fct_user_engagement_daily
MERGE INTO curated.fct_user_engagement_daily AS target
USING scored AS source
ON target.user_id = source.user_id AND target.activity_date = source.activity_date
WHEN MATCHED THEN UPDATE SET 
    target.engagement_score = source.engagement_score;

-- --------------------------------------------------------------
-- BACKFILL PROCEDURE
-- --------------------------------------------------------------
/*
To backfill the engagement score for a historical date range:

1. Identify the start_date and end_date to backfill.
2. For each date D in [start_date, end_date]:
   a. Run the above query with WHERE activity_date >= DATE_SUB(D, 28)
      AND activity_date <= D
   b. The MERGE will update engagement_score for all days in the window.

3. Because the percentile window is 28 days, backfilling N days
   requires O(N * 28) computations, not O(N²).

4. For a full historical backfill (e.g., 2 years):
   - Process in 28-day chunks to minimize redundant computation.
   - Each chunk's last 27 days overlap with the next chunk's first 27 days,
     ensuring continuity at chunk boundaries.
*/

-- Example: Backfill January 2024
-- Run this 31 times (once per day) or in 28-day overlapping chunks:

/*
-- Chunk 1: Jan 1-28
WITH daily_metrics AS (
    SELECT user_id, activity_date, total_watch_time_min, session_count, distinct_genre_count
    FROM curated.fct_user_engagement_daily
    WHERE activity_date BETWEEN '2023-12-05' AND '2024-01-28'  -- 28-day window ending Jan 28
), ... (same percentile logic) ...

-- Chunk 2: Jan 15-31  
WITH daily_metrics AS (
    SELECT user_id, activity_date, total_watch_time_min, session_count, distinct_genre_count
    FROM curated.fct_user_engagement_daily
    WHERE activity_date BETWEEN '2023-12-19' AND '2024-01-31'
), ... (same percentile logic) ...
*/

-- --------------------------------------------------------------
-- VALIDATION QUERY
-- --------------------------------------------------------------
SELECT 
    plan_segment,
    COUNT(DISTINCT user_id) AS users,
    MIN(engagement_score) AS min_score,
    MAX(engagement_score) AS max_score,
    ROUND(AVG(engagement_score), 1) AS avg_score,
    ROUND(STDDEV(engagement_score), 2) AS stddev_score
FROM (
    SELECT 
        user_id,
        activity_date,
        engagement_score,
        COALESCE(
            (SELECT current_plan FROM curated.dim_user u WHERE u.user_id = d.user_id AND is_current = TRUE),
            'unknown'
        ) AS plan_segment
    FROM curated.fct_user_engagement_daily d
    WHERE activity_date >= DATE_SUB(CURRENT_DATE(), 7)
)
GROUP BY plan_segment
ORDER BY avg_score DESC;
