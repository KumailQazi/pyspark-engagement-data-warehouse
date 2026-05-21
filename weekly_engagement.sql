-- ============================================================
-- analytics/weekly_engagement.sql
-- BI-facing query against mart_user_engagement_weekly
-- Answers: "How engaged are users this week vs last week, 
--           and which segments are changing fastest?"
-- ============================================================

-- --------------------------------------------------------------
-- REPORT 1: Executive Summary by Segment
-- --------------------------------------------------------------
WITH current_week AS (
    SELECT 
        week_start_date,
        segment,
        user_plan_at_week_start AS plan,
        COUNT(DISTINCT user_id) AS total_users,
        ROUND(AVG(total_watch_time_min), 2) AS avg_watch_time_min,
        ROUND(AVG(session_count), 1) AS avg_sessions,
        ROUND(AVG(avg_engagement_score), 1) AS avg_engagement_score,
        ROUND(AVG(COALESCE(week_over_week_change_pct, 0)), 2) AS avg_wow_change_pct,
        COUNT(DISTINCT CASE WHEN engagement_trend = 'Surging' THEN user_id END) AS surging_users,
        COUNT(DISTINCT CASE WHEN engagement_trend = 'At Risk' THEN user_id END) AS at_risk_users
    FROM marts.mart_user_engagement_weekly
    WHERE week_start_date = (SELECT MAX(week_start_date) FROM marts.mart_user_engagement_weekly)
    GROUP BY week_start_date, segment, user_plan_at_week_start
),
prev_week AS (
    SELECT 
        segment,
        user_plan_at_week_start AS plan,
        ROUND(AVG(avg_engagement_score), 1) AS prev_avg_engagement_score
    FROM marts.mart_user_engagement_weekly
    WHERE week_start_date = (
        SELECT MAX(week_start_date) 
        FROM marts.mart_user_engagement_weekly 
        WHERE week_start_date < (SELECT MAX(week_start_date) FROM marts.mart_user_engagement_weekly)
    )
    GROUP BY segment, user_plan_at_week_start
)
SELECT 
    c.week_start_date,
    c.segment,
    c.plan,
    c.total_users,
    c.avg_watch_time_min,
    c.avg_sessions,
    c.avg_engagement_score,
    c.avg_wow_change_pct,
    c.surging_users,
    c.at_risk_users,
    p.prev_avg_engagement_score,
    ROUND(c.avg_engagement_score - p.prev_avg_engagement_score, 1) AS engagement_score_delta
FROM current_week c
LEFT JOIN prev_week p 
    ON c.segment = p.segment 
    AND c.plan = p.plan
ORDER BY 
    c.avg_wow_change_pct DESC,
    c.segment,
    c.plan;

-- --------------------------------------------------------------
-- REPORT 2: Segment Velocity (Fastest Changing Segments)
-- --------------------------------------------------------------
SELECT 
    segment,
    user_plan_at_week_start AS plan,
    COUNT(DISTINCT user_id) AS user_count,
    ROUND(AVG(total_watch_time_min), 2) AS avg_watch_time,
    ROUND(AVG(week_over_week_change_pct), 2) AS avg_wow_change,
    ROUND(
        COUNT(DISTINCT CASE WHEN engagement_trend IN ('Surging', 'Growing') THEN user_id END) * 100.0 
        / COUNT(DISTINCT user_id), 1
    ) AS pct_growing,
    ROUND(
        COUNT(DISTINCT CASE WHEN engagement_trend IN ('Declining', 'At Risk') THEN user_id END) * 100.0 
        / COUNT(DISTINCT user_id), 1
    ) AS pct_declining
FROM marts.mart_user_engagement_weekly
WHERE week_start_date = (SELECT MAX(week_start_date) FROM marts.mart_user_engagement_weekly)
GROUP BY segment, user_plan_at_week_start
ORDER BY ABS(avg_wow_change) DESC;

-- --------------------------------------------------------------
-- REPORT 3: Top 100 At-Risk Users (Actionable for Marketing)
-- --------------------------------------------------------------
SELECT 
    user_id,
    user_plan_at_week_start AS plan,
    segment,
    total_watch_time_min,
    session_count,
    days_active,
    avg_engagement_score,
    week_over_week_change_pct,
    engagement_trend
FROM marts.mart_user_engagement_weekly
WHERE week_start_date = (SELECT MAX(week_start_date) FROM marts.mart_user_engagement_weekly)
  AND engagement_trend = 'At Risk'
ORDER BY week_over_week_change_pct ASC
LIMIT 100;
