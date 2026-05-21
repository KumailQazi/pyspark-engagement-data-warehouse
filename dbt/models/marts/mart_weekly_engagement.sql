{{ config(
    materialized='incremental',
    unique_key=['week_start_date', 'current_plan', 'platform'],
    partition_by={'field': 'week_start_date', 'data_type': 'date'},
    tags=['weekly_reporting']
) }}

WITH daily_engagement AS (
    SELECT * FROM {{ ref('fct_user_day_engagement') }}
    -- Incremental logic: only process new data
    {% if is_incremental() %}
    WHERE date_key >= (SELECT MAX(date_key) FROM {{ this }})
    {% endif %}
),

users AS (
    SELECT * FROM {{ ref('dim_user_scd2') }}
),

weekly_user_metrics AS (
    SELECT 
        date_trunc('week', f.date_key) as week_start_date,
        u.current_plan,
        f.platform,
        COUNT(DISTINCT f.user_key) as active_users,
        SUM(f.total_watch_time_sec) / 60.0 as total_watch_time_minutes,
        AVG(f.engagement_score) as avg_engagement_score
    FROM daily_engagement f
    JOIN users u ON f.user_key = u.user_key
    GROUP BY 1, 2, 3
)

SELECT 
    week_start_date,
    current_plan,
    platform,
    active_users,
    total_watch_time_minutes,
    avg_engagement_score,
    -- Window function for Week-over-Week comparison without self-joins
    LAG(total_watch_time_minutes, 1) OVER (
        PARTITION BY current_plan, platform 
        ORDER BY week_start_date
    ) as prev_week_watch_time,
    
    ROUND(((total_watch_time_minutes - LAG(total_watch_time_minutes, 1) OVER (
        PARTITION BY current_plan, platform 
        ORDER BY week_start_date
    )) / NULLIF(LAG(total_watch_time_minutes, 1) OVER (
        PARTITION BY current_plan, platform 
        ORDER BY week_start_date
    ), 0)) * 100, 2) as wow_watch_time_growth_pct

FROM weekly_user_metrics
