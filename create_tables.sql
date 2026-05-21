-- ============================================================
-- StreamCorp Engagement Platform — Delta Lake DDL
-- Compatible with Spark 3.4+ / Databricks / Azure Fabric
-- ============================================================

-- --------------------------------------------------------------
-- 1. RAW / BRONZE (External tables over ADLS JSON)
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.playback_events (
    event_id        STRING,
    user_id         STRING,
    device_id       STRING,
    session_id      STRING,
    content_id      STRING,
    event_type      STRING,
    event_ts        TIMESTAMP,
    server_ts       TIMESTAMP,
    position_sec    INT,
    platform        STRING,
    country_code    STRING
)
USING JSON
PARTITIONED BY (event_date DATE)
LOCATION 'abfss://raw@streamcorplake.dfs.core.windows.net/playback_events/';

CREATE TABLE IF NOT EXISTS raw.users (
    user_id         STRING,
    signup_ts       TIMESTAMP,
    signup_source   STRING,
    country_code    STRING,
    current_plan    STRING,
    plan_started_ts TIMESTAMP,
    status          STRING,
    last_updated_ts TIMESTAMP
)
USING PARQUET
LOCATION 'abfss://raw@streamcorplake.dfs.core.windows.net/users/';

CREATE TABLE IF NOT EXISTS raw.content (
    content_id      STRING,
    title           STRING,
    content_type    STRING,
    genre           STRING,
    language        STRING,
    duration_sec    INT,
    is_premium      BOOLEAN,
    publish_ts      TIMESTAMP
)
USING PARQUET
LOCATION 'abfss://raw@streamcorplake.dfs.core.windows.net/content/';

CREATE TABLE IF NOT EXISTS raw.app_sessions (
    session_id      STRING,
    user_id         STRING,
    device_id       STRING,
    session_start_ts TIMESTAMP,
    session_end_ts   TIMESTAMP,
    platform        STRING
)
USING PARQUET
PARTITIONED BY (session_date DATE)
LOCATION 'abfss://raw@streamcorplake.dfs.core.windows.net/app_sessions/';


-- --------------------------------------------------------------
-- 2. STAGING / SILVER
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.stg_playback_events (
    event_id        STRING,
    user_id         STRING,
    device_id       STRING,
    session_id      STRING,
    content_id      STRING,
    event_type      STRING,
    event_ts        TIMESTAMP,
    server_ts       TIMESTAMP,
    position_sec    INT,
    platform        STRING,
    country_code    STRING,
    event_date      DATE,
    is_late_arrival BOOLEAN,
    _ingest_ts      TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 'abfss://staging@streamcorplake.dfs.core.windows.net/stg_playback_events/';

CREATE TABLE IF NOT EXISTS staging.stg_sessions (
    session_id      STRING,
    user_id         STRING,
    device_id       STRING,
    session_start_ts TIMESTAMP,
    session_end_ts   TIMESTAMP,
    platform        STRING,
    session_date    DATE,
    _ingest_ts      TIMESTAMP
)
USING DELTA
PARTITIONED BY (session_date)
LOCATION 'abfss://staging@streamcorplake.dfs.core.windows.net/stg_sessions/';


-- --------------------------------------------------------------
-- 3. DIMENSIONS (Curated)
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS curated.dim_user (
    user_surr_key   BIGINT GENERATED ALWAYS AS IDENTITY,
    user_id         STRING,
    signup_ts       TIMESTAMP,
    signup_source   STRING,
    country_code    STRING,
    current_plan    STRING,
    plan_started_ts TIMESTAMP,
    status          STRING,
    valid_from      TIMESTAMP,
    valid_to        TIMESTAMP,
    is_current      BOOLEAN
)
USING DELTA
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/dim_user/';

CREATE TABLE IF NOT EXISTS curated.dim_content (
    content_id      STRING,
    title           STRING,
    content_type    STRING,
    genre           STRING,
    language        STRING,
    duration_sec    INT,
    is_premium      BOOLEAN,
    publish_ts      TIMESTAMP,
    _loaded_at      TIMESTAMP
)
USING DELTA
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/dim_content/';

CREATE TABLE IF NOT EXISTS curated.dim_date (
    date_key        INT,
    full_date       DATE,
    day_of_week     INT,
    week_of_year    INT,
    month_num       INT,
    year_num        INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
)
USING DELTA
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/dim_date/';


-- --------------------------------------------------------------
-- 4. FACTS (Curated)
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS curated.fct_playback_events (
    event_id        STRING,
    user_id         STRING,
    device_id       STRING,
    session_id      STRING,
    content_id      STRING,
    event_type      STRING,
    event_ts        TIMESTAMP,
    server_ts       TIMESTAMP,
    position_sec    INT,
    platform        STRING,
    country_code    STRING,
    event_date      DATE,
    is_late_arrival BOOLEAN,
    watch_time_sec  INT
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/fct_playback_events/';

CREATE TABLE IF NOT EXISTS curated.fct_sessions (
    session_id      STRING,
    user_id         STRING,
    device_id       STRING,
    session_start_ts TIMESTAMP,
    session_end_ts   TIMESTAMP,
    platform        STRING,
    total_watch_time_sec INT,
    distinct_content_count INT,
    event_count     INT,
    session_date    DATE
)
USING DELTA
PARTITIONED BY (session_date)
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/fct_sessions/';

CREATE TABLE IF NOT EXISTS curated.fct_user_engagement_daily (
    user_id                 STRING,
    date_key                INT,
    activity_date           DATE,
    total_watch_time_min    DECIMAL(10,2),
    session_count           INT,
    distinct_content_count  INT,
    distinct_genre_count    INT,
    distinct_type_count     INT,
    avg_session_duration_min DECIMAL(10,2),
    engagement_score        INT,
    primary_platform        STRING
)
USING DELTA
PARTITIONED BY (activity_date)
LOCATION 'abfss://curated@streamcorplake.dfs.core.windows.net/fct_user_engagement_daily/';


-- --------------------------------------------------------------
-- 5. MARTS (Gold)
-- --------------------------------------------------------------

CREATE TABLE IF NOT EXISTS marts.mart_user_engagement_weekly (
    user_id                     STRING,
    week_start_date             DATE,
    week_end_date               DATE,
    user_plan_at_week_start     STRING,
    user_status_at_week_start   STRING,
    total_watch_time_min        DECIMAL(10,2),
    session_count               INT,
    distinct_content_count      INT,
    distinct_genre_count        INT,
    days_active                 INT,
    avg_engagement_score        INT,
    week_over_week_change_pct   DECIMAL(10,2),
    engagement_trend            STRING,
    segment                     STRING
)
USING DELTA
PARTITIONED BY (week_start_date)
LOCATION 'abfss://marts@streamcorplake.dfs.core.windows.net/mart_user_engagement_weekly/';

-- --------------------------------------------------------------
-- 6. OPTIMIZATION (Run after initial load)
-- --------------------------------------------------------------

OPTIMIZE curated.fct_playback_events ZORDER BY (user_id, content_id);
OPTIMIZE curated.fct_sessions ZORDER BY (user_id);
OPTIMIZE curated.fct_user_engagement_daily ZORDER BY (user_id);
OPTIMIZE marts.mart_user_engagement_weekly ZORDER BY (segment, user_plan_at_week_start);

ANALYZE TABLE curated.fct_playback_events COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE curated.fct_user_engagement_daily COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE marts.mart_user_engagement_weekly COMPUTE STATISTICS FOR ALL COLUMNS;
