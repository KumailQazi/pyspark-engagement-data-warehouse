-- ============================================================
-- data_freshness.sql
-- Operational Monitoring Query
-- Checks the freshness of the latest playback events to detect
-- upstream ingestion delays or pipeline failures.
-- ============================================================

SELECT
    MAX(event_ts) AS latest_event_ts,
    MAX(server_ts) AS latest_server_ts,
    CURRENT_TIMESTAMP() AS current_time_utc,
    TIMESTAMPDIFF(MINUTE, MAX(server_ts), CURRENT_TIMESTAMP()) AS delay_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(server_ts), CURRENT_TIMESTAMP()) > 60 THEN 'CRITICAL: Data is older than 60 mins'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(server_ts), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING: Data is older than 30 mins'
        ELSE 'HEALTHY'
    END AS freshness_status
FROM curated.fct_playback_events;
