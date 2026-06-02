-- =====================================================================
--  Part 1 — Aggregated results loaded into ClickHouse (the task's goal).
--
--  AggregatingMergeTree holds partial aggregate states; a second MV keeps
--  it incrementally up to date as enriched events arrive. Querying with
--  the -Merge combinators finalizes the result. This gives count / sum /
--  avg per (day x channel x city x device x event_type) at any scale.
-- =====================================================================

CREATE TABLE IF NOT EXISTS azki.events_agg_daily
(
    event_date     Date,
    channel        LowCardinality(String),
    city           LowCardinality(String),
    device_type    LowCardinality(String),
    event_type     LowCardinality(String),
    events_count   AggregateFunction(count),
    unique_users   AggregateFunction(uniq, UInt32),
    premium_sum    AggregateFunction(sum, Float64),
    premium_avg    AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, channel, city, device_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.mv_events_agg_daily
TO azki.events_agg_daily
AS
SELECT
    toDate(event_time)                       AS event_date,
    channel,
    city,
    device_type,
    event_type,
    countState()                             AS events_count,
    uniqState(user_id)                       AS unique_users,
    sumState(ifNull(premium_amount, 0.))     AS premium_sum,
    avgState(ifNull(premium_amount, 0.))     AS premium_avg
FROM azki.events_enriched
GROUP BY event_date, channel, city, device_type, event_type;

-- ─── Convenience view that finalizes the states for humans/BI ───
CREATE VIEW IF NOT EXISTS azki.events_agg_daily_v AS
SELECT
    event_date,
    channel,
    city,
    device_type,
    event_type,
    countMerge(events_count)  AS events_count,
    uniqMerge(unique_users)   AS unique_users,
    sumMerge(premium_sum)     AS premium_sum,
    avgMerge(premium_avg)     AS premium_avg
FROM azki.events_agg_daily
GROUP BY event_date, channel, city, device_type, event_type;

-- Example consumer query:
--   SELECT event_date, channel, sum(events_count) AS c
--   FROM azki.events_agg_daily_v
--   WHERE event_type = 'purchase'
--   GROUP BY event_date, channel ORDER BY event_date, c DESC;
