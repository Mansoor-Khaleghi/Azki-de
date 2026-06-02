-- =====================================================================
--  Part 1 — Enriched events landing table + ingestion materialized view.
--
--  The MV drains the Kafka source, enriches each event with the user
--  dimension via dictGet (the stream<->users JOIN), and writes to a
--  MergeTree. This is the durable, queryable raw layer.
-- =====================================================================

-- Durable storage. LowCardinality on the categorical columns keeps it
-- compact and fast to filter/group. Delta+ZSTD on the timestamp.
CREATE TABLE IF NOT EXISTS azki.events_enriched
(
    event_time     DateTime CODEC(DoubleDelta, ZSTD(1)),
    user_id        UInt32,
    session_id     String,
    event_type     LowCardinality(String),
    channel        LowCardinality(String),
    premium_amount Nullable(Float64),
    -- enriched-from-users columns:
    city           LowCardinality(String),
    device_type    LowCardinality(String),
    signup_date    Date,
    -- lineage / observability:
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, user_id)
TTL event_time + INTERVAL 18 MONTH;   -- retention policy (governance)

-- The stream join: Kafka source -> enrich via users_dict -> MergeTree.
CREATE MATERIALIZED VIEW IF NOT EXISTS azki.mv_events_enriched
TO azki.events_enriched
AS
SELECT
    event_time,
    user_id,
    session_id,
    event_type,
    channel,
    premium_amount,
    dictGetOrDefault('azki.users_dict', 'city',        toUInt64(user_id), 'UNKNOWN') AS city,
    dictGetOrDefault('azki.users_dict', 'device_type', toUInt64(user_id), 'UNKNOWN') AS device_type,
    dictGetOrDefault('azki.users_dict', 'signup_date', toUInt64(user_id), toDate('1970-01-01')) AS signup_date
FROM azki.kafka_user_events
WHERE length(_error) = 0;   -- drop messages that failed to parse
