-- Target table for the Spark backfill job. ReplacingMergeTree gives
-- idempotency: re-running a backfill for the same window collapses duplicate
-- rows on merge (use FINAL or argMax at read time for the deduped view).
CREATE TABLE IF NOT EXISTS azki.events_enriched_backfill
(
    event_time     DateTime,
    user_id        UInt32,
    session_id     String,
    event_type     LowCardinality(String),
    channel        LowCardinality(String),
    premium_amount Nullable(Float64),
    city           LowCardinality(String),
    device_type    LowCardinality(String),
    signup_date    String,
    _backfilled_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_backfilled_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, session_id, event_time, event_type);
