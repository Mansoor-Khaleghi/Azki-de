-- =====================================================================
--  Target table for the Kafka Connect ClickHouse sink (bonus path).
--
--  The ClickHouse sink connector does not create tables — it writes the
--  `user_events` topic into a table of the same name, matching columns by
--  JSON field. This is the "pure sink" alternative to the Kafka table
--  engine; `azki connect-register` applies this before registering.
-- =====================================================================

CREATE TABLE IF NOT EXISTS azki.user_events
(
    event_time     DateTime,
    user_id        UInt32,
    session_id     String,
    event_type     String,
    channel        String,
    premium_amount Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (user_id, event_time);
