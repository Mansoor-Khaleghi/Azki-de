-- =====================================================================
--  Part 1 — Kafka source table (ClickHouse Kafka table engine).
--
--  This table is a CONSUMER, not storage: each SELECT advances the
--  consumer-group offset. We never query it directly; a materialized
--  view (03) drains it into a MergeTree. kafka_handle_error_mode='stream'
--  routes bad messages to a virtual error stream instead of stalling the
--  consumer (schema-drift resilience — see Part 3).
-- =====================================================================

CREATE TABLE IF NOT EXISTS azki.kafka_user_events
(
    event_time     DateTime,
    user_id        UInt32,
    session_id     String,
    event_type     String,
    channel        String,
    premium_amount Nullable(Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list       = 'kafka:9092',
    kafka_topic_list        = 'user_events',
    kafka_group_name        = 'clickhouse_user_events',
    kafka_format            = 'JSONEachRow',
    kafka_num_consumers     = 1,
    kafka_max_block_size    = 1048576,
    kafka_handle_error_mode = 'stream',     -- don't die on a poison message
    input_format_skip_unknown_fields = 1;   -- forward-compatible w/ new fields
