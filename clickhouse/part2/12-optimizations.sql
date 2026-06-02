-- =====================================================================
--  Part 2 — Query-performance optimizations for the analytical layer.
--  Apply after the tables exist. Each block notes the query pattern it
--  accelerates and the trade-off it carries.
-- =====================================================================

-- 1) PROJECTION: pre-sorted/pre-aggregated copy for a common BI pattern
--    (revenue by product line over time). ClickHouse picks it automatically
--    when a query matches; cost is extra write amplification + storage.
ALTER TABLE azki.fact_purchases
    ADD PROJECTION IF NOT EXISTS proj_revenue_by_line
    (
        SELECT
            product_line,
            toStartOfDay(event_time) AS day,
            sum(net_amount)          AS revenue,
            count()                  AS orders
        GROUP BY product_line, day
    );
ALTER TABLE azki.fact_purchases MATERIALIZE PROJECTION proj_revenue_by_line;

-- 2) DATA-SKIPPING INDEX: point lookups by user_id are off the sort key,
--    so a bloom filter lets ClickHouse skip granules that can't match.
ALTER TABLE azki.fact_purchases
    ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE azki.fact_purchases MATERIALIZE INDEX idx_user_id;

-- 3) Same idea on the raw layer for user-centric ad-hoc queries.
ALTER TABLE azki.events_enriched
    ADD INDEX IF NOT EXISTS idx_session session_id TYPE bloom_filter(0.01) GRANULARITY 4;

-- =====================================================================
--  Additional optimizations applied structurally elsewhere (documented
--  here so the rationale lives next to the SQL):
--
--   * LowCardinality(String) on every categorical column (channel, city,
--     device_type, event_type, product_line, payment_*). Dictionary-
--     encodes them -> smaller, faster GROUP BY / WHERE.
--   * ORDER BY keys chosen to match real filter/group order:
--       events_enriched   -> (event_type, event_time, user_id)
--       fact_purchases    -> (city, product_line, event_time, user_id)
--       events_agg_daily  -> (event_date, event_type, channel, city, ...)
--   * PARTITION BY toYYYYMM(...) — prunes whole months; keeps part count
--     sane (avoids the over-partitioning anti-pattern of daily partitions).
--   * CODEC(DoubleDelta, ZSTD) on timestamps — monotonic-ish values
--     compress extremely well.
--   * AggregatingMergeTree pre-aggregation (Part 1) — turns dashboard
--     scans of millions of rows into reads of a few thousand state rows.
--   * ReplacingMergeTree on order tables — idempotent CDC upserts; dedup
--     on merge. Use FINAL / argMax in queries needing the latest version.
--   * TTL on raw events (18 months) — automatic retention/cost control.
-- =====================================================================

-- Inspect what a query actually does:
--   EXPLAIN indexes = 1
--   SELECT product_line, sum(net_amount)
--   FROM azki.fact_purchases
--   WHERE event_time >= '2025-10-01'
--   GROUP BY product_line;
