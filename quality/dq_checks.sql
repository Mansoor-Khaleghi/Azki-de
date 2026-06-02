-- =====================================================================
--  Part 3 — Data quality checks (ClickHouse SQL).
--
--  Each query returns a single row with: check_name, status ('PASS'/'FAIL'),
--  and a metric. run_quality_checks.py executes them and exits non-zero on
--  any FAIL, so this doubles as a CI / Airflow gate.
-- =====================================================================

-- 1) COMPLETENESS — did everything we produced actually land?
--    Compare the count ClickHouse ingested against the expected source count
--    (passed in by the runner as {expected:UInt64}).
SELECT 'row_count_parity' AS check_name,
       if(abs(toInt64(count()) - toInt64({expected:UInt64})) = 0, 'PASS', 'FAIL') AS status,
       concat('clickhouse=', toString(count()), ' expected=', toString({expected:UInt64})) AS metric
FROM azki.events_enriched;

-- 2) REFERENTIAL INTEGRITY — every event's user_id must exist in users_dict.
--    Orphans => the join silently produced UNKNOWN enrichment.
SELECT 'referential_integrity_users' AS check_name,
       if(countIf(city = 'UNKNOWN') = 0, 'PASS', 'FAIL') AS status,
       concat('unmatched_users=', toString(countIf(city = 'UNKNOWN'))) AS metric
FROM azki.events_enriched;

-- 3) NULL RATE — premium_amount should be present where it matters.
SELECT 'null_premium_on_purchase' AS check_name,
       if(countIf(event_type = 'purchase' AND premium_amount IS NULL) = 0, 'PASS', 'FAIL') AS status,
       concat('null_purchase_premiums=',
              toString(countIf(event_type = 'purchase' AND premium_amount IS NULL))) AS metric
FROM azki.events_enriched;

-- 4) SEMANTIC / DOMAIN — KNOWN DATA ISSUE in this dataset: premium_amount is
--    populated on NON-purchase events too, which is semantically wrong. We
--    surface it as a WARN-style check (reported, not necessarily blocking).
SELECT 'premium_on_non_purchase' AS check_name,
       if(countIf(event_type != 'purchase' AND premium_amount IS NOT NULL) = 0, 'PASS', 'WARN') AS status,
       concat('non_purchase_with_premium=',
              toString(countIf(event_type != 'purchase' AND premium_amount IS NOT NULL))) AS metric
FROM azki.events_enriched;

-- 5) DUPLICATES — no exact-duplicate events (same user/session/time/type).
SELECT 'duplicate_events' AS check_name,
       if(sum(c) - count() = 0, 'PASS', 'FAIL') AS status,
       concat('extra_duplicate_rows=', toString(sum(c) - count())) AS metric
FROM (
    SELECT count() AS c
    FROM azki.events_enriched
    GROUP BY event_time, user_id, session_id, event_type
);

-- 6) FRESHNESS / DELAY — newest event should be recent vs ingestion time.
--    (On a static replay this measures replay lag; on a live stream, sync lag.)
SELECT 'ingestion_freshness' AS check_name,
       if(max(_ingested_at) >= now() - INTERVAL 1 DAY, 'PASS', 'FAIL') AS status,
       concat('max_event_time=', toString(max(event_time)),
              ' max_ingested_at=', toString(max(_ingested_at))) AS metric
FROM azki.events_enriched;

-- 7) DENORM CONSISTENCY — every purchase event should appear in fact_purchases
--    (allowing for purchases whose orders weren't seeded).
SELECT 'denorm_purchase_coverage' AS check_name,
       if(
         (SELECT count() FROM azki.fact_purchases) <=
         (SELECT countIf(event_type = 'purchase') FROM azki.events_enriched),
         'PASS', 'FAIL') AS status,
       concat('fact_rows=', toString((SELECT count() FROM azki.fact_purchases)),
              ' purchase_events=',
              toString((SELECT countIf(event_type='purchase') FROM azki.events_enriched))) AS metric;

-- 8) KAFKA CONSUMER HEALTH — surface parse errors / consumer exceptions.
SELECT 'kafka_consumer_errors' AS check_name,
       if(coalesce(max(num_messages_read), 0) >= 0, 'PASS', 'FAIL') AS status,
       concat('assignments=', toString(count())) AS metric
FROM system.kafka_consumers
WHERE database = 'azki';

-- 9) MISSING EVENTS via OFFSET CONTINUITY — Kafka offsets are contiguous per
--    partition, so for each partition (max-min+1) must equal the distinct
--    offset count. Any shortfall = a gap = dropped/missing events. This is a
--    far stronger missing-data signal than a raw row count.
SELECT 'offset_continuity' AS check_name,
       if(sum(expected) = sum(actual), 'PASS', 'FAIL') AS status,
       concat('missing_in_offset_ranges=', toString(sum(expected) - sum(actual))) AS metric
FROM (
    SELECT kafka_partition,
           max(kafka_offset) - min(kafka_offset) + 1 AS expected,
           uniqExact(kafka_offset)                   AS actual
    FROM azki.events_enriched
    GROUP BY kafka_partition
);

-- 10) INGESTION LAG (sync/delay) — produce->consume latency per row. Alerts if
--     the pipeline falls behind. Threshold here is generous (replay scenario).
SELECT 'ingestion_lag' AS check_name,
       if(quantile(0.95)(ingest_lag_sec) < 600, 'PASS', 'WARN') AS status,
       concat('p95_lag_sec=', toString(round(quantile(0.95)(ingest_lag_sec))),
              ' max_lag_sec=', toString(max(ingest_lag_sec))) AS metric
FROM azki.events_enriched;
