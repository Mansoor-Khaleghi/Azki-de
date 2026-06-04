# Data Quality & Monitoring Plan (Part 3)

This plan covers the pipeline `CSV → Kafka → ClickHouse (MVs) → facts/aggregates`.
Checks are layered: **in-stream** (catch at ingest), **at-rest** (validate the
warehouse), and **observability** (dashboards + alerts on the infrastructure).

The executable subset lives in [`dq_checks.sql`](dq_checks.sql), run by
`python -m azki dq` (implemented in [`azki/quality.py`](../azki/quality.py)) — a
gate that exits non-zero on any `FAIL`, ready to drop into Airflow/CI.

---

## 1. Sync & delay issues

| Risk | Detection | Action |
|---|---|---|
| ClickHouse falls behind the topic | Kafka **consumer lag** per partition (`kafka-consumer-groups`, Kafka-UI, or `system.kafka_consumers`) | Alert when lag > threshold for N minutes; scale `kafka_num_consumers`. |
| Stale warehouse | **Freshness check**: `now() - max(_ingested_at)` (check #6) | Page if freshness SLA (e.g. 5 min) is breached. |
| Pipeline latency | **Ingestion-lag check** (check #10): `p95`/`max` of `ingest_lag_sec` (produce→consume, from persisted `kafka_timestamp`) | Alert when p95 lag exceeds SLA. |
| Late-arriving orders vs purchase events | Compare `fact_purchases` count vs purchase events (check #7) | Periodic reconciliation MV / batch backfill for the gap. |

## 2. Missing events

- **Offset-continuity** (check #9): every enriched row persists its Kafka
  `partition`/`offset`; per partition `max(offset) - min(offset) + 1` must equal
  the distinct-offset count. Any shortfall is an *exact* count of dropped
  messages — a far stronger signal than row totals, and it pinpoints which
  offset ranges to replay.
- **Row-count parity** (check #1): ClickHouse count vs the source/producer
  count (the producer logs `delivered`; reconcile against `events_enriched`).
- **Referential integrity** (check #2): events whose `user_id` doesn't resolve
  in `users_dict` land as `city='UNKNOWN'` — counted and alerted.
- **Volume anomaly detection**: events-per-hour vs the trailing 7-day baseline;
  a sudden drop signals an upstream outage or a dropped partition.
- **Gap detection**: the offset ranges above (or per-day counts) trigger the
  Spark backfill ([`spark/backfill_job.py`](../spark/backfill_job.py)).

## 3. Schema drift

- **Schema Registry** (in the stack) enforces a contract on the topic; producers
  that violate it are rejected at publish time.
- ClickHouse Kafka engine uses `kafka_handle_error_mode='stream'` +
  `input_format_skip_unknown_fields=1`: unparseable messages go to a virtual
  error stream (route to a **dead-letter table**) instead of stalling the
  consumer. New optional fields are tolerated; the MV filters `_error`.
- **DDL drift**: hash the `SHOW CREATE TABLE` of critical tables in CI; alert on
  unreviewed change.
- The Connect sink config carries a **DLQ** (`user_events.dlq`) for the
  connector-based path.

## 4. Load monitoring

| Signal | Source |
|---|---|
| Insert throughput / errors | `system.query_log`, `system.errors` |
| Merge/parts health (avoid "too many parts") | `system.parts`, `system.merges` |
| Materialized-view failures | `system.query_views_log` |
| Consumer state & exceptions | `system.kafka_consumers` |
| Disk / memory / replication | ClickHouse `system.metrics`, `system.asynchronous_metrics` |

Wire these into **Prometheus + Grafana** (ClickHouse exposes a Prometheus
endpoint) with alerts on: consumer lag, freshness SLA, parts-per-partition,
failed inserts, and DQ-gate failures.

## 5. Known data issue (surfaced by this dataset)

`premium_amount` is populated on **non-purchase** events (`quote_view`,
`policy_click`, `signup`), which is semantically wrong — premium should only
exist for `purchase`. Check #4 reports it as a `WARN`. In production this is the
kind of finding that opens a data-contract ticket with the producing team.

## 6. Where checks run

```
producer ──► Kafka ──► [Schema Registry contract]
                         │
                         ▼
                 ClickHouse Kafka engine  ── parse errors ──► DLQ table
                         │ MV (+dictGet join)
                         ▼
                 events_enriched ──► azki dq  (gate, scheduled)
                         │ MVs                    │
                         ▼                        ▼
              fact_purchases / aggregates   Grafana dashboards + alerts
```
