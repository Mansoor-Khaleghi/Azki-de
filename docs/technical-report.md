# Azki — Senior Data Engineer Task: Technical Report

## 1. Overview

This project implements the three-part task end-to-end on a reproducible local
stack (`docker compose up` + `make demo`):

1. **Ingestion & modeling** — `user_events.csv` is streamed through **Kafka**,
   joined against the **MySQL `users`** table, and **aggregated into
   ClickHouse**.
2. **Query performance & governance** — a **denormalized `fact_purchases`**
   table is built with **materialized views** that union 4 product-order tables
   and join the `financial_order` table; plus performance and access-control
   measures.
3. **Data quality & monitoring** — an executable DQ gate plus a monitoring plan,
   and a **Spark** backfill job.

The dataset profile: 5,000 users (5 cities, 3 device types) and 20,000 events
over October 2025 across 4 event types (`signup`, `quote_view`, `policy_click`,
`purchase`) and 5 acquisition channels.

## 2. Part 1 — Data modeling & ingestion

### Flow
`producer → Kafka topic (JSONEachRow) → ClickHouse Kafka engine → MV (enrich via
dictGet) → events_enriched → MV (aggregate) → events_agg_daily`.

### Key design decisions

- **JSONEachRow over Avro** for the demo path: robust, human-debuggable, and
  natively understood by the ClickHouse Kafka engine. Schema Registry is still
  in the stack to demonstrate contract enforcement (Part 3).
- **The join as a ClickHouse dictionary.** `users` is a small, slowly-changing
  dimension. Modelling it as a `HASHED()` dictionary sourced from MySQL means
  the events↔users join is an in-memory `dictGet` evaluated inside the
  enrichment MV — O(1), no hot-path hit on MySQL, and auto-refreshed by
  `LIFETIME`. This is cheaper and simpler than a streaming join engine.
- **Two-layer modelling.** `events_enriched` (MergeTree) is the queryable raw
  truth; `events_agg_daily` (AggregatingMergeTree, fed by a second MV) holds
  partial `count/uniq/sum/avg` states per
  `day × channel × city × device × event_type`. Dashboards read finalized
  states from the `events_agg_daily_v` view — milliseconds, not full scans.
- **Resilience:** `kafka_handle_error_mode='stream'` and
  `input_format_skip_unknown_fields=1` keep a poison/extra-field message from
  stalling the consumer.

### Bonus — Kafka cluster & Connect
Compose brings up Kafka (KRaft), Schema Registry, Kafka Connect, and Kafka-UI.
`connect/` holds two connector configs: a **Debezium MySQL source** (users CDC →
topic) and a **ClickHouse sink** (topic → table, with a DLQ). The report's
primary path uses the Kafka engine because it lets the join+aggregation happen
inside ClickHouse via MVs; the sink connector is the alternative when the
warehouse should remain a pure sink.

## 3. Part 2 — Denormalization, performance & governance

### Denormalized table
The 5 production tables aren't shipped with the dataset, so they are modelled
and synthesized (`ingestion/generate_orders.py`) keyed to the real purchase
events. Each product line (`third`, `body`, `medical`, `fire`) is its own table
because each carries line-specific attributes; `product_orders_all` is a `VIEW`
that **UNION ALL**s them to a common grain with line-specific fields folded into
a `Map`. `mv_fact_purchases` fires on every `purchase` event in
`events_enriched`, **joins** the unioned product orders and the
`financial_order` table, and writes one wide row to `fact_purchases`.

**Late-arriving orders.** An INNER-JOIN MV denormalizes at ingest time, so a
purchase consumed *before* its order row exists would be dropped permanently.
To guarantee completeness I pair the streaming MV with an idempotent
reconciliation query (`14-denorm-reconcile.sql`, `make denorm-reconcile`) that
inserts only the purchases not yet present (guarded by `order_id`,
`LIMIT 1 BY order_id`). The MV is the low-latency happy path; the reconciliation
— run on a schedule — closes any gaps. This is the production-correct pattern
and makes the demo deterministic (verified: exactly 4,892 rows).

### Performance optimizations
- `LowCardinality(String)` on all categorical columns.
- Sort keys aligned to real filter/group order; monthly partitioning (avoids
  over-partitioning); `DoubleDelta+ZSTD` codecs on timestamps.
- A **projection** (`proj_revenue_by_line`) pre-sorts revenue-by-line-over-time;
  **bloom-filter skip indexes** for off-sort-key point lookups (`user_id`,
  `session_id`).
- **AggregatingMergeTree** pre-aggregation turns dashboard scans into tiny
  state reads. `ReplacingMergeTree` on order tables gives idempotent CDC upserts.
- **TTL** (18 months) on raw events for retention/cost.

### Governance & access control
RBAC roles (`analyst`, `data_scientist`, `finance`, `pipeline_rw`) with
least-privilege grants; a **PII/financial-masked view** (`fact_purchases_masked`)
for analysts; an example **row policy** for regional segregation; **quotas** and
**read-only settings constraints** to protect the cluster; retention TTL and
`ALTER … DELETE` for subject erasure; audit via `system.query_log`. Connector
secrets stay in env/secret stores, never in the repo.

## 4. Part 3 — Data quality & monitoring

A layered plan (in-stream / at-rest / observability) in
[`quality/DATA_QUALITY.md`](../quality/DATA_QUALITY.md) covering: **sync/delay**
(consumer lag, freshness SLA), **missing events** (row-count parity, referential
integrity, volume anomaly, gap detection), **schema drift** (Schema Registry,
error-stream→DLQ, DDL hashing), and **load monitoring** (`system.*` tables →
Prometheus/Grafana). The executable gate (`run_quality_checks.py` over
`dq_checks.sql`) exits non-zero on failure for CI/Airflow.

A real finding surfaced by the data: **`premium_amount` is populated on
non-purchase events**, which is semantically wrong — flagged as a `WARN` check
and the kind of issue that opens a data-contract ticket.

### Bonus — Spark backfill
`spark/backfill_job.py` reprocesses a date range from cold storage, re-enriches
against users (broadcast join), de-duplicates on the natural key, and loads
ClickHouse **idempotently** (ReplacingMergeTree + optional partition-scoped
`ALTER … DELETE` for hard restatements).

## 5. Verified results (actual local run)

The pipeline was run end-to-end on the provided dataset:

| Metric | Value |
|---|---|
| Events streamed → consumed by ClickHouse | 20,000 / 20,000 |
| Enrichment coverage (users matched) | 20,000 / 20,000 (0 `UNKNOWN`) |
| Distinct users seen in events | 4,916 |
| Purchase events → `fact_purchases` rows | 4,892 → 4,892 (across 4 product lines) |
| DQ gate | 7 PASS, 1 WARN, 0 FAIL |

The single WARN is the genuine data issue: **15,108 non-purchase events carry a
`premium_amount`**. Both the projection (`force_optimize_projection=1` succeeds)
and the bloom-filter skip indexes were confirmed active.

## 6. Trade-offs & what I'd do next in production

- **Single-node** everything for the task; production = replicated ClickHouse
  (`ReplicatedMergeTree` + Keeper), multi-broker Kafka, and Kafka Connect for
  both source and sink rather than a producer script.
- **Exactly-once:** the producer uses idempotent delivery; the Kafka engine path
  is at-least-once into ClickHouse — dedup via `ReplacingMergeTree`/natural keys.
- **Schema:** move from JSON to Avro/Protobuf under Schema Registry for stronger
  contracts once producers are owned by other teams.
- **Orchestration:** wrap schema deploys, the DQ gate, and backfills in Airflow;
  ship metrics to Grafana with alerting on lag/freshness/parts.
