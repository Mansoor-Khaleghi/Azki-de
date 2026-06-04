# Azki — Senior Data Engineer Hiring Task

An end-to-end analytics pipeline that takes raw **user events** and a **users
table** and turns them into **query-ready tables in ClickHouse** — aggregates
for dashboards and a wide, denormalized purchase table for analytics/ML. The
stack runs locally with Docker Compose; the data steps are driven by a small
Python CLI (`python -m azki …`).

> The provided dataset is **confidential** and is git-ignored. Drop `users.csv`
> and `user_events.csv` into `data/` before running (see [`data/README.md`](data/README.md)).

---

## 1. The big picture (in plain terms)

```
   ┌─────────────┐        ┌──────────┐        ┌────────────────────────────────┐
   │  RAW INPUT  │        │ TRANSPORT│        │        WAREHOUSE (ClickHouse)  │
   └─────────────┘        └──────────┘        └────────────────────────────────┘

 user_events.csv ─► producer ─► Kafka topic ─► Kafka engine ─► enrich ─► events_enriched
                                  (user_events)                 ▲          │
                                                                │          ├─► events_agg_daily   ─► dashboards
   users.csv ─► MySQL ─────────► users_dict (lookup) ───────────┘          │   (count / sum / avg)
                                                                           │
                                4 product tables + financial ──► join ─────┴─► fact_purchases     ─► analytics / ML
                                (third, body, medical, fire)                   (one wide row per purchase)
```

1. **Events come in.** A producer reads `user_events.csv` and streams each row
   into a **Kafka** topic, keyed by `user_id` so one user's events stay in
   order. Kafka buffers between producers and the warehouse.
2. **The warehouse pulls events.** ClickHouse's built-in **Kafka engine** table
   reads the topic — it's the mouth of the pipe, not storage.
3. **Each event gets enriched.** A materialized view looks up the user's
   `city`/`device_type`/`signup_date` from `users_dict` (an in-memory copy of
   the MySQL `users` table) and writes the combined row to **`events_enriched`**,
   the durable raw layer. This lookup is the events↔users join.
4. **Two tables build automatically from `events_enriched`:**
   - **`events_agg_daily`** — `count / unique users / sum / avg` per day ×
     channel × city × device × event type, for fast dashboards.
   - **`fact_purchases`** — for every `purchase` event, the order details are
     attached. Orders live in 5 tables (4 product lines + a shared
     `financial_order`); a view `UNION`s the products and a materialized view
     `JOIN`s them onto the purchase into one wide row.
5. **Late-data safety net.** The streaming join only sees orders that exist when
   a purchase arrives. A scheduled, idempotent `reconcile` backfills purchases
   whose order landed late — eventually complete, never double-counted.

---

## 2. System design

```mermaid
flowchart LR
    subgraph Sources
        CSV[user_events.csv]
        MYSQL[(MySQL users)]
    end

    subgraph Kafka["Kafka (KRaft)"]
        TOPIC[(topic: user_events)]
        SR[Schema Registry]
        DLQ[(user_events.dlq)]
    end

    subgraph Connect["Kafka Connect"]
        DBZ[Debezium MySQL source]
        SINK[ClickHouse sink]
    end

    subgraph ClickHouse
        KE[[Kafka engine table]]
        DICT{{users_dict dictionary}}
        MV1([MV: enrich via dictGet])
        ENR[(events_enriched)]
        MV2([MV: aggregate])
        AGG[(events_agg_daily)]
        MV3([MV: denormalize purchases])
        ORD[(third/body/medical/fire + financial_order)]
        FACT[(fact_purchases)]
    end

    CSV -->|producer JSONEachRow| TOPIC
    MYSQL -. CDC .-> DBZ -.-> TOPIC
    MYSQL ==>|refreshed lookup| DICT
    TOPIC --> KE --> MV1
    DICT -. dictGet .-> MV1
    KE -. parse errors .-> DLQ
    MV1 --> ENR --> MV2 --> AGG
    ENR --> MV3 --> FACT
    ORD --> MV3
    SR -.contract.-> TOPIC

    AGG --> BI[BI / Dashboards]
    FACT --> ML[ML / Analytics]
```

| Component | Role |
|---|---|
| Producer | Replays `user_events.csv` into Kafka, keyed by `user_id` |
| Kafka (KRaft) | Event transport / buffer |
| Schema Registry | Topic contract (schema-drift defense) |
| MySQL | "Production" users table (OLTP source) |
| ClickHouse Kafka engine | Consumes the topic |
| `users_dict` | In-memory users lookup from MySQL (O(1) `dictGet`) |
| MV chain | enrich → aggregate → denormalize |
| `events_enriched` | Queryable raw fact layer |
| `events_agg_daily` | Pre-aggregated metrics (count/sum/avg) |
| `fact_purchases` | Denormalized purchases (events + order details) |
| reconcile | Idempotent gap-filler for late-arriving orders |
| Kafka Connect | Debezium source + ClickHouse sink (bonus path) |
| Prefect flows | ingest / monitoring / backfill orchestration |

The full diagram and component rationale are in
[`docs/architecture.md`](docs/architecture.md); the deeper write-up is in
[`docs/technical-report.md`](docs/technical-report.md).

---

## 3. Quick start

```bash
# 0. put the confidential dataset in place:
#      data/users.csv   data/user_events.csv

# 1. start the stack:
docker compose up -d                # Kafka, MySQL, ClickHouse

# 2. install the Python deps (the producer needs confluent-kafka):
pip install -r requirements.txt     # or: pip install -r requirements.lock

# 3. run the pipeline end-to-end:
python -m azki demo
#   = init -> seed -> produce -> reconcile -> verify
```

Or step through it:

```bash
python -m azki init        # create dictionary, Kafka source, MVs, tables
python -m azki seed        # generate + load the synthetic order tables (Part 2)
python -m azki produce     # stream user_events.csv into Kafka
python -m azki reconcile   # gap-fill late-arriving orders into fact_purchases
python -m azki verify      # show row counts + sample aggregates
python -m azki dq          # run the data-quality gate
python -m azki apply-opt   # Part 2 performance optimizations
python -m azki apply-gov   # Part 2 governance (roles, masked view, quotas)
```

Bring up the full stack (Schema Registry, Kafka Connect, Kafka-UI) and the
Prefect orchestration with Compose profiles:

```bash
docker compose up -d                                       # full stack
docker compose --profile orchestration up -d prefect       # Prefect UI at :4200
```

---

## 4. CLI command reference

| Command | What it does |
|---|---|
| `azki init` | Create the dictionary, Kafka source, MVs, and tables |
| `azki seed` | Generate + load the 5 synthetic order tables (Part 2) |
| `azki produce` | Stream `user_events.csv` into Kafka |
| `azki verify` | Row counts + sample aggregates from ClickHouse |
| `azki dq` | Run the data-quality gate (non-zero exit on FAIL) |
| `azki reconcile` | Idempotently gap-fill `fact_purchases` for late orders |
| `azki apply-opt` / `azki apply-gov` | Part 2 performance / governance SQL |
| `azki connect-register` | Register the Debezium + ClickHouse Connect connectors |
| `azki backfill START END` | Run the Spark backfill for a date window |
| `azki demo` | Full happy path: init → seed → produce → reconcile → verify |

Run `python -m azki <command> --help` for per-command flags. All commands read
connection settings/credentials from `.env`.

---

## 5. How it maps to the task

| Part | Where |
|---|---|
| **Part 1** — ingest events from Kafka, join MySQL users, aggregate into ClickHouse | `azki produce` + `clickhouse/part1/` + `connect/` |
| **Part 2** — denormalized purchase table via MVs, performance, governance | `clickhouse/part2/` (+ `azki seed/reconcile/apply-opt/apply-gov`) |
| **Part 3** — data-quality plan + monitoring, Spark backfill (bonus) | `quality/` + `spark/` + `orchestration/` |

---

## 6. Design decisions & reasoning

**The events↔users join is a ClickHouse dictionary, not a streaming join.**
`users` is a small, slowly-changing dimension. As a `HASHED()` dictionary
sourced from MySQL, the join is an in-memory `dictGet` evaluated at insert
time — O(1), no load on the OLTP hot path, auto-refreshed on a `LIFETIME`. A
streaming-join engine would add a second distributed system for a hash lookup.

**ClickHouse's Kafka engine is the primary consume path.** Reading the topic
directly lets the join + aggregation happen inside the warehouse via
materialized views. The Kafka Connect ClickHouse **sink** is also provided as
the alternative for when the warehouse should stay a pure sink with a DLQ.

**Two modeling layers.** `events_enriched` (MergeTree) is the queryable raw
truth; `events_agg_daily` (AggregatingMergeTree) holds partial states kept up to
date by a second MV, so dashboards read finalized states in milliseconds.

**Streaming MV + idempotent reconcile for denormalization.** An INNER-JOIN MV
denormalizes a purchase at ingest (low latency) but can't see an order that
lands later. A scheduled, idempotent `reconcile` (guarded by `order_id`)
guarantees eventual completeness without double-counting.

**Secrets come from `.env`, nowhere else.** Settings load from the environment,
falling back to the committed `.env` (local-demo creds). Files that must embed a
credential carry `${VAR}` placeholders the CLI fills at apply time, so in
production the same env vars come from a secret manager unchanged.

**Spark only for backfill.** The live join is a hash lookup (in ClickHouse). The
Spark job handles bounded batch backfill of historical data — shuffle-heavy
dedup + broadcast enrichment.

---

## 7. Trade-offs (and what production would change)

| Here (task scope) | Production |
|---|---|
| Single-node Kafka (KRaft), ClickHouse, MySQL | Multi-broker Kafka; `ReplicatedMergeTree` + Keeper; replicated MySQL |
| `JSONEachRow` on the topic (human-debuggable) | Avro/Protobuf under Schema Registry for hard contracts |
| Producer replays a CSV | Kafka Connect (Debezium) sources for events and users CDC |
| `.env` with demo creds | Secret manager injecting the same vars |
| At-least-once into ClickHouse | Same; dedup via `ReplacingMergeTree` + natural keys |
| Prefect runs flows in one container | Prefect server + workers; alerting on lag/freshness/parts |

---

## 8. Configuration

All settings live in [`.env`](.env), read by both the CLI and Compose. Process
env vars override the file, so the same code runs on the host (`localhost`) and
inside Compose (service names like `clickhouse:8123`, `kafka:9092`).

| Variable | Meaning |
|---|---|
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` | ClickHouse credentials |
| `CH_HOST` / `CH_PORT` | ClickHouse HTTP endpoint |
| `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_ROOT_PASSWORD` / `MYSQL_DATABASE` | MySQL credentials |
| `KAFKA_TOPIC_EVENTS` / `KAFKA_BOOTSTRAP_HOST` / `KAFKA_BOOTSTRAP_INTERNAL` | Kafka topic + bootstrap endpoints |

---

## 9. Tests

```bash
pip install -r requirements.txt   # (or just: pip install pytest)
python -m pytest
```

The suite covers the pure logic without the running stack: config precedence,
order generation (determinism + join-completeness + reproducible seed), the
producer transform, the DQ runner's pass/warn/fail accounting, the SQL splitter
and `${VAR}` renderer (asserting the dictionary SQL holds no literal password),
and the CLI parser. The Spark `enrich_window` transform has its own tests,
auto-skipped when PySpark isn't installed.

---

## 10. Bonus paths

**Kafka Connect** (Debezium MySQL source + ClickHouse sink, creds from `.env`):

```bash
docker compose up -d
python -m azki connect-register
```

**Spark backfill** — reprocess a date window idempotently:

```bash
python -m azki backfill 2025-10-01 2025-10-07
```

**Orchestration** (Prefect — schedules, retries, UI at `:4200`):

```bash
docker compose --profile orchestration up -d prefect
docker exec azki-prefect python orchestration/flows.py monitoring   # run now
```

The `azki-monitoring` flow runs `reconcile → DQ gate` on a 5-minute schedule;
`azki-ingest` runs a full produce → wait → reconcile → DQ cycle; each task
retries and the DQ gate fails the run on any `FAIL`.

---

## 11. Repository layout

| Path | What |
|---|---|
| `azki/` | Python package + CLI (config, ClickHouse client, orders, producer, quality) |
| `docker-compose.yml` | Full stack (Kafka KRaft, Schema Registry, Connect, UI, MySQL, ClickHouse, Spark, Prefect) |
| `clickhouse/part1/` | Dictionary, Kafka source, enrichment MV, aggregates |
| `clickhouse/part2/` | Order tables, denormalized MV, reconciliation, optimizations, governance |
| `ingestion/mysql/` | MySQL schema + CSV load + CDC grants |
| `connect/` | Debezium source + ClickHouse sink configs (`${VAR}` creds) |
| `quality/` | DQ plan + SQL checks (parity, offset-continuity, ingestion-lag, …) |
| `spark/` | PySpark idempotent backfill + local validation |
| `orchestration/` | Prefect flows (ingest / monitoring / backfill) |
| `tests/` | pytest suite |
| `docs/` | Architecture diagram + technical report |
| `requirements.txt` / `requirements.lock` | Direct deps / pinned lock |

## Service endpoints

| Service | URL |
|---|---|
| ClickHouse HTTP | http://localhost:8123 |
| Kafka (host) | localhost:29092 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Kafka-UI | http://localhost:8080 |
| MySQL | localhost:3306 |
| Prefect UI | http://localhost:4200 |
