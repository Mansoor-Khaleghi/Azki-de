# Azki — Senior Data Engineer Hiring Task

An end-to-end ETL pipeline: **Kafka → (join with MySQL users) → ClickHouse**,
with a denormalized purchase fact table via **materialized views**, a
**data-quality** gate, and a **Spark** backfill job — all runnable locally with
Docker Compose.

> The provided dataset is **confidential** and is git-ignored. Place
> `users.csv` and `user_events.csv` in `data/` before running.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full diagram.

```
user_events.csv ─► Python producer ─► Kafka ─► ClickHouse Kafka engine
                                                      │ MV + dictGet
   MySQL users ──(ClickHouse dictionary)──────────────┘
                                                      ▼
                                  events_enriched ─► events_agg_daily (count/sum/avg)
                                                  └─► fact_purchases (denormalized, Part 2)
```

## Quick start

```bash
# 0. put users.csv + user_events.csv in ./data/, then:
pip install -r ingestion/producer/requirements.txt

make up          # start kafka + mysql + clickhouse
make ch-init     # create dictionary, Kafka source, MVs, tables
make seed-orders # generate + load synthetic order tables (Part 2)
make produce     # stream events into Kafka
make denorm-reconcile # gap-fill late-arriving orders into fact_purchases
make verify      # show counts + sample aggregates
make dq          # run the data-quality gate
make apply-opt   # Part 2 performance optimizations (projections, skip indexes)
make apply-gov   # Part 2 governance (roles, masked view, quotas)

# or the whole happy path (up -> schema -> orders -> stream -> reconcile -> verify):
make demo
```

Bonus stack (Schema Registry, Kafka Connect, Kafka-UI):

```bash
make up-bonus
# register connectors:
curl -XPOST -H 'Content-Type:application/json' \
  --data @connect/mysql-users-source.json localhost:8083/connectors
curl -XPOST -H 'Content-Type:application/json' \
  --data @connect/clickhouse-events-sink.json localhost:8083/connectors
```

Spark backfill (Part 3 bonus):

```bash
docker compose --profile spark run --rm spark \
  spark-submit --packages com.clickhouse:clickhouse-jdbc:0.6.3 \
  /opt/app/backfill_job.py --start 2025-10-01 --end 2025-10-07
```

## Repository layout

| Path | What |
|---|---|
| `docker-compose.yml` | Full stack (Kafka KRaft, Schema Registry, Connect, UI, MySQL, ClickHouse) |
| `ingestion/producer/` | Python Kafka producer |
| `ingestion/mysql/` | MySQL schema + CSV load + CDC grants |
| `ingestion/generate_orders.py` | Synthetic order generator (Part 2) |
| `clickhouse/part1/` | Dictionary, Kafka source, enrichment MV, aggregates |
| `clickhouse/part2/` | Order tables, denormalized MV, optimizations, governance |
| `connect/` | Debezium source + ClickHouse sink configs (bonus) |
| `quality/` | DQ plan, SQL checks, runner |
| `spark/` | PySpark idempotent backfill |
| `docs/` | Architecture diagram + technical report |

## Service endpoints

| Service | URL |
|---|---|
| ClickHouse HTTP | http://localhost:8123 |
| Kafka (host) | localhost:29092 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Kafka-UI | http://localhost:8080 |
| MySQL | localhost:3306 |

Each part maps to: **Part 1** → `clickhouse/part1/` + `ingestion/` + `connect/`;
**Part 2** → `clickhouse/part2/`; **Part 3** → `quality/` + `spark/`.
