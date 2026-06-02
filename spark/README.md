# Spark backfill (Part 3 bonus)

Reprocesses a date range of raw events from cold storage, re-enriches against
the users dimension, de-duplicates on the natural key, and loads ClickHouse
**idempotently**.

## Run

1. Create the target table:

```bash
docker exec -i azki-clickhouse clickhouse-client --user azki --password azkipw \
  --multiquery < spark/backfill_target.sql
```

2. Submit the job (ClickHouse JDBC driver pulled via `--packages`):

```bash
docker compose --profile spark run --rm spark \
  /opt/spark/bin/spark-submit \
    --packages com.clickhouse:clickhouse-jdbc:0.6.3,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
    /opt/app/backfill_job.py \
    --start 2025-10-01 --end 2025-10-07 \
    --events /opt/data/user_events.csv \
    --users /opt/data/users.csv \
    --ch-url "jdbc:clickhouse://clickhouse:8123/azki" \
    --target events_enriched_backfill
```

Add `--overwrite` for a hard re-statement (clears the target date range first
via `ALTER TABLE … DELETE`).

## Quick local validation (no cluster, no JDBC sink)

To exercise the enrich/dedup transform on a local SparkSession against the real
CSVs — handy for development and CI:

```bash
pip install pyspark==3.5.3
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
python spark/validate_backfill.py --start 2025-10-01 --end 2025-10-07
```

It prints window counts, unmatched-user count, a sample, and an idempotency
assertion. (Validated locally: the 2025-10-01..07 window yields 4,830 enriched
rows, 0 unmatched, deterministic across reruns.)

## Idempotency

- `dropDuplicates` on `(user_id, session_id, event_time, event_type)` in Spark.
- `ReplacingMergeTree` target collapses any residual duplicates on merge.
- `--overwrite` deletes the window before insert for full restatements.

## Why Spark here (and not for the Part 1 join)

The live join is a stream↔dimension lookup, best done in ClickHouse via
`dictGet`. Backfill is a **bounded batch** over historical partitions with
shuffle-heavy dedup and broadcast enrichment — exactly Spark's strength.
