# Spark backfill (Part 3 bonus)

Reprocesses a date range of raw events from cold storage, re-enriches against
the users dimension, de-duplicates on the natural key, and loads ClickHouse
**idempotently**.

Spark does the heavy compute (broadcast re-enrichment + natural-key dedup) and
stages the result as a single CSV. The `azki backfill` CLI then creates the
`ReplacingMergeTree` target and loads that CSV over plain HTTP — the same
ingestion path the rest of the pipeline uses. Decoupling the load from a
Spark↔ClickHouse JDBC driver keeps it robust across ClickHouse-server versions.

## Run

```bash
python -m azki backfill 2025-10-01 2025-10-07
```

That:

1. applies `spark/backfill_target.sql` (creates `azki.events_enriched_backfill`);
2. runs the job in the Spark container, which writes `spark/_backfill_out/part-*.csv`:

```bash
docker compose --profile spark run --rm spark \
  /opt/spark/bin/spark-submit /opt/app/backfill_job.py \
    --start 2025-10-01 --end 2025-10-07 \
    --events /opt/data/user_events.csv --users /opt/data/users.csv \
    --out /opt/app/_backfill_out
```

3. loads the staged CSV into the target over HTTP and prints the row count.

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
- `ReplacingMergeTree` target collapses any residual duplicates on merge —
  re-running the same window stays at the same deduped count (read with `FINAL`).

## Why Spark here (and not for the Part 1 join)

The live join is a stream↔dimension lookup, best done in ClickHouse via
`dictGet`. Backfill is a **bounded batch** over historical partitions with
shuffle-heavy dedup and broadcast enrichment — exactly Spark's strength.
