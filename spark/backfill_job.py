#!/usr/bin/env python3
"""
Azki DE Task — Spark backfill job (Part 3 bonus).

Handles missing-data / backfill scenarios: re-process a date range of raw
events from cold storage (here: the original CSV; in production: S3/HDFS
parquet partitioned by day), re-enrich against the users table, and stage the
result so it can be loaded into ClickHouse IDEMPOTENTLY.

Spark does the heavy compute (broadcast re-enrichment + natural-key dedup) and
writes a single CSV; the `azki backfill` CLI then loads that CSV into the
ReplacingMergeTree target over plain HTTP — the same ingestion path the rest of
the pipeline uses, so the load is decoupled from any Spark↔ClickHouse JDBC
driver/version coupling. Re-running the same window is idempotent: the target's
natural key (user_id, session_id, event_time, event_type) collapses duplicates
on merge.

Run (usually via `python -m azki backfill START END`):
  spark-submit backfill_job.py --start 2025-10-01 --end 2025-10-07 \
    --events /opt/data/user_events.csv --users /opt/data/users.csv \
    --out /opt/app/_backfill_out
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

EVENTS_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("channel", StringType()),
    StructField("premium_amount", DoubleType()),
])

USERS_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("signup_date", StringType()),
    StructField("city", StringType()),
    StructField("device_type", StringType()),
])


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="inclusive YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="inclusive YYYY-MM-DD")
    ap.add_argument("--events", default="/opt/data/user_events.csv")
    ap.add_argument("--users", default="/opt/data/users.csv")
    ap.add_argument("--out", default="/opt/app/_backfill_out",
                    help="dir for the staged CSV (the CLI loads it into ClickHouse)")
    return ap.parse_args()


def enrich_window(events, users, start, end):
    """Core backfill transform (extracted so it is unit-testable without a
    ClickHouse sink): filter to the date window, re-enrich against users via a
    broadcast join, and de-duplicate on the natural key for idempotency."""
    window = (events
              .withColumn("event_ts", F.to_timestamp("event_time"))
              .withColumn("event_date", F.to_date("event_ts"))
              .filter((F.col("event_date") >= F.lit(start)) &
                      (F.col("event_date") <= F.lit(end))))

    return (window.join(F.broadcast(users), on="user_id", how="left")
            .withColumn("city", F.coalesce(F.col("city"), F.lit("UNKNOWN")))
            .withColumn("device_type",
                        F.coalesce(F.col("device_type"), F.lit("UNKNOWN")))
            # ── de-dup on the natural key to guarantee idempotency ──
            .dropDuplicates(["user_id", "session_id", "event_time",
                             "event_type"])
            .select("event_time", "user_id", "session_id", "event_type",
                    "channel", "premium_amount", "city", "device_type",
                    "signup_date"))


def main():
    args = parse_args()
    spark = (SparkSession.builder
             .appName("azki-backfill")
             .getOrCreate())

    events = (spark.read.option("header", True).schema(EVENTS_SCHEMA)
              .csv(args.events))
    users = (spark.read.option("header", True).schema(USERS_SCHEMA)
             .csv(args.users))

    enriched = enrich_window(events, users, args.start, args.end)

    count = enriched.count()
    print(f"[backfill] {args.start}..{args.end}: {count} rows computed")

    # Stage a single CSV; `azki backfill` loads it into ClickHouse over HTTP.
    (enriched.coalesce(1).write
     .option("header", True)
     .mode("overwrite")
     .csv(args.out))

    print(f"[backfill] staged {count} rows to {args.out}")
    spark.stop()


if __name__ == "__main__":
    main()
