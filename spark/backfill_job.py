#!/usr/bin/env python3
"""
Azki DE Task — Spark backfill job (Part 3 bonus).

Handles missing-data / backfill scenarios: re-process a date range of raw
events from cold storage (here: the original CSV; in production: S3/HDFS
parquet partitioned by day), re-enrich against the users table, and load
into ClickHouse IDEMPOTENTLY so reruns don't double-count.

Idempotency strategy:
  * Target is a ReplacingMergeTree-style table keyed on the natural event key
    (user_id, session_id, event_time, event_type); re-inserting the same key
    is collapsed on merge.
  * We also delete the target date range before insert (partition-scoped
    overwrite) when --overwrite is set, for hard re-statements.

Run:
  spark-submit --packages com.clickhouse:clickhouse-jdbc:0.6.3 \
    backfill_job.py --start 2025-10-01 --end 2025-10-07 \
    --events /opt/data/user_events.csv --users /opt/data/users.csv \
    --ch-url jdbc:clickhouse://clickhouse:8123/azki
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

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
    ap.add_argument("--ch-url", default="jdbc:clickhouse://clickhouse:8123/azki")
    ap.add_argument("--ch-user", default=os.environ.get("CH_USER", "azki"))
    ap.add_argument("--ch-password", default=os.environ.get("CH_PASSWORD", ""))
    ap.add_argument("--target", default="events_enriched_backfill")
    ap.add_argument("--overwrite", action="store_true",
                    help="hard-restate: clear target date range first")
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
    print(f"[backfill] {args.start}..{args.end}: {count} rows to load")

    if args.overwrite:
        # Partition-scoped hard restatement via ClickHouse ALTER DELETE.
        # (Issued through JDBC before the insert.)
        _clear_range(args)

    # ── Idempotent load into ClickHouse ──
    (enriched.write
     .format("jdbc")
     .option("url", args.ch_url)
     .option("dbtable", args.target)
     .option("user", args.ch_user)
     .option("password", args.ch_password)
     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
     .option("batchsize", 100000)
     .mode("append")
     .save())

    print(f"[backfill] loaded {count} rows into {args.target}")
    spark.stop()


def _clear_range(args):
    """Delete the target date range first (hard restatement)."""
    import jaydebeapi  # optional; documented in spark/README
    conn = jaydebeapi.connect(
        "com.clickhouse.jdbc.ClickHouseDriver", args.ch_url,
        [args.ch_user, args.ch_password])
    cur = conn.cursor()
    cur.execute(
        f"ALTER TABLE {args.target} DELETE WHERE "
        f"toDate(event_time) BETWEEN '{args.start}' AND '{args.end}'")
    cur.close()
    conn.close()
    print(f"[backfill] cleared {args.start}..{args.end} in {args.target}")


if __name__ == "__main__":
    main()
