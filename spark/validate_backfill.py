#!/usr/bin/env python3
"""
Local validation of the Spark backfill transform (Part 3 bonus).

Runs `enrich_window` from backfill_job.py against the real CSVs for a date
window, WITHOUT the ClickHouse JDBC sink, and prints row counts + a sample.
Proves the enrich/dedup logic on a local SparkSession.

    python spark/validate_backfill.py --start 2025-10-01 --end 2025-10-07
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import SparkSession  # noqa: E402

from backfill_job import (EVENTS_SCHEMA, USERS_SCHEMA, enrich_window)  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-10-01")
    ap.add_argument("--end", default="2025-10-07")
    ap.add_argument("--events", default="data/user_events.csv")
    ap.add_argument("--users", default="data/users.csv")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("azki-backfill-validate")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    events = (spark.read.option("header", True).schema(EVENTS_SCHEMA)
              .csv(args.events))
    users = (spark.read.option("header", True).schema(USERS_SCHEMA)
             .csv(args.users))

    total_events = events.count()
    enriched = enrich_window(events, users, args.start, args.end)
    out = enriched.count()
    unmatched = enriched.filter("city = 'UNKNOWN'").count()

    print(f"\n[validate] total source events     : {total_events}")
    print(f"[validate] window {args.start}..{args.end}")
    print(f"[validate] enriched rows in window : {out}")
    print(f"[validate] unmatched users (UNKNOWN): {unmatched}")
    print("[validate] sample:")
    enriched.orderBy("event_time").show(5, truncate=False)

    # idempotency: re-running the transform yields the identical count
    out2 = enrich_window(events, users, args.start, args.end).count()
    assert out == out2, "non-deterministic output!"
    print(f"[validate] idempotency check        : PASS ({out} == {out2})\n")

    spark.stop()


if __name__ == "__main__":
    main()
