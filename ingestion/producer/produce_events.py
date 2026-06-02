#!/usr/bin/env python3
"""
Azki DE Task — Kafka producer.

Streams user_events.csv into the `user_events` Kafka topic as JSONEachRow,
keyed by user_id so that all events for a user land on the same partition
(preserving per-user ordering). ClickHouse's Kafka table engine consumes
this topic downstream.

Usage:
    python produce_events.py --bootstrap localhost:29092 \
        --topic user_events --file ../../data/user_events.csv [--rate 0]

    --rate 0     => fire as fast as possible (bulk replay; default)
    --rate 500   => throttle to ~500 msgs/sec (simulate a live stream)
"""
import argparse
import csv
import json
import sys
import time

from confluent_kafka import Producer


def parse_args():
    p = argparse.ArgumentParser(description="Stream user_events.csv to Kafka")
    p.add_argument("--bootstrap", default="localhost:29092",
                   help="Kafka bootstrap servers")
    p.add_argument("--topic", default="user_events", help="target topic")
    p.add_argument("--file", default="data/user_events.csv", help="events CSV")
    p.add_argument("--rate", type=float, default=0.0,
                   help="max msgs/sec (0 = unlimited)")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after N rows (0 = all); handy for smoke tests")
    return p.parse_args()


def main():
    args = parse_args()
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,   # exactly-once semantics into the topic
    })

    delivered = 0
    failed = 0

    def on_delivery(err, msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            if failed <= 10:
                print(f"  delivery failed: {err}", file=sys.stderr)
        else:
            delivered += 1

    sent = 0
    start = time.monotonic()
    with open(args.file, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Cast to the schema ClickHouse expects. premium_amount kept as
            # numeric; empty -> None so the consumer can decide policy.
            premium = row.get("premium_amount", "").strip()
            event = {
                "event_time": row["event_time"],
                "user_id": int(row["user_id"]),
                "session_id": row["session_id"],
                "event_type": row["event_type"],
                "channel": row["channel"],
                "premium_amount": float(premium) if premium else None,
            }
            producer.produce(
                topic=args.topic,
                key=str(event["user_id"]),
                value=json.dumps(event),
                on_delivery=on_delivery,
            )
            sent += 1

            # Serve delivery callbacks without blocking.
            producer.poll(0)

            if args.rate > 0:
                expected = sent / args.rate
                drift = expected - (time.monotonic() - start)
                if drift > 0:
                    time.sleep(drift)

            if args.limit and sent >= args.limit:
                break

            if sent % 2000 == 0:
                print(f"  queued {sent} rows...")

    print(f"Flushing ({sent} rows queued)...")
    producer.flush(60)
    elapsed = time.monotonic() - start
    print(f"Done. sent={sent} delivered={delivered} failed={failed} "
          f"in {elapsed:.1f}s ({sent / max(elapsed, 1e-9):.0f} msg/s)")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
