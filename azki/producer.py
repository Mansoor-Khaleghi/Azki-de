"""Kafka producer (Part 1).

Streams user_events.csv into the ``user_events`` topic as JSONEachRow, keyed by
user_id so all events for a user land on the same partition (preserving
per-user ordering). ClickHouse's Kafka table engine consumes this downstream.

``confluent_kafka`` is imported lazily inside ``stream()`` so the pure
``build_event`` transform stays importable (and unit-testable) without the
native librdkafka dependency.
"""
from __future__ import annotations

import csv
import json
import sys
import time


def build_event(row: dict) -> dict:
    """Cast one CSV row to the JSON schema ClickHouse expects.

    Empty ``premium_amount`` -> ``None`` so the consumer decides policy rather
    than silently coercing missing money to 0.
    """
    premium = (row.get("premium_amount") or "").strip()
    return {
        "event_time": row["event_time"],
        "user_id": int(row["user_id"]),
        "session_id": row["session_id"],
        "event_type": row["event_type"],
        "channel": row["channel"],
        "premium_amount": float(premium) if premium else None,
    }


def stream(bootstrap: str, topic: str, file: str,
           rate: float = 0.0, limit: int = 0) -> tuple[int, int, int]:
    """Produce events to Kafka. Returns (sent, delivered, failed)."""
    from confluent_kafka import Producer  # lazy: keeps build_event dep-free

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,   # exactly-once semantics into the topic
    })

    delivered = failed = sent = 0

    def on_delivery(err, msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            if failed <= 10:
                print(f"  delivery failed: {err}", file=sys.stderr)
        else:
            delivered += 1

    start = time.monotonic()
    with open(file, newline="") as fh:
        for row in csv.DictReader(fh):
            event = build_event(row)
            producer.produce(topic=topic, key=str(event["user_id"]),
                             value=json.dumps(event), on_delivery=on_delivery)
            sent += 1
            producer.poll(0)

            if rate > 0:
                drift = sent / rate - (time.monotonic() - start)
                if drift > 0:
                    time.sleep(drift)
            if limit and sent >= limit:
                break
            if sent % 2000 == 0:
                print(f"  queued {sent} rows...")

    print(f"Flushing ({sent} rows queued)...")
    producer.flush(60)
    elapsed = time.monotonic() - start
    print(f"Done. sent={sent} delivered={delivered} failed={failed} "
          f"in {elapsed:.1f}s ({sent / max(elapsed, 1e-9):.0f} msg/s)")
    return sent, delivered, failed


def main(argv: list[str] | None = None) -> int:
    import argparse
    from .config import load_settings
    s = load_settings()
    ap = argparse.ArgumentParser(description="Stream user_events.csv to Kafka")
    ap.add_argument("--bootstrap", default=s.kafka_bootstrap_host)
    ap.add_argument("--topic", default=s.kafka_topic)
    ap.add_argument("--file", default="data/user_events.csv")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="max msgs/sec (0 = unlimited)")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after N rows (0 = all); handy for smoke tests")
    args = ap.parse_args(argv)
    _, _, failed = stream(args.bootstrap, args.topic, args.file,
                          args.rate, args.limit)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
