"""Stream user_events.csv into Kafka as JSONEachRow, keyed by user_id.

Two interchangeable backends, picked automatically:

* **confluent-kafka** — the native client, used when the package is installed
  (``pip install confluent-kafka``). Highest throughput, idempotent producer.
* **docker fallback** — pipes the same keyed ``JSONEachRow`` lines into the
  running Kafka container's ``kafka-console-producer.sh``. Needs nothing on the
  host but a working ``docker``, so the demo runs from zero with no pip install.

Both write byte-for-byte identical messages to the topic, so ClickHouse's Kafka
engine consumes them the same way.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time

# Container + in-container bootstrap used by the docker fallback. The compose
# file pins the broker's name to ``azki-kafka`` and exposes PLAINTEXT on 9092.
KAFKA_CONTAINER = os.environ.get("KAFKA_CONTAINER", "azki-kafka")
KAFKA_CONTAINER_BOOTSTRAP = os.environ.get("KAFKA_CONTAINER_BOOTSTRAP", "localhost:9092")


def build_event(row: dict) -> dict:
    """Cast a CSV row to the event schema; empty premium_amount -> None."""
    premium = (row.get("premium_amount") or "").strip()
    return {
        "event_time": row["event_time"],
        "user_id": int(row["user_id"]),
        "session_id": row["session_id"],
        "event_type": row["event_type"],
        "channel": row["channel"],
        "premium_amount": float(premium) if premium else None,
    }


def _iter_events(file: str, limit: int):
    """Yield (key, json_value) pairs from the CSV, capped at ``limit``."""
    with open(file, newline="") as fh:
        for i, row in enumerate(csv.DictReader(fh)):
            if limit and i >= limit:
                break
            event = build_event(row)
            yield str(event["user_id"]), json.dumps(event)


def stream(bootstrap: str, topic: str, file: str,
           rate: float = 0.0, limit: int = 0) -> tuple[int, int, int]:
    """Produce events to Kafka; return (sent, delivered, failed).

    Uses confluent-kafka if importable, otherwise the docker console-producer.
    """
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        print("confluent-kafka not installed — producing via the Kafka "
              f"container ({KAFKA_CONTAINER}).")
        return _stream_docker(topic, file, rate, limit)
    return _stream_confluent(bootstrap, topic, file, rate, limit)


def _stream_confluent(bootstrap: str, topic: str, file: str,
                      rate: float, limit: int) -> tuple[int, int, int]:
    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
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
    for key, value in _iter_events(file, limit):
        producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
        sent += 1
        producer.poll(0)
        if rate > 0:
            drift = sent / rate - (time.monotonic() - start)
            if drift > 0:
                time.sleep(drift)
        if sent % 2000 == 0:
            print(f"  queued {sent} rows...")

    print(f"Flushing ({sent} rows queued)...")
    producer.flush(60)
    elapsed = time.monotonic() - start
    print(f"Done. sent={sent} delivered={delivered} failed={failed} "
          f"in {elapsed:.1f}s ({sent / max(elapsed, 1e-9):.0f} msg/s)")
    return sent, delivered, failed


def _stream_docker(topic: str, file: str,
                   rate: float, limit: int) -> tuple[int, int, int]:
    """Pipe keyed JSONEachRow lines into the broker's console producer.

    The console producer reads ``<user_id>\\t<json>`` lines from stdin; we set
    ``parse.key=true`` so each message keeps the same partitioning key as the
    native client. ``acks=all`` mirrors the idempotent settings above.
    """
    cmd = [
        "docker", "exec", "-i", KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", KAFKA_CONTAINER_BOOTSTRAP,
        "--topic", topic,
        "--producer-property", "acks=all",
        "--producer-property", "compression.type=lz4",
        "--property", "parse.key=true",
        "--property", "key.separator=\t",
    ]
    start = time.monotonic()
    sent = 0
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
    except FileNotFoundError:
        print("ERROR: `docker` not found and confluent-kafka is not installed. "
              "Install confluent-kafka (pip install confluent-kafka) or run with "
              "docker available.", file=sys.stderr)
        return 0, 0, 1

    assert proc.stdin is not None
    try:
        for key, value in _iter_events(file, limit):
            proc.stdin.write(f"{key}\t{value}\n")
            sent += 1
            if rate > 0:
                proc.stdin.flush()
                drift = sent / rate - (time.monotonic() - start)
                if drift > 0:
                    time.sleep(drift)
            if sent % 2000 == 0:
                print(f"  queued {sent} rows...")
        proc.stdin.close()
    except BrokenPipeError:
        pass

    print(f"Flushing ({sent} rows queued)...")
    rc = proc.wait(timeout=120)
    elapsed = time.monotonic() - start
    failed = 0 if rc == 0 else sent
    delivered = sent if rc == 0 else 0
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
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)
    _, _, failed = stream(args.bootstrap, args.topic, args.file,
                          args.rate, args.limit)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
