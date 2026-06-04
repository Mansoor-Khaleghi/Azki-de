#!/usr/bin/env python3
"""Prefect flows that schedule and retry the pipeline via the azki CLI.

    python orchestration/flows.py monitoring     # reconcile + DQ gate, once
    python orchestration/flows.py ingest          # produce -> wait -> reconcile -> DQ
    python orchestration/flows.py backfill 2025-10-01 2025-10-07
    python orchestration/flows.py serve           # schedule monitoring every 5 min
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time  # noqa: E402

from prefect import flow, get_run_logger, task  # noqa: E402

from azki.clickhouse import Client  # noqa: E402
from azki.config import load_settings  # noqa: E402

SETTINGS = load_settings()
CLIENT = Client(SETTINGS)
AZKI = [sys.executable, "-m", "azki"]


def _run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


@task(retries=3, retry_delay_seconds=10)
def produce_events(limit: int = 0):
    log = get_run_logger()
    cmd = AZKI + ["produce"]
    if limit:
        cmd += ["--limit", str(limit)]
    r = _run(cmd)
    log.info(r.stdout.strip().splitlines()[-1] if r.stdout else "produced")
    if r.returncode != 0:
        raise RuntimeError(f"producer failed: {r.stderr[-500:]}")
    return "ok"


@task
def wait_for_consumption(expected: int = 20000, timeout_s: int = 120):
    log = get_run_logger()
    deadline = time.monotonic() + timeout_s
    n = 0
    while time.monotonic() < deadline:
        n = int(CLIENT.query(
            f"SELECT count() FROM {SETTINGS.ch_db}.events_enriched") or 0)
        if n >= expected:
            log.info(f"consumed {n} rows")
            return n
        time.sleep(3)
    raise TimeoutError(f"only consumed {n}/{expected} within {timeout_s}s")


@task(retries=2, retry_delay_seconds=5)
def reconcile_denorm():
    log = get_run_logger()
    r = _run(AZKI + ["reconcile"])
    if r.returncode != 0:
        raise RuntimeError(f"reconcile failed: {r.stderr[-500:]}")
    log.info(r.stdout.strip())
    return r.stdout.strip()


@task
def run_dq_gate(expected: int = 20000):
    log = get_run_logger()
    r = _run(AZKI + ["dq", "--expected", str(expected)])
    log.info("\n" + r.stdout)
    if r.returncode != 0:
        raise RuntimeError("DATA QUALITY GATE FAILED")
    return "passed"


@task(retries=1)
def trigger_backfill(start: str, end: str):
    log = get_run_logger()
    r = _run([sys.executable, "spark/validate_backfill.py",
              "--start", start, "--end", end])
    log.info(r.stdout[-800:])
    if r.returncode != 0:
        raise RuntimeError(f"backfill failed: {r.stderr[-500:]}")
    return "ok"


@flow(name="azki-monitoring")
def monitoring_flow(expected: int = 20000):
    reconcile_denorm()
    run_dq_gate(expected)


@flow(name="azki-ingest")
def ingest_flow(expected: int = 20000):
    produce_events()
    wait_for_consumption(expected)
    reconcile_denorm()
    run_dq_gate(expected)


@flow(name="azki-backfill")
def backfill_flow(start: str, end: str):
    trigger_backfill(start, end)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "monitoring"
    if cmd == "monitoring":
        monitoring_flow()
    elif cmd == "ingest":
        ingest_flow()
    elif cmd == "backfill":
        backfill_flow(sys.argv[2], sys.argv[3])
    elif cmd == "serve":
        monitoring_flow.serve(name="azki-dq-monitor", interval=300)
    else:
        print(__doc__)
        sys.exit(1)
