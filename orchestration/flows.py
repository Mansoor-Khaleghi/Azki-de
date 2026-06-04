#!/usr/bin/env python3
"""
Azki DE Task — Prefect orchestration layer.

Wraps the pipeline's building blocks (producer, reconciliation, DQ gate, Spark
backfill) as Prefect tasks/flows with retries and scheduling, so the whole
thing is operable instead of a pile of manual commands. Every task drives the
canonical `azki` CLI / package — single source of truth, no logic duplicated
and no credentials in this file (they come from .env via azki.config).

Run standalone (no server needed):
    python orchestration/flows.py monitoring     # reconcile + DQ gate, once
    python orchestration/flows.py ingest          # produce -> wait -> reconcile -> DQ
    python orchestration/flows.py backfill 2025-10-01 2025-10-07

Schedule (needs a Prefect server/worker — see docker-compose `prefect` profile):
    python orchestration/flows.py serve           # DQ every 5 min
"""
import os
import subprocess
import sys
import time

# Make the repo-root `azki` package importable whether run from the host or
# from inside the compose `prefect` container (cwd=/app).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger  # noqa: E402

from azki.clickhouse import Client  # noqa: E402
from azki.config import load_settings  # noqa: E402

SETTINGS = load_settings()
CLIENT = Client(SETTINGS)
AZKI = [sys.executable, "-m", "azki"]


def _run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


@task(retries=3, retry_delay_seconds=10)
def produce_events(limit: int = 0):
    """Stream user_events.csv into Kafka (retried on transient broker errors)."""
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
    """Block until ClickHouse has consumed `expected` rows (or timeout)."""
    log = get_run_logger()
    deadline = time.monotonic() + timeout_s
    n = 0
    while time.monotonic() < deadline:
        n = int(CLIENT.query(f"SELECT count() FROM {SETTINGS.ch_db}.events_enriched") or 0)
        if n >= expected:
            log.info(f"consumed {n} rows")
            return n
        time.sleep(3)
    raise TimeoutError(f"only consumed {n}/{expected} within {timeout_s}s")


@task(retries=2, retry_delay_seconds=5)
def reconcile_denorm():
    """Idempotent gap-fill of fact_purchases for late-arriving orders."""
    log = get_run_logger()
    r = _run(AZKI + ["reconcile"])
    if r.returncode != 0:
        raise RuntimeError(f"reconcile failed: {r.stderr[-500:]}")
    log.info(r.stdout.strip())
    return r.stdout.strip()


@task
def run_dq_gate(expected: int = 20000):
    """Run the data-quality gate; FAIL (non-zero exit) aborts the flow."""
    log = get_run_logger()
    r = _run(AZKI + ["dq", "--expected", str(expected)])
    log.info("\n" + r.stdout)
    if r.returncode != 0:
        raise RuntimeError("DATA QUALITY GATE FAILED")
    return "passed"


@task(retries=1)
def trigger_backfill(start: str, end: str):
    """Kick the Spark backfill for a date window (validation harness here)."""
    log = get_run_logger()
    r = _run([sys.executable, "spark/validate_backfill.py", "--start", start, "--end", end])
    log.info(r.stdout[-800:])
    if r.returncode != 0:
        raise RuntimeError(f"backfill failed: {r.stderr[-500:]}")
    return "ok"


# ─────────────────────────── flows ───────────────────────────

@flow(name="azki-monitoring")
def monitoring_flow(expected: int = 20000):
    """Scheduled health loop: reconcile late orders, then gate data quality."""
    reconcile_denorm()
    run_dq_gate(expected)


@flow(name="azki-ingest")
def ingest_flow(expected: int = 20000):
    """One full ingest cycle: produce -> wait -> reconcile -> DQ gate."""
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
        # register a scheduled deployment: DQ + reconcile every 5 minutes
        monitoring_flow.serve(name="azki-dq-monitor", interval=300)
    else:
        print(__doc__)
        sys.exit(1)
