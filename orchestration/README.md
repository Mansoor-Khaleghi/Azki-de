# Orchestration (Prefect)

A thin **Prefect** layer that turns the pipeline's manual steps into operable,
schedulable, retrying flows. Tasks shell out to the canonical scripts/SQL
(`produce_events.py`, `14-denorm-reconcile.sql`, `run_quality_checks.py`,
`validate_backfill.py`) — **no logic is duplicated**, so the orchestrator and
the Makefile stay in sync.

Run it on the **host / control plane** (it drives the stack via `docker exec`
and `localhost:29092`), not inside a container — avoids the docker-in-docker
anti-pattern. In production the same flows deploy to a Prefect worker that has
network access to the cluster.

## Flows

| Flow | Tasks | Purpose |
|---|---|---|
| `azki-ingest` | produce → wait_for_consumption → reconcile → DQ gate | one full ingest cycle |
| `azki-monitoring` | reconcile → DQ gate | scheduled health loop (every 5 min) |
| `azki-backfill` | trigger_backfill(start, end) | windowed Spark backfill |

Resilience: `produce_events` retries 3× (transient broker errors),
`reconcile_denorm` 2×, `trigger_backfill` 1×. The DQ gate raises on any
`FAIL`, which fails the flow run (and would alert in a real deployment).

## Run

```bash
pip install -r orchestration/requirements.txt   # prefect>=3

# run a flow once, locally (no server required):
python orchestration/flows.py monitoring                 # reconcile + DQ
python orchestration/flows.py ingest                     # full cycle
python orchestration/flows.py backfill 2025-10-01 2025-10-07

# scheduled: DQ + reconcile every 5 minutes (self-hosted, ephemeral server):
python orchestration/flows.py serve
```

`serve` registers a scheduled deployment and runs it on an interval — open the
Prefect UI (`prefect server start`, default http://localhost:4200) to watch
runs, retries, and logs.

## Why Prefect (vs cron / Airflow)

- Runs flows **locally with zero infra** for dev (`python flows.py`), and scales
  to a server+worker deployment for prod — same code.
- First-class **retries, logging, and observability** per task.
- Lighter to stand up than Airflow for this size of pipeline; the brief's Part 3
  (monitoring) maps cleanly onto a scheduled `monitoring_flow`.
