# Orchestration (Prefect)

A thin **Prefect** layer that turns the pipeline's manual steps into operable,
schedulable, retrying flows. Tasks drive the canonical `azki` CLI / package
(`azki produce`, `azki reconcile`, `azki dq`, `validate_backfill.py`) —
**no logic is duplicated**, so the orchestrator and the CLI stay in sync.

The flows are **connection-agnostic** (ClickHouse over HTTP, Kafka bootstrap —
both from `.env`/env via `CH_HOST` / `KAFKA_BOOTSTRAP`), so the exact same code
runs two ways:

- **In compose** (default, zero local setup): `azki orchestrate` starts the
  `prefect` service — server + UI + the scheduled `monitoring` flow — talking to
  the stack over the compose network. UI at http://localhost:4200.
- **On the host** (dev): `pip install -r requirements.txt` then
  `python orchestration/flows.py ...` against `localhost`.

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

**In compose (recommended):**

```bash
python -m azki orchestrate   # docker compose --profile orchestration up -d prefect
# -> Prefect server + UI at http://localhost:4200, serving the scheduled
#    azki-monitoring flow (reconcile + DQ every 5 min).

# trigger a run on demand inside the container:
docker exec azki-prefect python orchestration/flows.py monitoring
```

**On the host (dev):**

```bash
pip install -r requirements.txt                          # prefect>=3 (+ rest)
python orchestration/flows.py monitoring                 # reconcile + DQ
python orchestration/flows.py ingest                     # full cycle
python orchestration/flows.py backfill 2025-10-01 2025-10-07
python orchestration/flows.py serve                      # scheduled, every 5 min
```

`serve` registers a scheduled deployment and runs it on an interval — open the
Prefect UI (http://localhost:4200) to watch runs, retries, and logs.

## Why Prefect (vs cron / Airflow)

- Runs flows **locally with zero infra** for dev (`python flows.py`), and scales
  to a server+worker deployment for prod — same code.
- First-class **retries, logging, and observability** per task.
- Lighter to stand up than Airflow for this size of pipeline; the brief's Part 3
  (monitoring) maps cleanly onto a scheduled `monitoring_flow`.
