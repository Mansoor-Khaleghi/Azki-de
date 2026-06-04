"""``python -m azki <command>`` — the single operational surface.

Replaces the old Makefile. Every command reads connection settings/credentials
from ``.env`` (see ``azki.config``); ClickHouse is driven over HTTP so the same
commands work from the host or inside a container.

Common flows:
    python -m azki demo            # up -> init -> seed -> produce -> reconcile -> verify
    python -m azki up              # start core stack (kafka, mysql, clickhouse)
    python -m azki init            # create dictionary, Kafka source, MVs, tables
    python -m azki seed            # generate + load synthetic order tables
    python -m azki produce         # stream user_events.csv into Kafka
    python -m azki verify          # row counts + sample aggregates
    python -m azki dq              # run the data-quality gate
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import orders, producer, quality
from .clickhouse import Client, render
from .compose import REPO_ROOT, check_data, compose
from .config import Settings, load_settings

# ── SQL applied by `init`, in order. Part 1 (dictionary -> Kafka source ->
#    enrichment MV -> aggregates) then Part 2 (order tables -> denorm MV). ──
INIT_SQL = [
    "clickhouse/part1/01-users-dictionary.sql",
    "clickhouse/part1/02-kafka-source.sql",
    "clickhouse/part1/03-events-enriched.sql",
    "clickhouse/part1/04-aggregates.sql",
    "clickhouse/part2/10-order-tables.sql",
    "clickhouse/part2/11-denormalized-purchases.sql",
]
ORDER_TABLES = ["third", "body", "medical", "fire", "financial"]
CONNECTORS = ["connect/mysql-users-source.json", "connect/clickhouse-events-sink.json"]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text()


def _apply_sql(client: Client, path: str, env: dict[str, str]) -> None:
    """Render ${VAR} placeholders from .env, then run the script over HTTP."""
    n = client.execute_script(render(_read(path), env))
    print(f"  applied {path} ({n} statements)")


def _expected_rows(events="data/user_events.csv") -> int:
    p = REPO_ROOT / events
    if not p.exists():
        return 20000
    with p.open() as fh:
        return max(sum(1 for _ in fh) - 1, 0)  # minus header


# ─────────────────────────── commands ───────────────────────────

def cmd_check_data(s, a):
    check_data()


def cmd_up(s, a):
    check_data()
    compose("up", "-d", "kafka", "mysql", "clickhouse")
    print("Waiting for ClickHouse to be ready...")
    if Client(s).wait_until_ready():
        print("ClickHouse ready.")
    else:
        print("WARNING: ClickHouse not ready in time.", file=sys.stderr)
    compose("ps")


def cmd_up_bonus(s, a):
    check_data()
    compose("up", "-d")


def cmd_orchestrate(s, a):
    compose("--profile", "orchestration", "up", "-d", "prefect")
    print("Prefect UI -> http://localhost:4200 (serving azki-monitoring every 5 min)")


def cmd_down(s, a):
    compose("down")


def cmd_clean(s, a):
    compose("down", "-v")


def cmd_logs(s, a):
    compose("logs", "-f", "clickhouse", "kafka", check=False)


def cmd_init(s, a):
    client = Client(s)
    env = s.render_env()
    print(">> applying ClickHouse schema (Part 1 + Part 2)")
    for path in INIT_SQL:
        _apply_sql(client, path, env)


def cmd_seed(s, a):
    out = REPO_ROOT / "data" / "orders"
    counts = orders.generate_orders(str(REPO_ROOT / a.events), str(out), a.seed)
    print(f"generated {counts['financial']} orders: "
          + ", ".join(f"{k}={v}" for k, v in counts.items()))
    client = Client(s)
    for t in ORDER_TABLES:
        csv_path = out / f"{t}_order.csv"
        print(f"loading {t}...")
        client.insert_csv(f"{s.ch_db}.{t}_order", str(csv_path))


def cmd_produce(s, a):
    _, _, failed = producer.stream(a.bootstrap, a.topic,
                                   str(REPO_ROOT / a.file), a.rate, a.limit)
    if failed:
        raise SystemExit(1)


def cmd_verify(s, a):
    client = Client(s)
    db = s.ch_db
    sections = [
        ("users_dict (from MySQL)",
         f"SELECT count() AS users FROM {db}.users_dict"),
        ("events_enriched (raw enriched layer)",
         f"SELECT count() AS rows, uniq(user_id) AS users, "
         f"min(event_time), max(event_time) FROM {db}.events_enriched"),
        ("enrichment coverage (should be ~0 UNKNOWN)",
         f"SELECT countIf(city='UNKNOWN') AS unmatched, count() AS total "
         f"FROM {db}.events_enriched"),
        ("aggregates by event_type (count / uniq users / avg premium)",
         f"SELECT event_type, sum(events_count) AS events, "
         f"sum(unique_users) AS approx_users, round(avg(premium_avg)) AS avg_premium "
         f"FROM {db}.events_agg_daily_v GROUP BY event_type ORDER BY events DESC"),
        ("top channels for purchases",
         f"SELECT channel, sum(events_count) AS purchases, "
         f"round(sum(premium_sum)) AS premium_sum FROM {db}.events_agg_daily_v "
         f"WHERE event_type='purchase' GROUP BY channel ORDER BY purchases DESC"),
        ("fact_purchases denormalized sample by product line",
         f"SELECT product_line, count() AS orders, round(avg(net_amount)) AS avg_net, "
         f"round(avg(installments),1) AS avg_installments FROM {db}.fact_purchases "
         f"GROUP BY product_line ORDER BY orders DESC"),
    ]
    print("================ Azki pipeline verification ================")
    for title, sql in sections:
        print(f"\n## {title}:")
        try:
            print(client.query(sql, fmt="PrettyCompact"))
        except Exception as e:  # noqa: BLE001
            print(f"(unavailable: {e})")
    print("\n============================================================")


def cmd_dq(s, a):
    expected = a.expected if a.expected is not None else _expected_rows()
    failures, _ = quality.run_checks(Client(s), str(REPO_ROOT / a.sql), expected)
    if failures:
        raise SystemExit(1)


def cmd_reconcile(s, a):
    _apply_sql(Client(s), "clickhouse/part2/14-denorm-reconcile.sql", s.render_env())
    n = Client(s).query(f"SELECT count() FROM {s.ch_db}.fact_purchases")
    print(f"fact_purchases now {n}")


def cmd_apply_opt(s, a):
    _apply_sql(Client(s), "clickhouse/part2/12-optimizations.sql", s.render_env())


def cmd_apply_gov(s, a):
    _apply_sql(Client(s), "clickhouse/part2/13-governance.sql", s.render_env())


def cmd_connect_register(s, a):
    """Register Debezium source + ClickHouse sink, injecting creds from .env."""
    import urllib.request
    env = s.render_env()
    for path in CONNECTORS:
        body = render(_read(path), env).encode()
        req = urllib.request.Request(
            "http://localhost:8083/connectors", data=body,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                print(f"registered {Path(path).name}: {resp.status}")
        except Exception as e:  # noqa: BLE001
            print(f"failed to register {Path(path).name}: {e}", file=sys.stderr)


def cmd_backfill(s, a):
    compose("--profile", "spark", "run", "--rm",
            "-e", f"CH_PASSWORD={s.ch_password}",
            "spark", "spark-submit",
            "--packages", "com.clickhouse:clickhouse-jdbc:0.6.3",
            "/opt/app/backfill_job.py", "--start", a.start, "--end", a.end)


def cmd_demo(s, a):
    cmd_up(s, a)
    cmd_init(s, a)
    cmd_seed(s, a)
    cmd_produce(s, a)
    print("Waiting for ClickHouse to consume the topic...")
    client = Client(s)
    expected = _expected_rows()
    import time
    for _ in range(30):
        n = int(client.query(f"SELECT count() FROM {s.ch_db}.events_enriched") or 0)
        if n >= expected:
            print(f"consumed {n} events")
            break
        time.sleep(2)
    cmd_reconcile(s, a)   # gap-fill purchases the streaming MV missed
    cmd_verify(s, a)


# ─────────────────────────── parser ───────────────────────────

def build_parser(settings: Settings | None = None) -> argparse.ArgumentParser:
    s = settings or load_settings()
    p = argparse.ArgumentParser(prog="azki", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    def add(name, func, help):
        sp = sub.add_parser(name, help=help)
        sp.set_defaults(func=func)
        return sp

    add("check-data", cmd_check_data, "fail fast if the dataset is missing")
    add("up", cmd_up, "start core stack (kafka, mysql, clickhouse)")
    add("up-bonus", cmd_up_bonus, "start full stack incl. Schema Registry, Connect, UI")
    add("orchestrate", cmd_orchestrate, "start Prefect server + UI + scheduled flow")
    add("down", cmd_down, "stop containers (keep volumes)")
    add("clean", cmd_clean, "stop and remove volumes (full reset)")
    add("logs", cmd_logs, "tail clickhouse + kafka logs")
    add("init", cmd_init, "create dictionary, Kafka source, MVs, tables")

    sp = add("seed", cmd_seed, "generate + load synthetic order tables (Part 2)")
    sp.add_argument("--events", default="data/user_events.csv")
    sp.add_argument("--seed", type=int, default=42)

    sp = add("produce", cmd_produce, "stream user_events.csv into Kafka")
    sp.add_argument("--bootstrap", default=s.kafka_bootstrap_host)
    sp.add_argument("--topic", default=s.kafka_topic)
    sp.add_argument("--file", default="data/user_events.csv")
    sp.add_argument("--rate", type=float, default=0.0)
    sp.add_argument("--limit", type=int, default=0)

    add("verify", cmd_verify, "show row counts + sample aggregates")

    sp = add("dq", cmd_dq, "run the data-quality gate")
    sp.add_argument("--expected", type=int, default=None,
                    help="expected source rows (default: count data/user_events.csv)")
    sp.add_argument("--sql", default="quality/dq_checks.sql")

    add("reconcile", cmd_reconcile, "gap-fill fact_purchases for late orders")
    add("apply-opt", cmd_apply_opt, "apply Part 2 performance optimizations")
    add("apply-gov", cmd_apply_gov, "apply Part 2 governance (roles/policies)")
    add("connect-register", cmd_connect_register, "register Kafka Connect connectors")

    sp = add("backfill", cmd_backfill, "run the Spark backfill for a date window")
    sp.add_argument("start", help="inclusive YYYY-MM-DD")
    sp.add_argument("end", help="inclusive YYYY-MM-DD")

    sp = add("demo", cmd_demo, "full happy path: up -> init -> seed -> produce -> verify")
    sp.add_argument("--events", default="data/user_events.csv")
    sp.add_argument("--seed", type=int, default=42)
    sp.add_argument("--bootstrap", default=s.kafka_bootstrap_host)
    sp.add_argument("--topic", default=s.kafka_topic)
    sp.add_argument("--file", default="data/user_events.csv")
    sp.add_argument("--rate", type=float, default=0.0)
    sp.add_argument("--limit", type=int, default=0)
    return p


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    args = build_parser(settings).parse_args(argv)
    args.func(settings, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
