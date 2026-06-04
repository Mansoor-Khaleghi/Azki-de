"""``python -m azki <command>`` — pipeline operations against the running stack.

Start the stack with ``docker compose up -d``; these commands then build the
schema and move data through it. All credentials come from .env.

    python -m azki demo      # init -> seed -> produce -> reconcile -> verify
    python -m azki init      # create dictionary, Kafka source, MVs, tables
    python -m azki seed      # generate + load synthetic order tables
    python -m azki produce   # stream user_events.csv into Kafka
    python -m azki verify    # row counts + sample aggregates
    python -m azki dq        # data-quality gate
"""
from __future__ import annotations

import argparse
import sys
import time

from . import orders, producer, quality
from .clickhouse import Client, render
from .compose import REPO_ROOT, check_data, compose
from .config import Settings, load_settings

INIT_SQL = [
    "clickhouse/part1/01-users-dictionary.sql",
    "clickhouse/part1/02-kafka-source.sql",
    "clickhouse/part1/03-events-enriched.sql",
    "clickhouse/part1/04-aggregates.sql",
    "clickhouse/part2/10-order-tables.sql",
    "clickhouse/part2/11-denormalized-purchases.sql",
]
ORDER_TABLES = ["third", "body", "medical", "fire", "financial"]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text()


def _apply_sql(client: Client, path: str, env: dict[str, str]) -> None:
    n = client.execute_script(render(_read(path), env))
    print(f"  applied {path} ({n} statements)")


def _expected_rows(events="data/user_events.csv") -> int:
    p = REPO_ROOT / events
    if not p.exists():
        return 20000
    with p.open() as fh:
        return max(sum(1 for _ in fh) - 1, 0)


# ─────────────────────────── commands ───────────────────────────

def cmd_init(s, a):
    client = Client(s)
    env = s.render_env()
    print(">> applying ClickHouse schema (Part 1 + Part 2)")
    for path in INIT_SQL:
        _apply_sql(client, path, env)


def cmd_seed(s, a):
    out = REPO_ROOT / "data" / "orders"
    counts = orders.generate_orders(str(REPO_ROOT / a.events), str(out), a.seed)
    print("generated orders: " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    client = Client(s)
    for t in ORDER_TABLES:
        print(f"loading {t}...")
        client.insert_csv(f"{s.ch_db}.{t}_order", str(out / f"{t}_order.csv"))


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
        except Exception as e:
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


def _register_connector(env: dict[str, str], path: str) -> None:
    import json
    import urllib.request
    spec = json.loads(render(_read(path), env))
    name = spec["name"]
    # PUT /connectors/{name}/config is create-or-update, so re-running is safe
    # (POST /connectors would 409 if the connector already exists).
    body = json.dumps(spec["config"]).encode()
    req = urllib.request.Request(
        f"http://localhost:8083/connectors/{name}/config", data=body,
        headers={"Content-Type": "application/json"}, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"registered {name}: {resp.status}")
    except Exception as e:
        print(f"failed to register {name}: {e}", file=sys.stderr)


def _wait_for_connect(plugins: list[str], attempts: int = 60,
                      delay: float = 3.0) -> bool:
    """Poll the Connect REST API until it's up and all ``plugins`` are installed.

    On a fresh `docker compose up`, the connect container reinstalls its plugins
    on boot (~1-2 min), so registering immediately would fail.
    """
    import json
    import urllib.request
    for _ in range(attempts):
        try:
            with urllib.request.urlopen(
                    "http://localhost:8083/connector-plugins", timeout=5) as resp:
                classes = {p.get("class") for p in json.loads(resp.read())}
            if all(p in classes for p in plugins):
                return True
            print("  waiting for Kafka Connect to install plugins...")
        except Exception:
            print("  waiting for Kafka Connect REST API (:8083)...")
        time.sleep(delay)
    return False


def cmd_connect_register(s, a):
    env = s.render_env()
    if not _wait_for_connect(["io.debezium.connector.mysql.MySqlConnector",
                              "com.clickhouse.kafka.connect.ClickHouseSinkConnector"]):
        print("Kafka Connect not ready (REST :8083 / connector plugins). Is the "
              "full stack up (`docker compose up -d`) and the connect container "
              "healthy?", file=sys.stderr)
        raise SystemExit(1)
    # Debezium MySQL source — users dimension as a CDC stream.
    _register_connector(env, "connect/mysql-users-source.json")
    # ClickHouse sink (+ DLQ) — the "pure sink" alternative to the Kafka engine.
    # It writes into a table it does not create itself.
    _apply_sql(Client(s), "connect/clickhouse-sink-target.sql", env)
    _register_connector(env, "connect/clickhouse-events-sink.json")


def cmd_backfill(s, a):
    import glob
    client = Client(s)
    # ReplacingMergeTree target — re-runs of the same window collapse on merge.
    _apply_sql(client, "spark/backfill_target.sql", s.render_env())
    # Spark computes + dedups the window and stages one CSV under spark/.
    compose("--profile", "spark", "run", "--rm",
            "spark", "/opt/spark/bin/spark-submit",
            "/opt/app/backfill_job.py", "--start", a.start, "--end", a.end)
    # Load the staged CSV into ClickHouse over the same HTTP path the rest of
    # the pipeline uses (decoupled from any Spark↔ClickHouse JDBC driver).
    parts = sorted(glob.glob(str(REPO_ROOT / "spark" / "_backfill_out" / "part-*.csv")))
    if not parts:
        print("no staged CSV produced by Spark", file=sys.stderr)
        raise SystemExit(1)
    for p in parts:
        client.insert_csv(f"{s.ch_db}.events_enriched_backfill", p)
    n = client.query(f"SELECT count() FROM {s.ch_db}.events_enriched_backfill")
    print(f"events_enriched_backfill now {n} rows "
          f"({len(parts)} staged file(s) loaded)")


def cmd_demo(s, a):
    check_data()
    client = Client(s)
    print("Waiting for ClickHouse...")
    if not client.wait_until_ready():
        print("ClickHouse not reachable — is `docker compose up -d` running?",
              file=sys.stderr)
        raise SystemExit(1)
    cmd_init(s, a)
    cmd_seed(s, a)
    cmd_produce(s, a)
    print("Waiting for ClickHouse to consume the topic...")
    expected = _expected_rows()
    for _ in range(30):
        n = int(client.query(f"SELECT count() FROM {s.ch_db}.events_enriched") or 0)
        if n >= expected:
            print(f"consumed {n} events")
            break
        time.sleep(2)
    cmd_reconcile(s, a)
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
    sp.add_argument("--expected", type=int, default=None)
    sp.add_argument("--sql", default="quality/dq_checks.sql")

    add("reconcile", cmd_reconcile, "gap-fill fact_purchases for late orders")
    add("apply-opt", cmd_apply_opt, "apply Part 2 performance optimizations")
    add("apply-gov", cmd_apply_gov, "apply Part 2 governance (roles/policies)")
    add("connect-register", cmd_connect_register,
        "register the Debezium source + ClickHouse sink connectors")

    sp = add("backfill", cmd_backfill, "run the Spark backfill for a date window")
    sp.add_argument("start", help="inclusive YYYY-MM-DD")
    sp.add_argument("end", help="inclusive YYYY-MM-DD")

    sp = add("demo", cmd_demo, "init -> seed -> produce -> reconcile -> verify")
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
