#!/usr/bin/env python3
"""
Azki DE Task — data quality runner (Part 3).

Executes the checks in dq_checks.sql against ClickHouse over HTTP and prints
a report. Exits non-zero if any check FAILs (WARN does not fail the gate), so
it can drop straight into Airflow / CI as a quality gate.

Usage:
    python run_quality_checks.py --host localhost --port 8123 \
        --user azki --password azkipw --expected 20000
"""
import argparse
import sys
import urllib.parse
import urllib.request


def run_query(base, auth, sql, params):
    q = dict(params)
    q["query"] = sql
    url = base + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers=auth)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode().strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8123)
    ap.add_argument("--user", default="azki")
    ap.add_argument("--password", default="azkipw")
    ap.add_argument("--expected", type=int, default=20000,
                    help="expected source row count for parity check")
    ap.add_argument("--sql", default="quality/dq_checks.sql")
    args = ap.parse_args()

    base = f"http://{args.host}:{args.port}/"
    auth = {"X-ClickHouse-User": args.user, "X-ClickHouse-Key": args.password}
    params = {
        "param_expected": str(args.expected),
        "default_format": "TabSeparated",
    }

    # Strip full-line SQL comments, then split into statements.
    with open(args.sql) as fh:
        sql_lines = [ln for ln in fh.read().splitlines()
                     if not ln.lstrip().startswith("--")]
    statements = [s.strip() for s in "\n".join(sql_lines).split(";")
                  if s.strip()]

    failures, warns = 0, 0
    print(f"\n{'CHECK':<32} {'STATUS':<6} METRIC")
    print("-" * 78)
    for stmt in statements:
        try:
            out = run_query(base, auth, stmt, params)
        except Exception as e:
            print(f"{'<query error>':<32} {'ERROR':<6} {e}")
            failures += 1
            continue
        if not out:
            continue
        cols = out.split("\t")
        name = cols[0] if cols else "?"
        status = cols[1] if len(cols) > 1 else "?"
        metric = cols[2] if len(cols) > 2 else ""
        print(f"{name:<32} {status:<6} {metric}")
        if status == "FAIL":
            failures += 1
        elif status == "WARN":
            warns += 1

    print("-" * 78)
    print(f"Summary: {failures} failed, {warns} warnings\n")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
