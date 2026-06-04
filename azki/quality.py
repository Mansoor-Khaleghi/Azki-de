"""Data-quality gate: run dq_checks.sql and exit non-zero on any FAIL."""
from __future__ import annotations

from .clickhouse import Client, split_statements
from .config import load_settings


def parse_check(out: str) -> tuple[str, str, str] | None:
    """Parse a tab-separated check result into (name, status, metric)."""
    if not out:
        return None
    cols = out.split("\t")
    name = cols[0] if cols else "?"
    status = cols[1] if len(cols) > 1 else "?"
    metric = cols[2] if len(cols) > 2 else ""
    return name, status, metric


def run_checks(client: Client, sql_path: str, expected: int) -> tuple[int, int]:
    """Run every check, print a report, return (failures, warnings)."""
    params = {"param_expected": str(expected), "default_format": "TabSeparated"}
    with open(sql_path) as fh:
        statements = split_statements(fh.read())

    failures = warns = 0
    print(f"\n{'CHECK':<32} {'STATUS':<6} METRIC")
    print("-" * 78)
    for stmt in statements:
        try:
            parsed = parse_check(client.query(stmt, params))
        except Exception as e:
            print(f"{'<query error>':<32} {'ERROR':<6} {e}")
            failures += 1
            continue
        if parsed is None:
            continue
        name, status, metric = parsed
        print(f"{name:<32} {status:<6} {metric}")
        if status == "FAIL":
            failures += 1
        elif status == "WARN":
            warns += 1

    print("-" * 78)
    print(f"Summary: {failures} failed, {warns} warnings\n")
    return failures, warns


def main(argv: list[str] | None = None) -> int:
    import argparse
    from dataclasses import replace
    s = load_settings()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=s.ch_host)
    ap.add_argument("--port", type=int, default=s.ch_port)
    ap.add_argument("--expected", type=int, default=20000)
    ap.add_argument("--sql", default="quality/dq_checks.sql")
    args = ap.parse_args(argv)

    client = Client(replace(s, ch_host=args.host, ch_port=args.port))
    failures, _ = run_checks(client, args.sql, args.expected)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
