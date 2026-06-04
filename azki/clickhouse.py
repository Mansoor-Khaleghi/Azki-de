"""Tiny ClickHouse HTTP client + SQL helpers (stdlib only).

We talk to ClickHouse over its HTTP interface rather than ``docker exec
clickhouse-client`` so every command works identically from the host, from CI,
or from inside another container — and so credentials come from settings, never
the command line.
"""
from __future__ import annotations

import re
import time
import urllib.parse
import urllib.request

from .config import Settings

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def split_statements(sql_text: str) -> list[str]:
    """Strip full-line ``--`` comments and split a script into statements.

    Used both to execute ``.sql`` files statement-by-statement over HTTP and by
    the data-quality runner. Our SQL has no semicolons inside string literals,
    so a plain split on ``;`` is safe.
    """
    lines = [ln for ln in sql_text.splitlines() if not ln.lstrip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def render(text: str, env: dict[str, str]) -> str:
    """Substitute ``${VAR}`` placeholders from ``env`` (leave unknowns intact).

    This is how secrets stay out of the repo: SQL/connector files ship with
    ``${MYSQL_PASSWORD}`` etc. and are filled at apply time from ``.env``.
    """
    return _PLACEHOLDER.sub(lambda m: env.get(m.group(1), m.group(0)), text)


class Client:
    def __init__(self, settings: Settings, timeout: int = 60):
        self.settings = settings
        self.base = settings.ch_http_url
        self.timeout = timeout
        self.auth = {
            "X-ClickHouse-User": settings.ch_user,
            "X-ClickHouse-Key": settings.ch_password,
        }

    def query(self, sql: str, params: dict | None = None,
              fmt: str | None = None, body: bytes | None = None) -> str:
        q = dict(params or {})
        q["query"] = sql
        if fmt:
            q["default_format"] = fmt
        url = self.base + "?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, data=body, headers=self.auth)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.read().decode().strip()

    def execute_script(self, sql_text: str, params: dict | None = None) -> int:
        """Run every statement in a script; returns the count executed."""
        statements = split_statements(sql_text)
        for stmt in statements:
            self.query(stmt, params)
        return len(statements)

    def insert_csv(self, table: str, csv_path: str) -> None:
        with open(csv_path, "rb") as fh:
            self.query(f"INSERT INTO {table} FORMAT CSVWithNames", body=fh.read())

    def wait_until_ready(self, attempts: int = 30, delay: float = 2.0) -> bool:
        for _ in range(attempts):
            try:
                if self.query("SELECT 1") == "1":
                    return True
            except Exception:
                pass
            time.sleep(delay)
        return False
