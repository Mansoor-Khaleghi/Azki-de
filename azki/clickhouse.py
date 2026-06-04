"""ClickHouse HTTP client and SQL helpers."""
from __future__ import annotations

import re
import time
import urllib.parse
import urllib.request

from .config import Settings

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def split_statements(sql_text: str) -> list[str]:
    """Drop full-line ``--`` comments and split a script on ``;``."""
    lines = [ln for ln in sql_text.splitlines() if not ln.lstrip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def render(text: str, env: dict[str, str]) -> str:
    """Substitute ``${VAR}`` from ``env``; leave unknown placeholders intact."""
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
