"""ClickHouse HTTP client and SQL helpers."""
from __future__ import annotations

import re
import time
import urllib.parse
import urllib.request

from .config import Settings

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def split_statements(sql_text: str) -> list[str]:
    """Strip ``--`` comments (full-line and trailing) and split on ``;``.

    Assumes ``--`` never appears inside a string literal, which holds for the
    SQL in this repo.
    """
    cleaned = []
    for ln in sql_text.splitlines():
        idx = ln.find("--")
        cleaned.append(ln[:idx] if idx != -1 else ln)
    return [s.strip() for s in "\n".join(cleaned).split(";") if s.strip()]


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
        # Always POST: GET over the ClickHouse HTTP interface is readonly, so
        # DDL/DML must go via POST. The statement travels in the request body;
        # when uploading data (INSERT ... FORMAT ...), the body is that data and
        # the statement moves to the `query` URL parameter instead.
        q = dict(params or {})
        if fmt:
            q["default_format"] = fmt
        if body is None:
            data = sql.encode()
        else:
            q["query"] = sql
            data = body
        url = self.base + "?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, data=data, headers=self.auth)
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
