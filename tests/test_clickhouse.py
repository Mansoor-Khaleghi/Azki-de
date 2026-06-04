"""ClickHouse SQL helpers + HTTP client (network mocked)."""
from unittest import mock

from azki.clickhouse import Client, render, split_statements
from azki.config import load_settings


def test_split_statements_strips_comments_and_splits():
    sql = """
    -- a leading comment
    CREATE TABLE a (x Int);
    -- another
    INSERT INTO a VALUES (1);
    """
    stmts = split_statements(sql)
    assert stmts == ["CREATE TABLE a (x Int)", "INSERT INTO a VALUES (1)"]


def test_split_statements_drops_empty_trailing():
    assert split_statements("SELECT 1;\n\n;  ") == ["SELECT 1"]


def test_render_substitutes_known_vars_and_keeps_unknown():
    text = "user '${MYSQL_USER}' pw '${MYSQL_PASSWORD}' keep '${NOPE}'"
    out = render(text, {"MYSQL_USER": "azki", "MYSQL_PASSWORD": "s3cr3t"})
    assert out == "user 'azki' pw 's3cr3t' keep '${NOPE}'"


def test_render_fills_dictionary_sql_from_repo(tmp_path):
    # the real dictionary SQL must no longer contain a literal password
    from azki.config import REPO_ROOT
    raw = (REPO_ROOT / "clickhouse/part1/01-users-dictionary.sql").read_text()
    assert "${MYSQL_PASSWORD}" in raw and "azkipw" not in raw
    filled = render(raw, {"MYSQL_USER": "u", "MYSQL_PASSWORD": "pw",
                          "MYSQL_DATABASE": "azki"})
    assert "password 'pw'" in filled


def test_client_query_builds_authenticated_url(tmp_path):
    env = tmp_path / ".env"
    env.write_text("CH_HOST=localhost\nCH_PORT=8123\n"
                   "CLICKHOUSE_USER=u\nCLICKHOUSE_PASSWORD=pw\n")
    client = Client(load_settings(env))

    captured = {}

    class FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"42\n"

    def fake_urlopen(req, timeout=None):
        captured["url"] = req.full_url
        captured["headers"] = req.headers
        return FakeResp()

    with mock.patch("urllib.request.urlopen", fake_urlopen):
        out = client.query("SELECT 1")

    assert out == "42"
    assert captured["url"].startswith("http://localhost:8123/?")
    # credentials travel as headers, never in the query string / argv
    assert captured["headers"]["X-clickhouse-user"] == "u"
    assert captured["headers"]["X-clickhouse-key"] == "pw"
