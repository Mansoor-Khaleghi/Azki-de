"""CLI wiring: parser builds, every command dispatches, helpers compute."""
import argparse

import pytest

from azki import cli
from azki.config import load_settings

EXPECTED_COMMANDS = {
    "init", "reset", "seed", "produce", "verify", "dq", "reconcile", "apply-opt",
    "apply-gov", "connect-register", "backfill", "demo",
}


@pytest.fixture
def settings(tmp_path):
    (tmp_path / ".env").write_text("CLICKHOUSE_PASSWORD=pw\nMYSQL_PASSWORD=mpw\n")
    return load_settings(tmp_path / ".env")


def test_parser_exposes_all_commands(settings):
    parser = cli.build_parser(settings)
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    assert set(sub.choices) == EXPECTED_COMMANDS


def test_every_command_binds_a_handler(settings):
    parser = cli.build_parser(settings)
    for name in EXPECTED_COMMANDS:
        ns = parser.parse_args(_min_args(name))
        assert callable(ns.func)


def _min_args(name):
    if name == "backfill":
        return [name, "2025-10-01", "2025-10-07"]
    return [name]


def test_backfill_requires_dates(settings):
    parser = cli.build_parser(settings)
    with pytest.raises(SystemExit):
        parser.parse_args(["backfill"])  # missing start/end


def test_produce_defaults_come_from_settings(settings):
    parser = cli.build_parser(settings)
    ns = parser.parse_args(["produce"])
    assert ns.bootstrap == settings.kafka_bootstrap_host
    assert ns.topic == settings.kafka_topic


def test_expected_rows_counts_minus_header(sample_events_csv, monkeypatch):
    monkeypatch.setattr(cli, "REPO_ROOT", sample_events_csv.parent)
    # 3 data rows in the fixture
    assert cli._expected_rows("user_events.csv") == 3


def test_expected_rows_missing_file_defaults(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    assert cli._expected_rows("nope.csv") == 20000


def test_main_dispatches_to_handler(monkeypatch):
    called = {}
    monkeypatch.setattr(cli, "cmd_verify", lambda s, a: called.setdefault("ok", True))
    cli.main(["verify"])
    assert called["ok"]
