"""Settings load from .env with real-env override and sensible defaults."""
from azki.config import load_settings, parse_env_file


def _write_env(tmp_path, body):
    p = tmp_path / ".env"
    p.write_text(body)
    return p


def test_parse_env_file_skips_comments_and_blanks(tmp_path):
    env = parse_env_file(_write_env(tmp_path, "# comment\n\nA=1\nB = two \n"))
    assert env == {"A": "1", "B": "two"}


def test_load_settings_reads_file(tmp_path):
    p = _write_env(tmp_path,
                   "CLICKHOUSE_USER=ch\nCLICKHOUSE_PASSWORD=secret\n"
                   "MYSQL_PASSWORD=mpw\nKAFKA_TOPIC_EVENTS=evts\n")
    s = load_settings(p)
    assert s.ch_user == "ch"
    assert s.ch_password == "secret"
    assert s.mysql_password == "mpw"
    assert s.kafka_topic == "evts"


def test_defaults_when_absent(tmp_path):
    s = load_settings(_write_env(tmp_path, ""))
    assert s.ch_host == "localhost"
    assert s.ch_port == 8123
    assert s.ch_user == "azki"
    assert s.ch_db == "azki"
    assert s.kafka_bootstrap_host == "localhost:29092"


def test_real_env_overrides_file(tmp_path, monkeypatch):
    p = _write_env(tmp_path, "CH_HOST=localhost\nCLICKHOUSE_PASSWORD=fromfile\n")
    monkeypatch.setenv("CH_HOST", "clickhouse")
    monkeypatch.setenv("CH_PASSWORD", "fromenv")
    s = load_settings(p)
    assert s.ch_host == "clickhouse"        # env wins over file
    assert s.ch_password == "fromenv"       # CH_PASSWORD wins over CLICKHOUSE_PASSWORD


def test_ch_password_falls_back_to_clickhouse_password(tmp_path):
    s = load_settings(_write_env(tmp_path, "CLICKHOUSE_PASSWORD=onlythis\n"))
    assert s.ch_password == "onlythis"


def test_render_env_exposes_substitution_vars(tmp_path):
    s = load_settings(_write_env(tmp_path, "MYSQL_PASSWORD=p\nCLICKHOUSE_USER=u\n"))
    env = s.render_env()
    assert env["MYSQL_PASSWORD"] == "p"
    assert env["CLICKHOUSE_USER"] == "u"
