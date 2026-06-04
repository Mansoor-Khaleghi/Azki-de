"""Single source of truth for connection settings and credentials.

Everything is read from the environment, falling back to the committed ``.env``
file (throwaway local-demo creds only — see the header in ``.env``). Real
process env vars always win over the file, so the same code runs unchanged:

  * on the host          -> reads ``.env`` (CH on localhost:8123, Kafka :29092)
  * inside compose       -> compose injects CH_HOST=clickhouse, KAFKA_BOOTSTRAP=
                            kafka:9092 etc., which override the file values.

No password is hardcoded anywhere else in the project; SQL/connector templates
carry ``${VAR}`` placeholders that the CLI fills from these settings.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_env_file(path: Path) -> dict[str, str]:
    """Minimal ``.env`` parser (no python-dotenv dependency)."""
    data: dict[str, str] = {}
    if not path.exists():
        return data
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        data[key.strip()] = val.strip()
    return data


def _pick(env_file: dict[str, str], *keys: str, default: str = "") -> str:
    """Real env var wins, then the .env file value, then the default."""
    for k in keys:
        if os.environ.get(k):
            return os.environ[k]
    for k in keys:
        if env_file.get(k):
            return env_file[k]
    return default


@dataclass(frozen=True)
class Settings:
    # ClickHouse (analytical warehouse)
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_db: str
    # MySQL (OLTP users source)
    mysql_user: str
    mysql_password: str
    mysql_root_password: str
    mysql_db: str
    # Kafka
    kafka_bootstrap_host: str
    kafka_bootstrap_internal: str
    kafka_topic: str
    # Compose
    compose_project: str

    @property
    def ch_http_url(self) -> str:
        return f"http://{self.ch_host}:{self.ch_port}/"

    def render_env(self) -> dict[str, str]:
        """Values exposed to ``${VAR}`` substitution in SQL/connector files."""
        return {
            "MYSQL_USER": self.mysql_user,
            "MYSQL_PASSWORD": self.mysql_password,
            "MYSQL_DATABASE": self.mysql_db,
            "CLICKHOUSE_USER": self.ch_user,
            "CLICKHOUSE_PASSWORD": self.ch_password,
            "CLICKHOUSE_DB": self.ch_db,
            "CH_USER": self.ch_user,
            "CH_PASSWORD": self.ch_password,
        }


def load_settings(env_path: str | os.PathLike | None = None) -> Settings:
    path = Path(env_path) if env_path else REPO_ROOT / ".env"
    f = parse_env_file(path)
    return Settings(
        ch_host=_pick(f, "CH_HOST", default="localhost"),
        ch_port=int(_pick(f, "CH_PORT", default="8123")),
        ch_user=_pick(f, "CH_USER", "CLICKHOUSE_USER", default="azki"),
        ch_password=_pick(f, "CH_PASSWORD", "CLICKHOUSE_PASSWORD"),
        ch_db=_pick(f, "CLICKHOUSE_DB", default="azki"),
        mysql_user=_pick(f, "MYSQL_USER", default="azki"),
        mysql_password=_pick(f, "MYSQL_PASSWORD"),
        mysql_root_password=_pick(f, "MYSQL_ROOT_PASSWORD"),
        mysql_db=_pick(f, "MYSQL_DATABASE", default="azki"),
        kafka_bootstrap_host=_pick(
            f, "KAFKA_BOOTSTRAP", "KAFKA_BOOTSTRAP_HOST", default="localhost:29092"
        ),
        kafka_bootstrap_internal=_pick(
            f, "KAFKA_BOOTSTRAP_INTERNAL", default="kafka:9092"
        ),
        kafka_topic=_pick(f, "KAFKA_TOPIC_EVENTS", default="user_events"),
        compose_project=_pick(f, "COMPOSE_PROJECT_NAME", default="azki"),
    )
