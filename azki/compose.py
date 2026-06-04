"""Dataset check and a small ``docker compose`` runner (used by backfill)."""
from __future__ import annotations

import subprocess
import sys

from .config import REPO_ROOT

REQUIRED_DATA = ["data/users.csv", "data/user_events.csv"]


def check_data() -> None:
    missing = [f for f in REQUIRED_DATA if not (REPO_ROOT / f).exists()]
    if missing:
        for f in missing:
            print(f"ERROR: missing {f} — place the dataset in data/ "
                  f"(see data/README.md)", file=sys.stderr)
        raise SystemExit(1)


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", *args]
    print("+ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=REPO_ROOT, check=check)
