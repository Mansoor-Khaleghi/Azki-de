"""Thin wrappers around ``docker compose`` and host-side dataset checks.

These replace the orchestration the Makefile used to do (start the stack, wait
for health, tail logs), but in Python so they share config and error handling
with the rest of the CLI.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from .config import REPO_ROOT

REQUIRED_DATA = ["data/users.csv", "data/user_events.csv"]


def check_data() -> None:
    """Fail fast (like the old ``make check-data``) if the dataset is absent."""
    missing = [f for f in REQUIRED_DATA if not (REPO_ROOT / f).exists()]
    if missing:
        for f in missing:
            print(f"ERROR: missing {f} — place the confidential dataset in "
                  f"data/ (see data/README.md)", file=sys.stderr)
        raise SystemExit(1)
    print("dataset present.")


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run ``docker compose <args>`` from the repo root (so ``.env`` is read)."""
    cmd = ["docker", "compose", *args]
    print("+ " + " ".join(cmd))
    return subprocess.run(cmd, cwd=REPO_ROOT, check=check)
