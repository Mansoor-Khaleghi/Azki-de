"""Shared test fixtures + path setup.

Adds the repo root (for `import azki`) and spark/ (for the standalone
backfill_job module) to sys.path so the suite runs from anywhere.
"""
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "spark"))


@pytest.fixture
def sample_events_csv(tmp_path):
    """A tiny user_events.csv: 2 purchases (one with empty premium) + 1 view."""
    p = tmp_path / "user_events.csv"
    p.write_text(
        "event_time,user_id,session_id,event_type,channel,premium_amount\n"
        "2025-10-01 10:00:00,1,s-a,purchase,web,1000000\n"
        "2025-10-01 10:05:00,2,s-b,view,app,\n"
        "2025-10-02 11:00:00,3,s-c,purchase,web,\n"
    )
    return p
