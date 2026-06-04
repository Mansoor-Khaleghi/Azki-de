"""Spark backfill transform — enrich + dedup + idempotency.

Skipped automatically when pyspark isn't installed (the CLI/runtime don't
require it; it's only for the backfill path).
"""
import pytest

pyspark = pytest.importorskip("pyspark")

import backfill_job  # from spark/ (added to sys.path in conftest)  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.appName("test-backfill").master("local[1]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def _events(spark):
    rows = [
        ("2025-10-03 10:00:00", 1, "s1", "purchase", "web", 100.0),
        ("2025-10-03 10:00:00", 1, "s1", "purchase", "web", 100.0),  # dup natural key
        ("2025-10-09 10:00:00", 2, "s2", "view", "app", None),       # out of window
        ("2025-10-04 11:00:00", 9, "s9", "view", "app", None),       # unknown user
    ]
    return spark.createDataFrame(rows, schema=backfill_job.EVENTS_SCHEMA)


def _users(spark):
    return spark.createDataFrame(
        [(1, "2024-01-01", "Tehran", "ios"), (2, "2024-02-01", "Shiraz", "web")],
        schema=backfill_job.USERS_SCHEMA)


def test_enrich_window_filters_dates_and_dedups(spark):
    out = backfill_job.enrich_window(_events(spark), _users(spark),
                                     "2025-10-01", "2025-10-07")
    rows = out.collect()
    # 2 in-window source rows collapse to: 1 deduped purchase + 1 unknown-user view
    assert len(rows) == 2


def test_unknown_user_enriched_as_unknown(spark):
    out = backfill_job.enrich_window(_events(spark), _users(spark),
                                     "2025-10-01", "2025-10-07")
    cities = {r["user_id"]: r["city"] for r in out.collect()}
    assert cities[9] == "UNKNOWN"
    assert cities[1] == "Tehran"


def test_idempotent_row_count(spark):
    def window():
        return backfill_job.enrich_window(
            _events(spark), _users(spark), "2025-10-01", "2025-10-07").count()

    assert window() == window()
