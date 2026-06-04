"""Data-quality runner: row parsing + FAIL/WARN accounting (CH mocked)."""
from azki import quality


def test_parse_check_row():
    assert quality.parse_check("row_count_parity\tPASS\tclickhouse=20000") == (
        "row_count_parity", "PASS", "clickhouse=20000")


def test_parse_check_empty_is_none():
    assert quality.parse_check("") is None


class _FakeClient:
    """Returns canned check rows in order; mimics Client.query()."""
    def __init__(self, rows):
        self._rows = list(rows)

    def query(self, sql, params=None):
        return self._rows.pop(0)


def test_run_checks_counts_failures_and_warns(tmp_path):
    sql = tmp_path / "dq.sql"
    sql.write_text("SELECT 1;\nSELECT 2;\nSELECT 3;")
    client = _FakeClient([
        "completeness\tPASS\tok",
        "lag\tWARN\thigh",
        "parity\tFAIL\tmismatch",
    ])
    failures, warns = quality.run_checks(client, str(sql), expected=20000)
    assert failures == 1
    assert warns == 1


def test_run_checks_query_error_is_a_failure(tmp_path):
    sql = tmp_path / "dq.sql"
    sql.write_text("SELECT 1;")

    class Boom:
        def query(self, *a, **k):
            raise RuntimeError("connection refused")

    failures, warns = quality.run_checks(Boom(), str(sql), expected=1)
    assert failures == 1
