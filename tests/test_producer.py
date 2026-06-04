"""Producer event transform (no Kafka broker needed)."""
import pytest

from azki.producer import build_event


def test_build_event_casts_types():
    row = {"event_time": "2025-10-01 10:00:00", "user_id": "7", "session_id": "s1",
           "event_type": "purchase", "channel": "web", "premium_amount": "1500.5"}
    e = build_event(row)
    assert e["user_id"] == 7 and isinstance(e["user_id"], int)
    assert e["premium_amount"] == 1500.5 and isinstance(e["premium_amount"], float)
    assert e["event_type"] == "purchase"


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_empty_premium_is_none(raw):
    row = {"event_time": "t", "user_id": "1", "session_id": "s", "event_type": "view",
           "channel": "app", "premium_amount": raw}
    assert build_event(row)["premium_amount"] is None


def test_missing_premium_key_is_none():
    row = {"event_time": "t", "user_id": "1", "session_id": "s",
           "event_type": "view", "channel": "app"}
    assert build_event(row)["premium_amount"] is None
