"""Synthetic order generation: determinism, join-completeness, reproducibility."""
import csv

from azki import orders


def _read(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def test_stable_line_is_deterministic_and_in_range():
    a = orders.stable_line("42", "sess-1")
    assert a == orders.stable_line("42", "sess-1")        # stable across calls
    assert a in orders.PRODUCT_LINES


def test_only_purchases_become_orders(sample_events_csv, tmp_path):
    out = tmp_path / "orders"
    counts = orders.generate_orders(str(sample_events_csv), str(out))
    # the sample has 2 purchases (+1 view, ignored)
    assert counts["financial"] == 2
    assert sum(counts[line] for line in orders.PRODUCT_LINES) == 2


def test_every_product_order_has_one_financial_row(sample_events_csv, tmp_path):
    out = tmp_path / "orders"
    orders.generate_orders(str(sample_events_csv), str(out))
    product_ids, fin_ids = set(), set()
    for line in orders.PRODUCT_LINES:
        for row in _read(out / f"{line}_order.csv"):
            product_ids.add(row["order_id"])
    for row in _read(out / "financial_order.csv"):
        fin_ids.add(row["order_id"])
    # exactly one financial row per product order -> the denorm JOIN is complete
    assert product_ids == fin_ids


def test_empty_premium_becomes_zero(sample_events_csv, tmp_path):
    """The purchase with empty premium_amount must still produce a numeric row."""
    out = tmp_path / "orders"
    orders.generate_orders(str(sample_events_csv), str(out))
    premiums = []
    for line in orders.PRODUCT_LINES:
        premiums += [float(r["premium"]) for r in _read(out / f"{line}_order.csv")]
    assert 0.0 in premiums  # the empty-premium purchase


def test_seed_makes_output_reproducible(sample_events_csv, tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    orders.generate_orders(str(sample_events_csv), str(a), seed=7)
    orders.generate_orders(str(sample_events_csv), str(b), seed=7)
    for line in list(orders.PRODUCT_LINES) + ["financial"]:
        name = f"{line}_order.csv"
        assert (a / name).read_text() == (b / name).read_text()
