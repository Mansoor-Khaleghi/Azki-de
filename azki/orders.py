"""Synthetic order generator (Part 2).

The task says: "Assume order details come from 5 different production tables."
The dataset doesn't ship them, so we synthesize realistic order rows keyed to
the actual purchase events, so the denormalization MV is demonstrably correct
(every purchase joins to exactly one product order + one financial row).

For each ``purchase`` event in user_events.csv we emit:
  * one row in ONE of the four product tables (deterministic by hash), carrying
    the event's premium and session, so (user_id, session_id) joins back; and
  * one matching row in financial_order (same order_id).
"""
from __future__ import annotations

import csv
import hashlib
import os
import random

PRODUCT_LINES = ["third", "body", "medical", "fire"]

VEHICLE_TYPES = ["car", "motorcycle", "heavy"]
COVERAGE_TIERS = ["base", "1.5x", "2x", "3x"]
VEHICLE_BRANDS = ["Saipa", "IranKhodro", "Toyota", "Hyundai", "Kia", "Renault"]
PLAN_TIERS = ["bronze", "silver", "gold", "platinum"]
PROPERTY_TYPES = ["residential", "commercial"]
PAYMENT_METHODS = ["gateway", "wallet", "installment"]
PAYMENT_STATUS = ["paid", "paid", "paid", "pending", "failed"]  # mostly paid

HEADERS = {
    "third": ["order_id", "user_id", "session_id", "premium", "created_at",
              "vehicle_type", "coverage_tier", "no_claim_years"],
    "body": ["order_id", "user_id", "session_id", "premium", "created_at",
             "vehicle_value", "vehicle_brand", "franchise_pct"],
    "medical": ["order_id", "user_id", "session_id", "premium", "created_at",
                "plan_tier", "insured_count", "has_dental"],
    "fire": ["order_id", "user_id", "session_id", "premium", "created_at",
             "property_type", "building_area", "coverage_amount"],
    "financial": ["order_id", "payment_method", "installments",
                  "discount_amount", "tax_amount", "net_amount",
                  "payment_status", "paid_at"],
}


def stable_line(user_id: str, session_id: str) -> str:
    """Deterministic product-line assignment so reruns are reproducible."""
    h = hashlib.md5(f"{user_id}:{session_id}".encode()).hexdigest()
    return PRODUCT_LINES[int(h, 16) % len(PRODUCT_LINES)]


def generate_orders(events_path: str, out_dir: str, seed: int = 42) -> dict[str, int]:
    """Write the 5 order CSVs from purchase events. Returns per-table row counts.

    Pure (no DB) so it is unit-testable; the CLI then loads the CSVs into
    ClickHouse. Deterministic given ``seed`` + the same input.
    """
    rng = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)

    writers, files, counts = {}, {}, {name: 0 for name in HEADERS}
    for name, cols in HEADERS.items():
        fh = open(os.path.join(out_dir, f"{name}_order.csv"), "w", newline="")
        files[name] = fh
        w = csv.writer(fh)
        w.writerow(cols)
        writers[name] = w

    order_id = 0
    try:
        with open(events_path, newline="") as fh:
            for row in csv.DictReader(fh):
                if row["event_type"] != "purchase":
                    continue
                order_id += 1
                uid, sid = row["user_id"], row["session_id"]
                premium = float(row["premium_amount"]) if row["premium_amount"] else 0.0
                created = row["event_time"]
                line = stable_line(uid, sid)

                if line == "third":
                    writers["third"].writerow([order_id, uid, sid, premium, created,
                                               rng.choice(VEHICLE_TYPES),
                                               rng.choice(COVERAGE_TIERS),
                                               rng.randint(0, 10)])
                elif line == "body":
                    writers["body"].writerow([order_id, uid, sid, premium, created,
                                              round(premium * rng.uniform(8, 20), 2),
                                              rng.choice(VEHICLE_BRANDS),
                                              round(rng.uniform(0, 10), 2)])
                elif line == "medical":
                    writers["medical"].writerow([order_id, uid, sid, premium, created,
                                                 rng.choice(PLAN_TIERS),
                                                 rng.randint(1, 6),
                                                 rng.randint(0, 1)])
                else:  # fire
                    writers["fire"].writerow([order_id, uid, sid, premium, created,
                                              rng.choice(PROPERTY_TYPES),
                                              rng.randint(40, 500),
                                              round(premium * rng.uniform(20, 60), 2)])
                counts[line] += 1

                # financial_order: shared across all lines, keyed by order_id
                discount = round(premium * rng.uniform(0, 0.15), 2)
                tax = round((premium - discount) * 0.09, 2)
                net = round(premium - discount + tax, 2)
                installments = rng.choice([1, 1, 1, 3, 6, 12])
                writers["financial"].writerow([order_id,
                                               rng.choice(PAYMENT_METHODS),
                                               installments, discount, tax, net,
                                               rng.choice(PAYMENT_STATUS),
                                               created])
                counts["financial"] += 1
    finally:
        for fh in files.values():
            fh.close()
    return counts


def main(argv: list[str] | None = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--events", default="data/user_events.csv")
    ap.add_argument("--out", default="data/orders")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args(argv)
    counts = generate_orders(args.events, args.out, args.seed)
    print(f"Generated {counts['financial']} orders across {len(PRODUCT_LINES)} "
          f"product tables + financial_order -> {args.out}/")
    print("  " + ", ".join(f"{k}={v}" for k, v in counts.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
