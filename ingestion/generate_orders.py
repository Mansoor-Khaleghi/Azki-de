#!/usr/bin/env python3
"""
Azki DE Task — synthetic order generator (Part 2).

The hiring task says: "Assume order details come from 5 different production
tables." The dataset doesn't ship them, so we synthesize realistic order rows
keyed to the actual purchase events, so the denormalization MV is demonstrably
correct (every purchase joins to exactly one product order + one financial row).

For each `purchase` event in user_events.csv we emit:
  * one row in ONE of the four product tables (round-robin by hash), carrying
    the event's premium and session, so (user_id, session_id) joins back; and
  * one matching row in financial_order (same order_id).

Output: CSVWithNames files under data/orders/, loadable straight into the
ClickHouse tables defined in clickhouse/part2/10-order-tables.sql.
"""
import argparse
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


def stable_line(user_id: str, session_id: str) -> str:
    """Deterministic product-line assignment so reruns are reproducible."""
    h = hashlib.md5(f"{user_id}:{session_id}".encode()).hexdigest()
    return PRODUCT_LINES[int(h, 16) % len(PRODUCT_LINES)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default="data/user_events.csv")
    ap.add_argument("--out", default="data/orders")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    random.seed(args.seed)
    os.makedirs(args.out, exist_ok=True)

    writers, files = {}, {}
    headers = {
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
    for name, cols in headers.items():
        f = open(os.path.join(args.out, f"{name}_order.csv"), "w", newline="")
        files[name] = f
        w = csv.writer(f)
        w.writerow(cols)
        writers[name] = w

    order_id = 0
    n_purchase = 0
    with open(args.events, newline="") as fh:
        for row in csv.DictReader(fh):
            if row["event_type"] != "purchase":
                continue
            order_id += 1
            n_purchase += 1
            uid, sid = row["user_id"], row["session_id"]
            premium = float(row["premium_amount"]) if row["premium_amount"] else 0.0
            created = row["event_time"]
            line = stable_line(uid, sid)

            if line == "third":
                writers["third"].writerow([order_id, uid, sid, premium, created,
                                           random.choice(VEHICLE_TYPES),
                                           random.choice(COVERAGE_TIERS),
                                           random.randint(0, 10)])
            elif line == "body":
                writers["body"].writerow([order_id, uid, sid, premium, created,
                                          round(premium * random.uniform(8, 20), 2),
                                          random.choice(VEHICLE_BRANDS),
                                          round(random.uniform(0, 10), 2)])
            elif line == "medical":
                writers["medical"].writerow([order_id, uid, sid, premium, created,
                                             random.choice(PLAN_TIERS),
                                             random.randint(1, 6),
                                             random.randint(0, 1)])
            else:  # fire
                writers["fire"].writerow([order_id, uid, sid, premium, created,
                                          random.choice(PROPERTY_TYPES),
                                          random.randint(40, 500),
                                          round(premium * random.uniform(20, 60), 2)])

            # financial_order: shared across all lines, keyed by order_id
            discount = round(premium * random.uniform(0, 0.15), 2)
            tax = round((premium - discount) * 0.09, 2)
            net = round(premium - discount + tax, 2)
            installments = random.choice([1, 1, 1, 3, 6, 12])
            writers["financial"].writerow([order_id,
                                           random.choice(PAYMENT_METHODS),
                                           installments, discount, tax, net,
                                           random.choice(PAYMENT_STATUS),
                                           created])

    for f in files.values():
        f.close()
    print(f"Generated {n_purchase} orders across {len(PRODUCT_LINES)} product "
          f"tables + financial_order -> {args.out}/")


if __name__ == "__main__":
    main()
