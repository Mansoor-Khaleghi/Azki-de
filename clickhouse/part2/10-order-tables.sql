-- =====================================================================
--  Part 2 — The 5 "production" order tables, replicated into ClickHouse.
--
--  Four product order tables (one per insurance line) + one financial
--  table. In production these would be Debezium CDC topics off MySQL;
--  here they are MergeTree tables seeded by ingestion/generate_orders.py
--  from the purchase events, so the denormalization MV is demonstrable.
--
--  Each product line has line-specific attributes (the reason they are
--  separate tables); they share the order grain: one row per purchase,
--  keyed by order_id and linkable to an event via (user_id, session_id).
-- =====================================================================

-- ─── Third-party auto liability (بیمه شخص ثالث) ───
CREATE TABLE IF NOT EXISTS azki.third_order
(
    order_id        UInt64,
    user_id         UInt32,
    session_id      String,
    premium         Float64,
    created_at      DateTime,
    vehicle_type    LowCardinality(String),   -- car / motorcycle / heavy
    coverage_tier   LowCardinality(String),   -- e.g. obligation level
    no_claim_years  UInt8
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (user_id, session_id, order_id);

-- ─── Auto body / comprehensive (بیمه بدنه) ───
CREATE TABLE IF NOT EXISTS azki.body_order
(
    order_id        UInt64,
    user_id         UInt32,
    session_id      String,
    premium         Float64,
    created_at      DateTime,
    vehicle_value   Float64,
    vehicle_brand   LowCardinality(String),
    franchise_pct   Decimal(4, 2)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (user_id, session_id, order_id);

-- ─── Supplementary medical (بیمه درمان تکمیلی) ───
CREATE TABLE IF NOT EXISTS azki.medical_order
(
    order_id        UInt64,
    user_id         UInt32,
    session_id      String,
    premium         Float64,
    created_at      DateTime,
    plan_tier       LowCardinality(String),
    insured_count   UInt8,
    has_dental      UInt8
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (user_id, session_id, order_id);

-- ─── Fire / property (بیمه آتش‌سوزی) ───
CREATE TABLE IF NOT EXISTS azki.fire_order
(
    order_id        UInt64,
    user_id         UInt32,
    session_id      String,
    premium         Float64,
    created_at      DateTime,
    property_type   LowCardinality(String),   -- residential / commercial
    building_area   UInt32,
    coverage_amount Float64
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (user_id, session_id, order_id);

-- ─── Financial details, shared across all product lines ───
CREATE TABLE IF NOT EXISTS azki.financial_order
(
    order_id        UInt64,
    payment_method  LowCardinality(String),   -- gateway / wallet / installment
    installments    UInt8,
    discount_amount Float64,
    tax_amount      Float64,
    net_amount      Float64,
    payment_status  LowCardinality(String),   -- paid / pending / failed
    paid_at         DateTime
)
ENGINE = ReplacingMergeTree(paid_at)
ORDER BY order_id;

-- ─── Unified product-orders view (the UNION the task asks for) ───
--  Normalises the four line-specific tables to a common grain and folds
--  the line-specific attributes into a Map so the denorm MV stays generic.
CREATE VIEW IF NOT EXISTS azki.product_orders_all AS
SELECT order_id, user_id, session_id, premium, created_at,
       'third' AS product_line,
       map('vehicle_type', vehicle_type,
           'coverage_tier', coverage_tier,
           'no_claim_years', toString(no_claim_years)) AS attributes
FROM azki.third_order
UNION ALL
SELECT order_id, user_id, session_id, premium, created_at,
       'body' AS product_line,
       map('vehicle_value', toString(vehicle_value),
           'vehicle_brand', vehicle_brand,
           'franchise_pct', toString(franchise_pct)) AS attributes
FROM azki.body_order
UNION ALL
SELECT order_id, user_id, session_id, premium, created_at,
       'medical' AS product_line,
       map('plan_tier', plan_tier,
           'insured_count', toString(insured_count),
           'has_dental', toString(has_dental)) AS attributes
FROM azki.medical_order
UNION ALL
SELECT order_id, user_id, session_id, premium, created_at,
       'fire' AS product_line,
       map('property_type', property_type,
           'building_area', toString(building_area),
           'coverage_amount', toString(coverage_amount)) AS attributes
FROM azki.fire_order;
