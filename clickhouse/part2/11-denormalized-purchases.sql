-- =====================================================================
--  Part 2 — Denormalized purchase fact table via a materialized view.
--
--  For every event_type = 'purchase', enrich the event with its order
--  details: product line + attributes (UNION of 4 product tables) joined
--  to the shared financial_order. One wide, query-ready row per purchase
--  so BI/ML never has to touch 6 tables at read time.
--
--  ORDERING: the MV joins the newly-inserted purchase block against the
--  order tables, so orders must exist when the purchase event arrives.
--  In this demo the order tables are seeded before the event replay; in
--  production you'd guarantee this with either (a) orders landing before
--  the purchase event (they do — the order is what creates the purchase),
--  or (b) a periodic batch reconciliation MV for late-arriving orders.
-- =====================================================================

CREATE TABLE IF NOT EXISTS azki.fact_purchases
(
    -- event grain
    event_time      DateTime CODEC(DoubleDelta, ZSTD(1)),
    user_id         UInt32,
    session_id      String,
    channel         LowCardinality(String),
    event_premium   Nullable(Float64),
    -- user dimension
    city            LowCardinality(String),
    device_type     LowCardinality(String),
    signup_date     Date,
    -- order details (product)
    order_id        UInt64,
    product_line    LowCardinality(String),
    order_premium   Float64,
    order_created_at DateTime,
    attributes      Map(String, String),
    -- order details (financial)
    payment_method  LowCardinality(String),
    installments    UInt8,
    discount_amount Float64,
    tax_amount      Float64,
    net_amount      Float64,
    payment_status  LowCardinality(String),
    paid_at         DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (city, product_line, event_time, user_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS azki.mv_fact_purchases
TO azki.fact_purchases
AS
SELECT
    e.event_time           AS event_time,
    e.user_id              AS user_id,
    e.session_id           AS session_id,
    e.channel              AS channel,
    e.premium_amount       AS event_premium,
    e.city                 AS city,
    e.device_type          AS device_type,
    e.signup_date          AS signup_date,
    po.order_id            AS order_id,
    po.product_line        AS product_line,
    po.premium             AS order_premium,
    po.created_at          AS order_created_at,
    po.attributes          AS attributes,
    fo.payment_method      AS payment_method,
    fo.installments        AS installments,
    fo.discount_amount     AS discount_amount,
    fo.tax_amount          AS tax_amount,
    fo.net_amount          AS net_amount,
    fo.payment_status      AS payment_status,
    fo.paid_at             AS paid_at
FROM azki.events_enriched AS e
INNER JOIN azki.product_orders_all AS po
        ON e.user_id = po.user_id AND e.session_id = po.session_id
LEFT  JOIN azki.financial_order AS fo
        ON po.order_id = fo.order_id
WHERE e.event_type = 'purchase';

-- ─── Backfill helper: same SELECT, run as a one-shot to denormalize
--     purchases that already exist in events_enriched (idempotent given
--     the MergeTree + dedup on order_id at query time). ───
-- INSERT INTO azki.fact_purchases
-- SELECT ... (identical projection) ... ;
