-- =====================================================================
--  Part 2 — Denormalization reconciliation (gap-filler for the MV).
--
--  WHY: `mv_fact_purchases` denormalizes a purchase at the moment the event
--  is ingested. If the order row hasn't landed yet (late-arriving order, or
--  an order produced after the event), the INNER JOIN yields nothing and the
--  MV can't retroactively fill it — a purchase would be missing from
--  fact_purchases forever.
--
--  This idempotent INSERT…SELECT reconciles those gaps: it inserts only the
--  purchases NOT already present (guarded by order_id), so it is safe to run
--  on a schedule (e.g. every few minutes via Airflow) and after backfills.
--  The streaming MV stays the low-latency happy path; this guarantees
--  eventual completeness.
-- =====================================================================

INSERT INTO azki.fact_purchases
SELECT
    e.event_time,
    e.user_id,
    e.session_id,
    e.channel,
    e.premium_amount      AS event_premium,
    e.city,
    e.device_type,
    e.signup_date,
    po.order_id,
    po.product_line,
    po.premium            AS order_premium,
    po.created_at         AS order_created_at,
    po.attributes,
    fo.payment_method,
    fo.installments,
    fo.discount_amount,
    fo.tax_amount,
    fo.net_amount,
    fo.payment_status,
    fo.paid_at
FROM azki.events_enriched AS e
INNER JOIN azki.product_orders_all AS po
        ON e.user_id = po.user_id AND e.session_id = po.session_id
LEFT  JOIN azki.financial_order AS fo
        ON po.order_id = fo.order_id
WHERE e.event_type = 'purchase'
  AND po.order_id NOT IN (SELECT order_id FROM azki.fact_purchases)
-- one row per order even if the source has duplicate purchase events
LIMIT 1 BY po.order_id;
