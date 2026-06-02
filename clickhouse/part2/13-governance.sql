-- =====================================================================
--  Part 2 — Data governance & access control (ClickHouse RBAC).
--
--  Principle of least privilege via roles, column/row-level restriction
--  for PII & financial data, quotas to protect the cluster, and audit
--  through the system log. Run as a privileged user.
-- =====================================================================

-- ─── Roles by job function ───
CREATE ROLE IF NOT EXISTS analyst;       -- BI / dashboards
CREATE ROLE IF NOT EXISTS data_scientist;-- modelling on aggregates + sample
CREATE ROLE IF NOT EXISTS finance;       -- full financial visibility
CREATE ROLE IF NOT EXISTS pipeline_rw;   -- the ETL service account

-- ─── Grants: analysts see aggregates + a PII-masked purchase view, never
--     raw financial columns or the raw event stream. ───
GRANT SELECT ON azki.events_agg_daily_v TO analyst;
GRANT SELECT ON azki.fact_purchases_masked TO analyst;

GRANT SELECT ON azki.events_agg_daily_v TO data_scientist;
GRANT SELECT ON azki.fact_purchases_masked TO data_scientist;
GRANT SELECT ON azki.events_enriched TO data_scientist;   -- behavioural features

GRANT SELECT ON azki.fact_purchases TO finance;           -- full financial
GRANT SELECT ON azki.financial_order TO finance;

-- Pipeline service account: write to the warehouse, manage dictionaries.
GRANT SELECT, INSERT, ALTER, dictGet ON azki.* TO pipeline_rw;

-- ─── Column-level protection: a masked view hides exact financials and
--     coarsens nothing the analyst legitimately needs. ───
CREATE VIEW IF NOT EXISTS azki.fact_purchases_masked AS
SELECT
    event_time,
    user_id,                              -- pseudonymous id (not direct PII)
    city,
    device_type,
    channel,
    product_line,
    attributes,
    -- bucket the money instead of exposing exact amounts
    round(net_amount, -5)        AS net_amount_bucket,
    payment_method,
    payment_status
FROM azki.fact_purchases;

-- ─── Row-level policy: example of regional data segregation. A Tehran
--     analyst role would only see Tehran rows. (Illustrative.) ───
-- CREATE ROW POLICY IF NOT EXISTS rp_tehran ON azki.fact_purchases
--     FOR SELECT USING city = 'Tehran' TO analyst_tehran;

-- ─── Quotas: cap resource use so one bad query can't starve the cluster ───
CREATE QUOTA IF NOT EXISTS q_analyst
    FOR INTERVAL 1 hour MAX queries = 1000, read_rows = 1000000000, execution_time = 1800
    TO analyst;

-- ─── Settings constraints: stop analysts from disabling safety limits ───
ALTER ROLE analyst SETTINGS
    max_execution_time = 60 CONST,
    max_memory_usage = 4000000000 CONST,
    readonly = 1;

-- ─── Assigning roles to users (example) ───
-- CREATE USER bi_dashboard IDENTIFIED BY 'xxx' DEFAULT ROLE analyst;
-- GRANT analyst TO bi_dashboard;

-- =====================================================================
--  Governance practices documented in the report (beyond raw SQL):
--   * Data classification: user_id = pseudonymous, financials = restricted,
--     premium/net_amount = confidential. Tag in a data catalog.
--   * Audit: ClickHouse system.query_log + access to financial_order is
--     reviewable; ship logs to a SIEM.
--   * Lineage: CSV -> Kafka -> CH MVs -> facts; documented + diagrammed.
--   * Retention/right-to-erasure: TTL on raw; ALTER TABLE ... DELETE for
--     GDPR-style subject deletion keyed on user_id.
--   * Secrets: connector passwords via env/secret store, never in repo.
-- =====================================================================
