-- =====================================================================
--  Part 1 — Users dimension as a ClickHouse DICTIONARY backed by MySQL.
--
--  Why a dictionary (not a JOIN to a MySQL-engine table)?
--   * dictGet() is an O(1) in-memory hash lookup — ideal for enriching a
--     high-throughput event stream at insert time inside a materialized view.
--   * LIFETIME refreshes it from MySQL automatically, so user attribute
--     changes (city/device) propagate without a manual reload.
--   * Keeps the hot path off the OLTP database.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS azki;

CREATE DICTIONARY IF NOT EXISTS azki.users_dict
(
    user_id     UInt32,
    signup_date Date,
    city        String,
    device_type String
)
PRIMARY KEY user_id
SOURCE(MYSQL(
    host 'mysql'
    port 3306
    user 'azki'
    password 'azkipw'
    db 'azki'
    table 'users'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);   -- refresh every 5–10 minutes

-- Sanity check (run manually):
--   SELECT dictGet('azki.users_dict', 'city', toUInt64(1));
