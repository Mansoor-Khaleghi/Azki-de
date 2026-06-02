#!/usr/bin/env bash
# Quick end-to-end verification of the ClickHouse warehouse.
set -euo pipefail
CH="docker exec -i azki-clickhouse clickhouse-client --user azki --password azkipw"

echo "================ Azki pipeline verification ================"
echo
echo "## users_dict (from MySQL):"
$CH --query "SELECT count() AS users FROM azki.users_dict"
echo
echo "## events_enriched (raw enriched layer):"
$CH --query "SELECT count() AS rows, uniq(user_id) AS users, min(event_time), max(event_time) FROM azki.events_enriched"
echo
echo "## enrichment coverage (should be ~0 UNKNOWN):"
$CH --query "SELECT countIf(city='UNKNOWN') AS unmatched, count() AS total FROM azki.events_enriched"
echo
echo "## aggregates by event_type (count / uniq users / avg premium):"
$CH --query "
  SELECT event_type,
         sum(events_count)        AS events,
         sum(unique_users)        AS approx_users,
         round(avg(premium_avg))  AS avg_premium
  FROM azki.events_agg_daily_v
  GROUP BY event_type ORDER BY events DESC
  FORMAT PrettyCompact"
echo
echo "## top channels for purchases:"
$CH --query "
  SELECT channel, sum(events_count) AS purchases, round(sum(premium_sum)) AS premium_sum
  FROM azki.events_agg_daily_v
  WHERE event_type='purchase'
  GROUP BY channel ORDER BY purchases DESC
  FORMAT PrettyCompact"
echo
echo "## fact_purchases (denormalized, Part 2):"
$CH --query "SELECT count() AS rows, uniq(product_line) AS lines FROM azki.fact_purchases" 2>/dev/null || echo "(not built yet)"
echo
echo "## denormalized sample by product line:"
$CH --query "
  SELECT product_line,
         count()                  AS orders,
         round(avg(net_amount))   AS avg_net,
         round(avg(installments),1) AS avg_installments
  FROM azki.fact_purchases
  GROUP BY product_line ORDER BY orders DESC
  FORMAT PrettyCompact" 2>/dev/null || echo "(fact_purchases empty)"
echo "============================================================"
