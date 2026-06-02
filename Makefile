# =====================================================================
#  Azki Senior DE Task — one-command orchestration
# =====================================================================
SHELL := /bin/bash
PYTHON ?= python3   # override with: make PYTHON=.venv/bin/python ...
CH := docker exec -i azki-clickhouse clickhouse-client --user azki --password azkipw

.PHONY: help check-data up up-bonus down clean logs \
        ch-init seed-orders produce verify dq backfill demo

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

check-data:  ## Fail fast if the (git-ignored) dataset is missing
	@for f in data/users.csv data/user_events.csv; do \
	  if [ ! -f "$$f" ]; then \
	    echo "ERROR: missing $$f — place the confidential dataset in data/ (see data/README.md)"; \
	    exit 1; \
	  fi; \
	done
	@echo "dataset present."

up: check-data  ## Start core stack (kafka, mysql, clickhouse)
	docker compose up -d kafka mysql clickhouse
	@echo "Waiting for ClickHouse to be ready..."
	@for i in $$(seq 1 30); do \
	  if $(CH) --query "SELECT 1" >/dev/null 2>&1; then echo "ClickHouse ready."; break; fi; \
	  sleep 2; \
	done
	@docker compose ps

up-bonus:  ## Start the full stack incl. Schema Registry, Connect, Kafka-UI
	docker compose up -d

orchestrate:  ## Start Prefect (server + UI + scheduled monitoring flow) at :4200
	docker compose --profile orchestration up -d prefect
	@echo "Prefect UI -> http://localhost:4200 (serving azki-monitoring every 5 min)"

down:  ## Stop containers (keep volumes)
	docker compose down

clean:  ## Stop and remove volumes (full reset)
	docker compose down -v

logs:  ## Tail clickhouse + kafka logs
	docker compose logs -f clickhouse kafka

ch-init:  ## Create ClickHouse dictionary, Kafka source, MVs, tables
	@echo ">> Part 1 schema"
	cat clickhouse/part1/01-users-dictionary.sql  | $(CH) --multiquery
	cat clickhouse/part1/02-kafka-source.sql       | $(CH) --multiquery
	cat clickhouse/part1/03-events-enriched.sql    | $(CH) --multiquery
	cat clickhouse/part1/04-aggregates.sql         | $(CH) --multiquery
	@echo ">> Part 2 schema"
	cat clickhouse/part2/10-order-tables.sql        | $(CH) --multiquery
	cat clickhouse/part2/11-denormalized-purchases.sql | $(CH) --multiquery

seed-orders:  ## Generate + load synthetic order tables (Part 2)
	$(PYTHON) ingestion/generate_orders.py --events data/user_events.csv --out data/orders
	@for t in third body medical fire financial; do \
	  echo "loading $$t..."; \
	  $(CH) --query "INSERT INTO azki.$${t}_order FORMAT CSVWithNames" < data/orders/$${t}_order.csv; \
	done

produce:  ## Stream user_events.csv into Kafka
	$(PYTHON) ingestion/producer/produce_events.py --bootstrap localhost:29092 \
	  --topic user_events --file data/user_events.csv

verify:  ## Show row counts + sample aggregates from ClickHouse
	bash scripts/verify.sh

dq:  ## Run data quality gate
	$(PYTHON) quality/run_quality_checks.py --expected $$(tail -n +2 data/user_events.csv | wc -l)

denorm-reconcile:  ## Gap-fill fact_purchases for late-arriving orders (idempotent)
	cat clickhouse/part2/14-denorm-reconcile.sql | $(CH) --multiquery

apply-opt:  ## Apply Part 2 performance optimizations
	cat clickhouse/part2/12-optimizations.sql | $(CH) --multiquery

apply-gov:  ## Apply Part 2 governance (roles/policies)
	cat clickhouse/part2/13-governance.sql | $(CH) --multiquery

demo: up ch-init seed-orders produce  ## Full happy-path: up -> schema -> orders -> stream
	@echo "Waiting for ClickHouse to consume the topic..."
	@for i in $$(seq 1 30); do \
	  n=$$($(CH) --query "SELECT count() FROM azki.events_enriched" 2>/dev/null || echo 0); \
	  if [ "$$n" -ge 20000 ]; then echo "consumed $$n events"; break; fi; sleep 2; \
	done
	$(MAKE) denorm-reconcile   # gap-fill any purchases the streaming MV missed
	$(MAKE) verify
