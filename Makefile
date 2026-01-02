.PHONY: up down build shell ingest dbt-run dbt-docs all

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose up -d --build

shell:
	docker compose exec runner bash

ingest:
	@echo "Running ingestion pipelines..."
	docker compose exec runner python -m pipelines.ingest.load_ccount
	docker compose exec runner python -m pipelines.ingest.load_demo
	docker compose exec runner python -m pipelines.ingest.load_upc
	docker compose exec runner python -m pipelines.ingest.load_movement

dbt-run:
	@echo "Running dbt models..."
	docker compose exec runner dbt run --profiles-dir dbt --project-dir dbt

dbt-docs:
	docker compose exec runner dbt docs generate --profiles-dir dbt --project-dir dbt
	docker compose exec runner dbt docs serve --port 8080 --profiles-dir dbt --project-dir dbt

all: build ingest dbt-run
