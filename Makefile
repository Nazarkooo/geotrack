SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

COMPOSE := docker compose
UV := uv run
# Replica count for `make scale-api`.
N ?= 4

.PHONY: help
help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Write .env with freshly generated secrets (keeps an existing file)
	@scripts/bootstrap-env.sh

.PHONY: build
build: env ## Build the application and PostGIS images
	$(COMPOSE) build

.PHONY: up
up: env ## Start the stack and wait until every service reports healthy
	$(COMPOSE) up -d --wait

.PHONY: up-monitoring
up-monitoring: env ## Start the stack together with Prometheus and Grafana
	$(COMPOSE) --profile monitoring up -d --wait

.PHONY: down
down: env ## Stop everything, keeping the database and the recorded metrics
	$(COMPOSE) --profile monitoring --profile loadtest down --remove-orphans

.PHONY: destroy
destroy: env ## Stop everything and drop the volumes as well
	$(COMPOSE) --profile monitoring --profile loadtest down -v --remove-orphans

.PHONY: ps
ps: env ## Show the status of every container
	$(COMPOSE) --profile monitoring --profile loadtest ps

.PHONY: logs
logs: ## Follow the logs of the running services
	$(COMPOSE) logs -f --tail=100

.PHONY: migrate
migrate: env ## Apply the database migrations
	$(COMPOSE) run --rm migrate

.PHONY: smoke
smoke: ## Drive login, zone creation, ingest and a live alert through the running stack
	scripts/smoke.sh

.PHONY: load
load: env ## Run the load generator inside the compose network and follow it
	$(COMPOSE) --profile loadtest up -d generator
	$(COMPOSE) logs -f generator

.PHONY: scale-api
scale-api: env ## Scale the API tier without restarting the rest, e.g. make scale-api N=4
	$(COMPOSE) up -d --no-deps --no-recreate --scale api=$(N) api

.PHONY: psql
psql: ## Open a psql shell on the database
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

.PHONY: redis-cli
redis-cli: ## Open a redis-cli shell
	$(COMPOSE) exec redis sh -c 'REDISCLI_AUTH="$$REDIS_PASSWORD" redis-cli --no-auth-warning'

.PHONY: test
test: ## Run the whole test suite (starts throwaway containers)
	$(UV) pytest -q

.PHONY: test-unit
test-unit: ## Run only the tests that need no infrastructure
	$(UV) pytest -q -m 'not integration'

.PHONY: lint
lint: ## Check style and formatting
	$(UV) ruff check .
	$(UV) ruff format --check .

.PHONY: format
format: ## Apply formatting and the safe lint fixes
	$(UV) ruff format .
	$(UV) ruff check --fix .

.PHONY: typecheck
typecheck: ## Run the type checker
	$(UV) mypy

.PHONY: check
check: lint typecheck test ## Everything CI runs

.PHONY: clean
clean: destroy ## Remove the stack, its volumes, its images and the local tool caches
	-docker image rm geotrack-app:latest geotrack-postgis:18-3.6
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage coverage.xml
