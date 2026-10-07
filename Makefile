# raging-rabbits — broker stacks + baseline runs.
#
#   make help                              # this help (default target)
#   make up-single                         # start a broker stack
#   make baseline-single CLIENTS=5000      # start stack + run baseline app
#   make down-single                       # stop a broker stack

.DEFAULT_GOAL := help

COMPOSE ?= docker compose
MVN ?= ./mvnw

# Baseline shape (matches src/main/resources/application.yaml defaults).
CLIENTS ?= 20000
TOPIC_CONTEXTS ?= orders,payments,shipping,notifications,billing,inventory
TOPIC_KEYS ?= 2
FANOUT_EXCHANGES ?= broadcast.announcements,broadcast.alerts,broadcast.config
CONCURRENCY ?= 8

BASELINE_ENV = CLIENTS=$(CLIENTS) TOPIC_CONTEXTS="$(TOPIC_CONTEXTS)" TOPIC_KEYS=$(TOPIC_KEYS) \
	FANOUT_EXCHANGES="$(FANOUT_EXCHANGES)" CONCURRENCY=$(CONCURRENCY)

.PHONY: help build test clean
.PHONY: up-single down-single reset-single baseline-single
.PHONY: up-federated-edges down-federated-edges reset-federated-edges baseline-federated-edges
.PHONY: up-quorum-cycler down-quorum-cycler reset-quorum-cycler baseline-quorum-cycler

help: ## Show this help (default target)
	@echo "Usage: make <target> [CLIENTS=20000]"
	@echo ""
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_.-]+:.*## / { printf "  %-24s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Package the app (skip tests)
	$(MVN) -q -DskipTests package

test: up-single ## Start single stack and run the test suite
	$(MVN) test

clean: ## Maven clean (build output only; broker data untouched)
	$(MVN) -q clean

# --- single: one RabbitMQ node (brokers/single) -----------------------------

up-single: ## Start the single broker stack
	$(COMPOSE) -f brokers/single/compose.yaml up -d

down-single: ## Stop the single broker stack
	$(COMPOSE) -f brokers/single/compose.yaml down

reset-single: ## Fresh reset of the single stack (deletes all broker data)
	$(COMPOSE) -f brokers/single/compose.yaml down
	$(COMPOSE) -f brokers/single/compose.yaml up -d

baseline-single: up-single ## Run the baseline against single (override: CLIENTS=.. TOPIC_KEYS=.. CONCURRENCY=..)
	@echo "waiting for AMQP on localhost:5672..."; \
	for i in $$(seq 1 60); do nc -z localhost 5672 2>/dev/null && break; sleep 2; done; \
	$(BASELINE_ENV) SPRING_RABBITMQ_PORT=5672 $(MVN) -q spring-boot:run

# --- federated-edges: planned (brokers/federated-edges) ---------------------

up-federated-edges: ## [planned] Start the federated-edges stack
	@echo "brokers/federated-edges: planned, not implemented yet (no compose.yaml)"; exit 1

down-federated-edges: ## [planned] Stop the federated-edges stack
	@echo "brokers/federated-edges: planned, not implemented yet (no compose.yaml)"; exit 1

reset-federated-edges: ## [planned] Fresh reset of the federated-edges stack
	@echo "brokers/federated-edges: planned, not implemented yet (no compose.yaml)"; exit 1

baseline-federated-edges: up-federated-edges ## [planned] Run the baseline against federated-edges
	@echo "brokers/federated-edges: planned, not implemented yet (no compose.yaml)"; exit 1

# --- quorum-cycler: planned (brokers/quorum-cycler) -------------------------

up-quorum-cycler: ## [planned] Start the quorum-cycler stack
	@echo "brokers/quorum-cycler: planned, not implemented yet (no compose.yaml)"; exit 1

down-quorum-cycler: ## [planned] Stop the quorum-cycler stack
	@echo "brokers/quorum-cycler: planned, not implemented yet (no compose.yaml)"; exit 1

reset-quorum-cycler: ## [planned] Fresh reset of the quorum-cycler stack
	@echo "brokers/quorum-cycler: planned, not implemented yet (no compose.yaml)"; exit 1

baseline-quorum-cycler: up-quorum-cycler ## [planned] Run the baseline against quorum-cycler
	@echo "brokers/quorum-cycler: planned, not implemented yet (no compose.yaml)"; exit 1
