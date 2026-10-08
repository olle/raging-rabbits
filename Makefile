# raging-rabbits — run the baseline against the single broker stack.
#
#   make help                      # this help (default target)
#   make run-single CLIENTS=5000   # start stack + run baseline app

.DEFAULT_GOAL := help

COMPOSE ?= docker compose
MVN ?= ./mvnw

# Baseline shape (matches src/main/resources/application.yaml defaults).
CLIENTS ?= 20000
TOPICS ?= orders,payments,shipping,notifications,billing,inventory
KEYS ?= 2
FANOUTS ?= broadcast.announcements,broadcast.alerts,broadcast.config
CONCURRENCY ?= 8
NOISE ?= off
CONSUMERS ?= 1

BASELINE_ENV = CLIENTS=$(CLIENTS) TOPICS="$(TOPICS)" KEYS=$(KEYS) \
	FANOUTS="$(FANOUTS)" CONCURRENCY=$(CONCURRENCY) NOISE=$(NOISE) CONSUMERS=$(CONSUMERS)

.PHONY: help run-single

help: ## Show this help (default target)
	@echo "Usage: make <target> [CLIENTS=20000]"
	@echo ""
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_.-]+:.*## / { printf "  %-24s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

run-single: ## Start the single stack and run the baseline (override: CLIENTS=.. KEYS=.. NOISE=low|medium|high CONSUMERS=<workers>)
	$(COMPOSE) -f brokers/single/compose.yaml up -d
	@echo "waiting for AMQP on localhost:5672..."; \
	for i in $$(seq 1 60); do nc -z localhost 5672 2>/dev/null && break; sleep 2; done; \
	$(BASELINE_ENV) SPRING_RABBITMQ_PORT=5672 $(MVN) -q spring-boot:run
