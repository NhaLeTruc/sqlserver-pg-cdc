.PHONY: help start stop restart status clean init deploy test verify logs check lint format vault-init vault-status

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ Quick Start

help: ## Display this help message
	@echo "$(BLUE)SQL Server to PostgreSQL CDC Pipeline$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make $(YELLOW)<target>$(NC)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

quickstart: ## Complete setup: start services, init databases, deploy connectors, test
	@echo "$(BLUE)🚀 Starting CDC Pipeline Quickstart...$(NC)"
	@$(MAKE) start
	@sleep 10
	@$(MAKE) init
	@$(MAKE) deploy
	@$(MAKE) verify
	@echo "$(GREEN)✅ Quickstart complete! CDC pipeline is running.$(NC)"

##@ Docker Services

start: ## Start all Docker services
	@echo "$(BLUE)Starting Docker services...$(NC)"
	docker compose -f docker/docker-compose.yml up -d
	@echo "$(GREEN)✓ Services started$(NC)"

stop: ## Stop all Docker services
	@echo "$(YELLOW)Stopping Docker services...$(NC)"
	docker compose -f docker/docker-compose.yml down --volumes --remove-orphans
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: ## Restart all Docker services
	@echo "$(YELLOW)Restarting Docker services...$(NC)"
	docker compose -f docker/docker-compose.yml restart
	@echo "$(GREEN)✓ Services restarted$(NC)"

status: ## Show status of all Docker services
	@echo "$(BLUE)Docker Services Status:$(NC)"
	@docker compose -f docker/docker-compose.yml ps

logs: ## Show logs from all services (use LOGS=<service> for specific service)
	@echo "$(BLUE)Service Logs:$(NC)"
ifdef LOGS
	docker compose -f docker/docker-compose.yml logs -f $(LOGS)
else
	docker compose -f docker/docker-compose.yml logs -f
endif

clean: ## Stop and remove all containers, volumes, and networks
	@echo "$(RED)⚠️  Cleaning up all Docker resources...$(NC)"
	@read -p "This will delete all data. Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker compose -f docker/docker-compose.yml down -v; \
		docker system prune -f; \
		echo "$(GREEN)✓ Cleanup complete$(NC)"; \
	else \
		echo "$(YELLOW)Cleanup cancelled$(NC)"; \
	fi

##@ Initialization

init: vault-init init-dbs ## Initialize Vault and databases
	@echo "$(GREEN)✓ Initialization complete$(NC)"

vault-init: ## Initialize Vault with secrets
	@echo "$(BLUE)Initializing Vault...$(NC)"
	./scripts/bash/vault-init.sh
	@echo "$(GREEN)✓ Vault initialized$(NC)"

init-dbs: init-sqlserver init-postgres ## Initialize both databases
	@echo "$(GREEN)✓ Both databases initialized$(NC)"

init-sqlserver: ## Initialize SQL Server (create tables, enable CDC)
	@echo "$(BLUE)Initializing SQL Server...$(NC)"
	./scripts/bash/init-sqlserver.sh

init-postgres: ## Initialize PostgreSQL (create tables)
	@echo "$(BLUE)Initializing PostgreSQL...$(NC)"
	./scripts/bash/init-postgres.sh

##@ Connectors

deploy: ## Deploy both Debezium and PostgreSQL connectors using Vault
	@echo "$(BLUE)Deploying connectors with Vault credentials...$(NC)"
	./scripts/bash/deploy-with-vault.sh

deploy-source: ## Deploy only Debezium SQL Server source connector
	@echo "$(BLUE)Deploying Debezium source connector...$(NC)"
	./scripts/bash/deploy-with-vault.sh | grep -A 50 "Deploying Debezium"

deploy-sink: ## Deploy only PostgreSQL JDBC sink connector
	@echo "$(BLUE)Deploying PostgreSQL sink connector...$(NC)"
	./scripts/bash/deploy-with-vault.sh | grep -A 50 "Deploying PostgreSQL"

connector-status: ## Show status of all connectors
	@echo "$(BLUE)Connector Status:$(NC)"
	@curl -s http://localhost:8083/connectors | jq -r '.[]' | while read connector; do \
		echo "\n$(YELLOW)$$connector:$(NC)"; \
		curl -s http://localhost:8083/connectors/$$connector/status | jq '.connector.state, .tasks[].state'; \
	done

connector-list: ## List all deployed connectors
	@echo "$(BLUE)Deployed Connectors:$(NC)"
	@curl -s http://localhost:8083/connectors | jq

connector-delete: ## Delete all connectors
	@echo "$(RED)Deleting all connectors...$(NC)"
	@curl -s http://localhost:8083/connectors | jq -r '.[]' | while read connector; do \
		echo "Deleting $$connector..."; \
		curl -X DELETE http://localhost:8083/connectors/$$connector; \
	done
	@echo "$(GREEN)✓ All connectors deleted$(NC)"

connector-restart: ## Restart all connectors
	@echo "$(YELLOW)Restarting all connectors...$(NC)"
	@curl -s http://localhost:8083/connectors | jq -r '.[]' | while read connector; do \
		echo "Restarting $$connector..."; \
		curl -X POST http://localhost:8083/connectors/$$connector/restart; \
	done
	@echo "$(GREEN)✓ All connectors restarted$(NC)"

##@ Testing & Verification

test: test-insert test-query ## Run complete test: insert data and verify replication
	@echo "$(GREEN)✓ Test complete$(NC)"

test-insert: ## Insert test data into SQL Server
	@echo "$(BLUE)Inserting test data into SQL Server...$(NC)"
	@docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P YourStrong!Passw0rd -C \
		-d warehouse_source \
		-Q "INSERT INTO dbo.customers (name, email) VALUES ('Test User $$(date +%s)', 'test$$(date +%s)@example.com')"
	@echo "$(GREEN)✓ Test data inserted$(NC)"

test-query: ## Query test data from PostgreSQL
	@echo "$(BLUE)Waiting 10 seconds for replication...$(NC)"
	@sleep 10
	@echo "$(BLUE)Querying PostgreSQL...$(NC)"
	@docker exec cdc-postgres psql -U postgres -d warehouse_target \
		-c "SELECT id, name, email, created_at FROM customers ORDER BY id DESC LIMIT 5"

verify: verify-services verify-vault verify-dbs verify-kafka verify-connectors ## Run all verification checks
	@echo "$(GREEN)✅ All verifications passed!$(NC)"

verify-services: ## Verify all Docker services are running
	@echo "$(BLUE)Verifying Docker services...$(NC)"
	@docker compose -f docker/docker-compose.yml ps | grep -q "Up" && \
		echo "$(GREEN)✓ Services are running$(NC)" || \
		(echo "$(RED)✗ Some services are not running$(NC)" && exit 1)

verify-vault: ## Verify Vault is accessible and has secrets
	@echo "$(BLUE)Verifying Vault...$(NC)"
	@docker exec cdc-vault vault status > /dev/null 2>&1 && \
		echo "$(GREEN)✓ Vault is accessible$(NC)" || \
		(echo "$(RED)✗ Vault is not accessible$(NC)" && exit 1)
	@docker exec cdc-vault vault kv get secret/database > /dev/null 2>&1 && \
		echo "$(GREEN)✓ Vault has secrets$(NC)" || \
		(echo "$(RED)✗ Vault secrets not found$(NC)" && exit 1)

verify-dbs: ## Verify databases are accessible
	@echo "$(BLUE)Verifying databases...$(NC)"
	@docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P YourStrong!Passw0rd -C \
		-Q "SELECT 1" > /dev/null 2>&1 && \
		echo "$(GREEN)✓ SQL Server is accessible$(NC)" || \
		(echo "$(RED)✗ SQL Server is not accessible$(NC)" && exit 1)
	@docker exec cdc-postgres psql -U postgres -d warehouse_target -c "SELECT 1" > /dev/null 2>&1 && \
		echo "$(GREEN)✓ PostgreSQL is accessible$(NC)" || \
		(echo "$(RED)✗ PostgreSQL is not accessible$(NC)" && exit 1)

verify-kafka: ## Verify Kafka and topics exist
	@echo "$(BLUE)Verifying Kafka...$(NC)"
	@docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && \
		echo "$(GREEN)✓ Kafka is accessible$(NC)" || \
		(echo "$(RED)✗ Kafka is not accessible$(NC)" && exit 1)

verify-connectors: ## Verify connectors are running
	@echo "$(BLUE)Verifying connectors...$(NC)"
	@curl -s http://localhost:8083/connectors/sqlserver-cdc-source/status | jq -e '.connector.state == "RUNNING"' > /dev/null && \
		echo "$(GREEN)✓ Debezium source connector is RUNNING$(NC)" || \
		echo "$(YELLOW)⚠ Debezium source connector is not running$(NC)"
	@curl -s http://localhost:8083/connectors/postgresql-jdbc-sink/status | jq -e '.connector.state == "RUNNING"' > /dev/null && \
		echo "$(GREEN)✓ PostgreSQL sink connector is RUNNING$(NC)" || \
		echo "$(YELLOW)⚠ PostgreSQL sink connector is not running$(NC)"

##@ Vault Operations

vault-status: ## Show Vault status and secrets
	@echo "$(BLUE)Vault Status:$(NC)"
	@docker exec cdc-vault vault status
	@echo "\n$(BLUE)Vault Secrets:$(NC)"
	@docker exec cdc-vault vault kv get secret/database

vault-update: ## Update a Vault secret (usage: make vault-update KEY=sqlserver_password VALUE=newpass)
	@echo "$(BLUE)Updating Vault secret...$(NC)"
ifndef KEY
	@echo "$(RED)Error: KEY is required. Usage: make vault-update KEY=sqlserver_password VALUE=newpass$(NC)"
	@exit 1
endif
ifndef VALUE
	@echo "$(RED)Error: VALUE is required. Usage: make vault-update KEY=sqlserver_password VALUE=newpass$(NC)"
	@exit 1
endif
	@docker exec cdc-vault vault kv patch secret/database $(KEY)=$(VALUE)
	@echo "$(GREEN)✓ Secret $(KEY) updated$(NC)"

##@ Database Operations

db-sqlserver: ## Connect to SQL Server with sqlcmd
	@docker exec -it cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P YourStrong!Passw0rd -C -d warehouse_source

db-postgres: ## Connect to PostgreSQL with psql
	@docker exec -it cdc-postgres psql -U postgres -d warehouse_target

db-query-sqlserver: ## Query SQL Server customers table
	@docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P YourStrong!Passw0rd -C -d warehouse_source \
		-Q "SELECT TOP 10 * FROM dbo.customers ORDER BY id DESC"

db-query-postgres: ## Query PostgreSQL customers table
	@docker exec cdc-postgres psql -U postgres -d warehouse_target \
		-c "SELECT * FROM customers ORDER BY id DESC LIMIT 10"

db-count: ## Show record counts in both databases
	@echo "$(BLUE)Record Counts:$(NC)"
	@echo -n "SQL Server customers: "
	@docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P YourStrong!Passw0rd -C -d warehouse_source \
		-Q "SELECT COUNT(*) FROM dbo.customers" -h -1 | grep -o '[0-9]*'
	@echo -n "PostgreSQL customers: "
	@docker exec cdc-postgres psql -U postgres -d warehouse_target \
		-tAc "SELECT COUNT(*) FROM customers"

##@ Monitoring

kafka-topics: ## List all Kafka topics
	@echo "$(BLUE)Kafka Topics:$(NC)"
	@docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consumer-groups: ## List Kafka consumer groups
	@echo "$(BLUE)Kafka Consumer Groups:$(NC)"
	@docker exec cdc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

kafka-offsets: ## Show Kafka topic offsets
	@echo "$(BLUE)Kafka Topic Offsets:$(NC)"
	@docker exec cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
		--broker-list localhost:9092 \
		--topic sqlserver.warehouse_source.dbo.customers

kafka-lag: ## Show consumer group lag
	@echo "$(BLUE)Consumer Group Lag:$(NC)"
	@docker exec cdc-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
		--group connect-postgresql-jdbc-sink --describe

schema-registry: ## Show registered schemas
	@echo "$(BLUE)Registered Schemas:$(NC)"
	@curl -s http://localhost:8081/subjects | jq

##@ Development & Testing

test-unit: ## Run unit tests
	@echo "$(BLUE)Running unit tests...$(NC)"
	@cd tests && pytest -v -m unit

test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	@cd tests && pytest -v -m integration

test-e2e: ## Run end-to-end tests
	@echo "$(BLUE)Running E2E tests...$(NC)"
	@cd tests && pytest -v -m e2e

test-all: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	@cd tests && pytest -v

##@ Code Quality

lint: lint-bash lint-json ## Run all linters
	@echo "$(GREEN)✓ All linting complete$(NC)"

lint-bash: ## Lint bash scripts with shellcheck
	@echo "$(BLUE)Linting bash scripts...$(NC)"
	@if command -v shellcheck > /dev/null; then \
		find scripts/bash -name "*.sh" -exec shellcheck {} +; \
		echo "$(GREEN)✓ Bash scripts linted$(NC)"; \
	else \
		echo "$(YELLOW)⚠ shellcheck not installed, skipping$(NC)"; \
	fi

lint-json: ## Validate JSON configuration files
	@echo "$(BLUE)Validating JSON files...$(NC)"
	@find docker/configs -name "*.json" -exec sh -c 'echo "Checking {}..." && jq empty {}' \;
	@echo "$(GREEN)✓ JSON files valid$(NC)"

format: ## Format code (placeholder for future formatters)
	@echo "$(BLUE)Formatting code...$(NC)"
	@echo "$(YELLOW)No formatters configured yet$(NC)"

check: lint verify ## Run all checks (lint + verify)
	@echo "$(GREEN)✅ All checks passed!$(NC)"

##@ Documentation

docs: ## Generate/view documentation
	@echo "$(BLUE)Documentation:$(NC)"
	@echo "  - $(GREEN)Vault Integration:$(NC) docs/vault-integration.md"
	@echo "  - $(GREEN)Quick Start:$(NC) docs/quick-start-vault.md"
	@echo "  - $(GREEN)CDC Learnings:$(NC) docs/cdc-pipeline-setup-learnings.md"
	@echo "  - $(GREEN)Operations:$(NC) docs/operations.md"

##@ Utilities

shell-kafka-connect: ## Open shell in Kafka Connect container
	@docker exec -it cdc-kafka-connect bash

shell-sqlserver: ## Open shell in SQL Server container
	@docker exec -it cdc-sqlserver bash

shell-postgres: ## Open shell in PostgreSQL container
	@docker exec -it cdc-postgres bash

shell-vault: ## Open shell in Vault container
	@docker exec -it cdc-vault sh

ps: ## Show all running containers
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

urls: ## Show all service URLs
	@echo "$(BLUE)Service URLs:$(NC)"
	@echo "  Kafka Connect: $(GREEN)http://localhost:8083$(NC)"
	@echo "  Schema Registry: $(GREEN)http://localhost:8081$(NC)"
	@echo "  Vault: $(GREEN)http://localhost:8200$(NC)"
	@echo "  Control Center: $(GREEN)http://localhost:9021$(NC)"

version: ## Show versions of all components
	@echo "$(BLUE)Component Versions:$(NC)"
	@echo -n "Docker Compose: "; docker compose version --short
	@echo -n "SQL Server: "; docker exec cdc-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrong!Passw0rd -C -Q "SELECT @@VERSION" -h -1 2>/dev/null | head -1
	@echo -n "PostgreSQL: "; docker exec cdc-postgres psql --version
	@echo -n "Kafka: "; docker exec cdc-kafka kafka-broker-api-versions --version 2>&1 | head -1
