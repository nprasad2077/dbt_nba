# --- Variables ---
# Service name of the container to run dbt commands in
CONTAINER := dbt_runner
# Project directory inside the container
PROJECT_DIR := /usr/app/dbt/nba_analytics

# Helper command for running dbt
DBT_RUN := docker compose exec $(CONTAINER) dbt --project-dir $(PROJECT_DIR)

# --- Phony Targets ---
.PHONY: clean up shell seed run-staging run-intermediate help

# --- Docker Commands ---

clean: ## 🧹 Force stop containers on the network and run compose down
	@echo "Stopping containers on network and running docker compose down..."
	@docker stop $$(docker ps -q --filter network=dbt_nba_nba_network) && docker compose down

up: ## 🚀 Build and start services with the 'dbt' profile
	@echo "Starting services with 'dbt' profile..."
	@docker compose --profile dbt up --build -d

shell: ## 💻 Open a bash shell inside the dbt_runner container
	@docker compose exec $(CONTAINER) bash

# --- DBT Commands ---
# These targets use the DBT_RUN variable to execute inside the container

seed: ## 🌱 Run dbt seed
	@echo "Running dbt seed..."
	@$(DBT_RUN) seed

run-staging: ## 🏃 Run dbt staging models
	@echo "Running staging models..."
	@$(DBT_RUN) run --select tag:staging

run-intermediate: ## 🏃 Run dbt intermediate models
	@echo "Running intermediate models..."
	@$(DBT_RUN) run --select intermediate

# --- Help Target ---

help: ## ℹ️  Show this help message
	@echo "Available commands:"
	@grep -E '## 💬?' Makefile | awk 'BEGIN {FS = "## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'