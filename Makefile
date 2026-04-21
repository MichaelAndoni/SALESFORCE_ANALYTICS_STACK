# Makefile — top-level developer convenience targets
#
# Assumes both projects live as siblings:
#   ./salesforce_ingest/   (CDK + Lambda + Glue)
#   ./salesforce_dbt/      (dbt project)
#
# Usage:
#   make setup              Install all dependencies
#   make deploy-dev         Deploy ingest infra to dev + run dbt against dev
#   make deploy-prod        Deploy ingest infra to prod (requires confirmation)
#   make dbt-run            Run dbt against current DEPLOY_ENV (default: dev)
#   make backfill           Full historical backfill from 2020-01-01
#   make test               Run all unit tests
#   make docs               Generate and serve dbt docs

DEPLOY_ENV   ?= dev
DBT_SELECT   ?=
DBT_TARGET   ?= $(DEPLOY_ENV)
PYTHON       ?= python3

# Infer AWS region and account from current CLI config
AWS_ACCOUNT  := $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
AWS_REGION   := $(shell aws configure get region 2>/dev/null || echo us-east-1)

.PHONY: help setup setup-ingest setup-dbt \
        deploy-dev deploy-prod cdk-diff cdk-deploy \
        dbt-deps dbt-seed dbt-snapshot dbt-run dbt-test dbt-build dbt-docs \
        backfill show-watermarks test lint clean

# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
help:
	@echo ""
	@echo "Salesforce Analytics Stack — Make targets"
	@echo "=========================================="
	@echo ""
	@echo "  Setup"
	@echo "    make setup              Install all Python and dbt dependencies"
	@echo ""
	@echo "  Deploy"
	@echo "    make deploy-dev         CDK deploy to dev + dbt build against dev"
	@echo "    make deploy-prod        CDK deploy to prod (prompts for confirmation)"
	@echo "    make cdk-diff           Show CDK diff without deploying"
	@echo ""
	@echo "  dbt"
	@echo "    make dbt-run            Run dbt models (DEPLOY_ENV=$(DEPLOY_ENV))"
	@echo "    make dbt-test           Run dbt tests"
	@echo "    make dbt-build          Run dbt build (run + test in dependency order)"
	@echo "    make dbt-docs           Generate and serve dbt docs"
	@echo "    make dbt-freshness      Check source freshness"
	@echo ""
	@echo "  Operations"
	@echo "    make backfill           Full backfill from 2020-01-01 (all objects)"
	@echo "    make show-watermarks    Print all SSM watermarks"
	@echo ""
	@echo "  Quality"
	@echo "    make test               Run Python unit tests"
	@echo "    make lint               Lint SQL (sqlfluff) and Python (ruff)"
	@echo ""
	@echo "  Variables (override on command line):"
	@echo "    DEPLOY_ENV=$(DEPLOY_ENV)    dev | prod"
	@echo "    DBT_SELECT=$(DBT_SELECT)   dbt --select expression"
	@echo "    DBT_TARGET=$(DBT_TARGET)   dbt --target override"
	@echo ""

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup: setup-ingest setup-dbt
	@echo "✅ All dependencies installed"

setup-ingest:
	cd salesforce_ingest/cdk && pip install -r requirements.txt -q
	cd salesforce_ingest && pip install -r lambda/extractor/requirements.txt -q
	pip install pytest pytest-mock "moto[s3,ssm,secretsmanager]" ruff mypy -q
	npm install -g aws-cdk@2.170.0 2>/dev/null || true
	@echo "✅ Ingest dependencies installed"

setup-dbt:
	pip install dbt-snowflake==1.8.0 sqlfluff sqlfluff-templater-dbt -q
	cd salesforce_dbt && dbt deps
	@echo "✅ dbt dependencies installed"

# ---------------------------------------------------------------------------
# CDK Infrastructure
# ---------------------------------------------------------------------------
cdk-diff:
	cd salesforce_ingest/cdk && cdk diff --context env=$(DEPLOY_ENV)

cdk-deploy:
	cd salesforce_ingest/cdk && cdk deploy \
		--require-approval never \
		--context env=$(DEPLOY_ENV) \
		--context sf_instance_url=$(SF_INSTANCE_URL) \
		--context sf_username=$(SF_USERNAME) \
		--context sf_client_id=$(SF_CLIENT_ID) \
		--context snowflake_account=$(SNOWFLAKE_ACCOUNT) \
		--context snowflake_user=$(SNOWFLAKE_USER)

deploy-dev: DEPLOY_ENV=dev
deploy-dev: cdk-deploy dbt-build
	@echo "✅ Dev deploy complete"

deploy-prod: DEPLOY_ENV=prod
deploy-prod:
	@echo ""
	@echo "⚠️  About to deploy to PRODUCTION (env=prod)"
	@echo "   AWS account: $(AWS_ACCOUNT)"
	@echo "   Region:      $(AWS_REGION)"
	@echo ""
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	$(MAKE) cdk-deploy DEPLOY_ENV=prod
	@echo "✅ Prod deploy complete"

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------
DBT_BASE = cd salesforce_dbt && dbt

dbt-deps:
	$(DBT_BASE) deps

dbt-seed:
	$(DBT_BASE) seed --target $(DBT_TARGET)

dbt-snapshot:
	$(DBT_BASE) snapshot --target $(DBT_TARGET)

dbt-run:
	$(DBT_BASE) run \
		--target $(DBT_TARGET) \
		$(if $(DBT_SELECT),--select $(DBT_SELECT),)

dbt-test:
	$(DBT_BASE) test \
		--target $(DBT_TARGET) \
		$(if $(DBT_SELECT),--select $(DBT_SELECT),)

dbt-build: dbt-seed dbt-snapshot dbt-run dbt-test
	@echo "✅ dbt build complete ($(DBT_TARGET))"

dbt-docs:
	$(DBT_BASE) docs generate --target $(DBT_TARGET)
	$(DBT_BASE) docs serve

dbt-freshness:
	$(DBT_BASE) source freshness --target $(DBT_TARGET)

dbt-full-refresh:
	$(DBT_BASE) run --full-refresh \
		--target $(DBT_TARGET) \
		$(if $(DBT_SELECT),--select $(DBT_SELECT),)

# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------
backfill:
	@echo "Starting full backfill from 2020-01-01 for all objects..."
	DEPLOY_ENV=$(DEPLOY_ENV) $(PYTHON) salesforce_ingest/scripts/ops.py \
		backfill --start 2020-01-01T00:00:00Z

backfill-object:
	@test -n "$(SF_OBJECT)" || (echo "Set SF_OBJECT=Opportunity" && exit 1)
	DEPLOY_ENV=$(DEPLOY_ENV) $(PYTHON) salesforce_ingest/scripts/ops.py \
		backfill --object $(SF_OBJECT) --start $(BACKFILL_START)

show-watermarks:
	DEPLOY_ENV=$(DEPLOY_ENV) $(PYTHON) salesforce_ingest/scripts/ops.py \
		show-watermarks

invoke-extractor:
	DEPLOY_ENV=$(DEPLOY_ENV) $(PYTHON) salesforce_ingest/scripts/ops.py \
		invoke-extractor $(if $(SF_OBJECTS),--objects $(SF_OBJECTS),)

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
test:
	pytest salesforce_ingest/tests/unit/ -v --tb=short
	@echo "✅ Unit tests passed"

lint:
	ruff check salesforce_ingest/lambda/ salesforce_ingest/glue/ salesforce_ingest/scripts/
	cd salesforce_dbt && sqlfluff lint models/ macros/ snapshots/ --dialect snowflake
	@echo "✅ Lint complete"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null; true
	rm -rf salesforce_dbt/target/ salesforce_dbt/dbt_packages/ 2>/dev/null; true
	rm -rf salesforce_ingest/cdk/.venv/ 2>/dev/null; true
	@echo "✅ Clean"
