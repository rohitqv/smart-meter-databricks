# Smart-grid meter pipeline — deploy automation
#
# Common workflow:
#   make install          # venv + deps + sanity tests
#   make seed             # generate historical data into S3
#   make deploy-backfill  # terraform apply with merge_historical=true
#   make pipeline-run     # trigger first DLT update (full refresh)
#   make verify           # list tables across bronze/silver/gold
#   make deploy           # flip back to merge_historical=false
#   make smoke            # run integration smoke test
#
# Override defaults: make seed BUCKET=other-bucket
#                    make verify CATALOG=staging_smart_grid

BUCKET   ?= bkt-ry-smart-grid-meter-bucket
CATALOG  ?= dev_smart_grid
PYTHON   ?= .venv/bin/python
PYTEST   ?= .venv/bin/pytest
TF       ?= terraform -chdir=terraform

.DEFAULT_GOAL := help
.PHONY: help install test seed seed-nrt tf-init tf-plan deploy-backfill deploy \
        pipeline-id pipeline-run verify smoke fmt clean destroy

help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Create venv, install deps, run unit tests
	uv venv --python 3.11 --clear
	uv pip install -e ".[dev]"
	$(PYTEST) tests/unit/ -v

test: ## Run unit tests
	$(PYTEST) tests/unit/ -v

seed: seed-nrt ## Generate historical data + one NRT tick into S3
	$(PYTHON) -m ingestion.historical.load_historical --bucket $(BUCKET)
	@echo "--- S3 contents ---"
	@aws s3 ls s3://$(BUCKET)/raw/historical/ --recursive | head

seed-nrt: ## Bootstrap NRT prefix with one tick (Auto Loader needs the path to exist)
	$(PYTHON) -m ingestion.nrt.simulate_nrt_feed --bucket $(BUCKET) --rows-per-table 10
	@aws s3 ls s3://$(BUCKET)/raw/nrt/ --recursive | head

tf-init: ## terraform init
	$(TF) init

tf-plan: ## terraform plan (steady-state)
	$(TF) plan

deploy-backfill: ## terraform apply with merge_historical=true (first run)
	$(TF) apply -auto-approve -var merge_historical=true

deploy: ## terraform apply with merge_historical=false (steady state)
	$(TF) apply -auto-approve

pipeline-id: ## Print the DLT pipeline ID
	@databricks pipelines list-pipelines --output json \
		| jq -r '.[] | select(.name == "$(CATALOG)_pipeline") | .pipeline_id'

pipeline-run: ## Trigger DLT pipeline with --full-refresh
	@PID=$$($(MAKE) -s pipeline-id); \
	if [ -z "$$PID" ]; then echo "pipeline not found — did you run 'make deploy-backfill'?" >&2; exit 1; fi; \
	echo "Starting update on pipeline $$PID..."; \
	databricks pipelines start-update "$$PID" --full-refresh

verify: ## SHOW TABLES across bronze/silver/gold
	@if [ -z "$$WAREHOUSE_ID" ]; then \
		WAREHOUSE_ID=$$(databricks warehouses list --output json | jq -r '.[0].id'); \
		echo "Using warehouse: $$WAREHOUSE_ID"; \
	fi; \
	for s in bronze silver gold; do \
		echo "=== $(CATALOG).$$s ==="; \
		databricks api post /api/2.0/sql/statements --json "{\"warehouse_id\":\"$$WAREHOUSE_ID\",\"statement\":\"SHOW TABLES IN $(CATALOG).$$s\",\"wait_timeout\":\"30s\"}" \
			| jq -r '.result.data_array[]? | @tsv'; \
	done

smoke: ## Run integration smoke test (requires DATABRICKS_TOKEN)
	@test -n "$$DATABRICKS_TOKEN" || (echo "DATABRICKS_TOKEN not set" >&2; exit 1)
	SMART_GRID_CATALOG=$(CATALOG) $(PYTEST) tests/integration/test_smoke.py -v

fmt: ## terraform fmt + ruff format
	$(TF) fmt
	.venv/bin/ruff format ingestion tests notebooks

clean: ## Remove venv and pytest cache
	rm -rf .venv .pytest_cache **/__pycache__

destroy: ## Tear down ALL Databricks resources (asks for confirmation)
	@read -p "Destroy catalog $(CATALOG) and all pipelines/jobs? [y/N] " ans; \
	[ "$$ans" = "y" ] || exit 1; \
	$(TF) destroy
