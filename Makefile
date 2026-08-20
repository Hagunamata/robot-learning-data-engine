# Robot-Learning Data Engine — one-command runtime.

.DEFAULT_GOAL := help
.PHONY: help up down seed demo ingest validate report scale test

# Overridable: `make ingest SOURCE=droid-slice` or `make ingest DRY_RUN=1`
SOURCE  ?= droid-100
PYTHON  ?= python
ENGINE  ?= spark
CATALOG ?= postgres
DRY_RUN ?=
_DRY := $(if $(DRY_RUN),--dry-run,)

help:  ## Show this help
	@echo "Robot-Learning Data Engine — available targets:"
	@echo "  make up        Bring up the stack (Postgres, Airflow, ELK, dashboard)"
	@echo "  make down      Tear down the stack"
	@echo "  make seed      Initialize catalog schema + load committed sample episodes"
	@echo "  make demo      Full pipeline on DROID-100 (ENGINE=spark|local CATALOG=postgres|sqlite)"
	@echo "  make ingest    Acquire a source under the storage guard"
	@echo "                   (SOURCE=droid-100 default; DRY_RUN=1 to plan only)"
	@echo "  make validate  Schema + signal gates + curate + evict"
	@echo "                   (SOURCE=droid-100; ENGINE=spark|local)"
	@echo "  make report    Launch the Streamlit data-quality dashboard"
	@echo "  make scale     Synthetic process-and-evict proof (peak<budget<total)"
	@echo "  make test      Run the unit/smoke tests"

up:  ## Bring up the stack (Postgres; Airflow/ELK/Kibana are commented placeholders)
	docker compose up -d

down:  ## Tear down the stack
	docker compose down

seed:  ## Init catalog schema (postgres/init runs on `up`; this is a no-op reminder)
	@echo "Catalog schema is applied from postgres/init/ when Postgres starts (make up)."
	@echo "The sqlite backend creates its table on first write (make demo CATALOG=sqlite)."

demo:  ## Full pipeline on DROID-100: acquire->validate->catalog->augment->catalog
	$(PYTHON) -m pipeline --source $(SOURCE) --engine $(ENGINE) --catalog-backend $(CATALOG)

ingest:  ## Selective acquisition of a source under the storage guard
	$(PYTHON) -m acquisition --source $(SOURCE) $(_DRY)

validate:  ## Full validation: schema + signal gates + curate + evict
	$(PYTHON) -m ingest --source $(SOURCE) --engine $(ENGINE)

report:  ## Launch the Streamlit data-quality dashboard (reads the catalog)
	$(PYTHON) -m streamlit run dashboard/app.py

scale:  ## Synthetic process-and-evict proof (seeded; measured peak<budget<total)
	$(PYTHON) -m scale --synthetic

test:  ## Run the unit/smoke tests
	$(PYTHON) -m pytest -q
