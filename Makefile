# Robot-Learning Data Engine — one-command runtime.
# Verbs mirror the prior repo (up/seed/demo) plus new ones (ingest/validate/report).
# M0: targets are stubs. Each is implemented in its milestone (see docs/01-conception.md §11).

.DEFAULT_GOAL := help
.PHONY: help up down seed demo ingest validate report test

# Overridable: `make ingest SOURCE=droid-slice` or `make ingest DRY_RUN=1`
SOURCE  ?= droid-100
PYTHON  ?= python
ENGINE  ?= spark
DRY_RUN ?=
_DRY := $(if $(DRY_RUN),--dry-run,)

help:  ## Show this help
	@echo "Robot-Learning Data Engine — available targets:"
	@echo "  make up        Bring up the stack (Postgres, Airflow, ELK, dashboard)"
	@echo "  make down      Tear down the stack"
	@echo "  make seed      Initialize catalog schema + load committed sample episodes"
	@echo "  make demo      Run the full pipeline end-to-end on DROID-100"
	@echo "  make ingest    Acquire a source under the storage guard          [M2]"
	@echo "                   (SOURCE=droid-100 default; DRY_RUN=1 to plan only)"
	@echo "  make validate  Schema + signal gates + curate + evict            [M3/M4]"
	@echo "                   (SOURCE=droid-100; ENGINE=spark|local)"
	@echo "  make report    Wire/refresh the quality dashboard               [M6]"
	@echo "  make test      Run the unit/smoke tests"

up:  ## Bring up all containers  [M2+]
	@echo "[M0 stub] docker compose up -d  — not yet implemented"

down:  ## Tear down all containers
	@echo "[M0 stub] docker compose down"

seed:  ## Init catalog schema + load sample episodes  [M5]
	@echo "[M0 stub] seed — not yet implemented"

demo:  ## Full pipeline on DROID-100  [M5]
	@echo "[M0 stub] demo — not yet implemented"

ingest:  ## Selective acquisition of a source under the storage guard  [M2]
	$(PYTHON) -m acquisition --source $(SOURCE) $(_DRY)

validate:  ## Full validation: schema + signal gates + curate + evict  [M3/M4]
	$(PYTHON) -m ingest --source $(SOURCE) --engine $(ENGINE)

report:  ## Refresh the quality dashboard  [M6]
	@echo "[M0 stub] report — not yet implemented"

test:  ## Run the unit/smoke tests
	$(PYTHON) -m pytest -q
