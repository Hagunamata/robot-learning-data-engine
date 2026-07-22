# Robot-Learning Data Engine — one-command runtime.
# Verbs mirror the prior repo (up/seed/demo) plus new ones (ingest/validate/report).
# M0: targets are stubs. Each is implemented in its milestone (see docs/01-conception.md §11).

.DEFAULT_GOAL := help
.PHONY: help up down seed demo ingest validate report

help:  ## Show this help
	@echo "Robot-Learning Data Engine — available targets:"
	@echo "  make up        Bring up the stack (Postgres, Airflow, ELK, dashboard)"
	@echo "  make down      Tear down the stack"
	@echo "  make seed      Initialize catalog schema + load committed sample episodes"
	@echo "  make demo      Run the full pipeline end-to-end on DROID-100"
	@echo "  make ingest    Acquire the dev source (storage guard enforced)  [M2]"
	@echo "  make validate  Run schema + signal quality gates                [M3/M4]"
	@echo "  make report    Wire/refresh the quality dashboard               [M6]"

up:  ## Bring up all containers  [M2+]
	@echo "[M0 stub] docker compose up -d  — not yet implemented"

down:  ## Tear down all containers
	@echo "[M0 stub] docker compose down"

seed:  ## Init catalog schema + load sample episodes  [M5]
	@echo "[M0 stub] seed — not yet implemented"

demo:  ## Full pipeline on DROID-100  [M5]
	@echo "[M0 stub] demo — not yet implemented"

ingest:  ## Selective acquisition of the dev source  [M2]
	@echo "[M0 stub] ingest — not yet implemented"

validate:  ## Schema + signal quality gates  [M3/M4]
	@echo "[M0 stub] validate — not yet implemented"

report:  ## Refresh the quality dashboard  [M6]
	@echo "[M0 stub] report — not yet implemented"
