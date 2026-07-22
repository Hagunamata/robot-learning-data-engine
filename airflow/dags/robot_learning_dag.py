"""End-to-end pipeline DAG (M5).

Maps the pipeline stages to Airflow tasks (reused layout from the prior repo). The task
callables reuse the SAME functions that `python -m pipeline` runs (docs/02-development.md
M5), so `make demo` and the DAG execute identical logic — the DAG just adds scheduling,
retries, and per-task logging/observability.

    acquire >> validate >> catalog_real >> augment >> catalog_aug

Runs on the Docker/Airflow runtime (needs airflow installed). `make demo` runs the same
stages without Airflow for a one-command local demo. Config comes from the mounted repo
(config/*.yaml); DATA_ROOT defaults to the compose-mounted ./data.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Stage building blocks (same code as the CLI pipeline).
from acquisition.config import load_sources
from acquisition.storage_guard import StorageGuard
from catalog import CatalogWriter, build_record
from ingest.config import load_quality_gates
from ingest.curate import run_validation
from data_generator import augment_dataset
from spark.jobs.signal_gates import Calibration
from pipeline import merge_datasets

SOURCE = os.getenv("RLDE_SOURCE", "droid-100")
DATA_ROOT = os.getenv("DATA_ROOT", "./data")
ENGINE = os.getenv("RLDE_ENGINE", "spark")
CATALOG_BACKEND = os.getenv("RLDE_CATALOG_BACKEND", "postgres")


def _ctx():
    cfg = load_sources("config/sources.yaml")
    gates = load_quality_gates("config/quality_gates.yaml")
    guard = StorageGuard(DATA_ROOT, budget_gb=cfg.storage_budget_gb)
    return cfg, gates, guard


def acquire_task() -> None:
    from acquisition.downloader import acquire

    cfg, _gates, guard = _ctx()
    acquire(cfg.get(SOURCE), guard)


def validate_task() -> None:
    cfg, gates, guard = _ctx()
    run_validation(SOURCE, guard, data_root=DATA_ROOT, gates=gates, engine=ENGINE)


def catalog_real_task() -> None:
    cfg, _gates, _guard = _ctx()
    src = cfg.get(SOURCE)
    rec = build_record(
        f"v0.1.0-{SOURCE}", SOURCE, f"{DATA_ROOT}/curated/{SOURCE}",
        hf_repo=src.hf_repo, license=src.license, notes="real",
    )
    CatalogWriter(CATALOG_BACKEND).record_version(rec)


def augment_task() -> None:
    cfg, gates, guard = _ctx()
    calib = Calibration.from_file(f"{DATA_ROOT}/calibration/{gates.signal.calibrate_from or SOURCE}.json")
    synth_id = f"{SOURCE}-synth"
    info = augment_dataset(f"{DATA_ROOT}/curated/{SOURCE}", f"{DATA_ROOT}/raw/{synth_id}", calib)
    if info["generated"] > 0:
        run_validation(synth_id, guard, data_root=DATA_ROOT, gates=gates, engine=ENGINE)
        merge_datasets(f"{DATA_ROOT}/curated/{SOURCE}", f"{DATA_ROOT}/curated/{synth_id}", f"{DATA_ROOT}/curated/{SOURCE}-aug")


def catalog_aug_task() -> None:
    cfg, _gates, _guard = _ctx()
    src = cfg.get(SOURCE)
    aug_root = f"{DATA_ROOT}/curated/{SOURCE}-aug"
    if os.path.exists(aug_root):
        rec = build_record(
            f"v0.2.0-{SOURCE}-aug", SOURCE, aug_root,
            hf_repo=src.hf_repo, license=src.license, notes="real+synthetic",
        )
        CatalogWriter(CATALOG_BACKEND).record_version(rec)


with DAG(
    dag_id="robot_learning_data_engine",
    description="Acquire -> validate -> curate/evict -> catalog -> augment -> catalog.",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # triggered on demand (make demo / Airflow UI)
    catchup=False,
    tags=["rlde", "batch"],
) as dag:
    t_acquire = PythonOperator(task_id="acquire", python_callable=acquire_task)
    t_validate = PythonOperator(task_id="validate", python_callable=validate_task)
    t_catalog_real = PythonOperator(task_id="catalog_real", python_callable=catalog_real_task)
    t_augment = PythonOperator(task_id="augment", python_callable=augment_task)
    t_catalog_aug = PythonOperator(task_id="catalog_aug", python_callable=catalog_aug_task)

    t_acquire >> t_validate >> t_catalog_real >> t_augment >> t_catalog_aug
