"""Streamlit data-quality dashboard (M6) — reads the CATALOG directly.

Per the locked decision: Streamlit reads the catalog (sqlite or Postgres) — the source
of truth for dataset versions, gate pass-rate, and task distribution — and reads the
scale manifests for peak-disk-vs-budget. Kibana is NOT used for these metrics (it stays
for pipeline logs only).

Run:
    streamlit run dashboard/app.py            # sqlite (default: data/catalog.db)
    RLDE_CATALOG_BACKEND=postgres streamlit run dashboard/app.py

Config via env: RLDE_CATALOG_BACKEND (sqlite|postgres), RLDE_CATALOG_DB (sqlite path),
RLDE_DATA_ROOT (for data/manifest/*.json). pandas ships with streamlit — no extra dep.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

CATALOG_COLUMNS = [
    "dataset_version", "source_id", "hf_repo", "license", "episode_count",
    "frame_count", "task_distribution", "gate_pass_rate", "bytes_on_disk", "notes",
]

# Resolve data/ relative to this file so the app works regardless of the launch cwd.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB = str(_REPO_ROOT / "data" / "catalog.db")
_DEFAULT_DATA_ROOT = str(_REPO_ROOT / "data")


@st.cache_data(ttl=10)
def load_catalog() -> pd.DataFrame:
    backend = os.getenv("RLDE_CATALOG_BACKEND", "sqlite")
    cols = ", ".join(CATALOG_COLUMNS)
    if backend == "postgres":
        import psycopg

        dsn = os.getenv("RLDE_CATALOG_DSN") or (
            f"host={os.getenv('POSTGRES_HOST','localhost')} port={os.getenv('POSTGRES_PORT','5432')} "
            f"dbname={os.getenv('POSTGRES_DB','robot_learning')} user={os.getenv('POSTGRES_USER','rlde')} "
            f"password={os.getenv('POSTGRES_PASSWORD','')}"
        )
        with psycopg.connect(dsn) as con:
            rows = con.execute(f"SELECT {cols} FROM catalog.dataset_version ORDER BY dataset_version").fetchall()
    else:
        db = os.getenv("RLDE_CATALOG_DB", _DEFAULT_DB)
        if not Path(db).exists():
            return pd.DataFrame(columns=CATALOG_COLUMNS)
        con = sqlite3.connect(db)
        rows = con.execute(f"SELECT {cols} FROM dataset_version ORDER BY dataset_version").fetchall()
        con.close()
    return pd.DataFrame(rows, columns=CATALOG_COLUMNS)


@st.cache_data(ttl=10)
def load_manifests() -> list[dict]:
    root = Path(os.getenv("RLDE_DATA_ROOT", _DEFAULT_DATA_ROOT))
    out = []
    for m in sorted((root / "manifest").glob("*.json")):
        try:
            out.append(json.loads(m.read_text(encoding="utf-8")))
        except Exception:
            continue
    return out


def _gb(n: float) -> float:
    return round(float(n) / (1024 ** 3), 4)


st.set_page_config(page_title="RLDE — Data Quality", page_icon="🤖", layout="wide")
st.title("🤖 Robot-Learning Data Engine — Data Quality")
st.caption("Reads the catalog (sqlite/Postgres) + scale manifests. Logs live in ELK/Kibana, separately.")

catalog = load_catalog()
if catalog.empty:
    st.warning("No catalog rows found. Run `make demo` (or the scale run) to populate the catalog.")
    st.stop()

# --- Dataset versions -------------------------------------------------------
st.subheader("Dataset versions & lineage")
st.dataframe(
    catalog[["dataset_version", "source_id", "license", "episode_count", "frame_count", "gate_pass_rate", "notes"]],
    hide_index=True,
    width="stretch",
)

# --- Gate pass-rate ---------------------------------------------------------
col1, col2 = st.columns(2)
with col1:
    st.subheader("Gate pass-rate by version")
    pr = catalog.dropna(subset=["gate_pass_rate"]).set_index("dataset_version")["gate_pass_rate"].astype(float)
    st.bar_chart(pr, width="stretch")

# --- Task distribution ------------------------------------------------------
with col2:
    st.subheader("Task distribution")
    version = st.selectbox("Version", catalog["dataset_version"].tolist())
    td = catalog.loc[catalog["dataset_version"] == version, "task_distribution"].iloc[0]
    dist = json.loads(td) if isinstance(td, str) else (td or {})
    if dist:
        s = pd.Series(dist, name="episodes").sort_values(ascending=False)
        st.bar_chart(s, width="stretch")
        st.caption(f"{len(dist)} tasks, {int(sum(dist.values()))} episodes in {version}")
    else:
        st.info("No task distribution recorded for this version.")

# --- Peak disk vs budget (scale runs) --------------------------------------
st.subheader("Storage: peak concurrent raw vs budget (scale runs)")
manifests = load_manifests()
if not manifests:
    st.info("No scale manifests yet. Run `python -m scale --synthetic` or the droid-slice scale run.")
else:
    for man in manifests:
        budget = int(man.get("budget_bytes", 0))
        peak = int(man.get("peak_raw_bytes", 0))
        total = int(man.get("total_processed_bytes", 0))
        holds = budget and peak < budget < total
        st.markdown(f"**{man.get('dataset_version', '?')}**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Budget (GB)", _gb(budget))
        c2.metric("Peak raw (GB)", _gb(peak), delta=f"{round(100*peak/budget,1)}% of budget" if budget else None,
                  delta_color="inverse")
        c3.metric("Total processed (GB)", _gb(total), delta=f"{round(total/budget,1)}x budget" if budget else None)
        c4.metric("Invariant", "HOLDS ✅" if holds else "FAILED ❌")
        st.progress(min(1.0, peak / budget) if budget else 0.0,
                    text="peak raw as a fraction of budget (want < 100%)")
        st.caption(
            f"episodes: {man.get('episodes_passed', 0)} passed / "
            f"{man.get('episodes_quarantined', 0)} quarantined · "
            f"{len(man.get('processed_units', []))} units processed"
        )
