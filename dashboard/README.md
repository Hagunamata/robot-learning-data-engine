# Quality dashboard — Streamlit (decision resolved)

**Decision (locked):** **Streamlit**, reading the **catalog** (Postgres/sqlite) directly.
Kibana is **not** used for these metrics — ELK/Kibana stays for pipeline **logs** only.

[`app.py`](app.py) shows:
- **Dataset versions & lineage** — one row per catalogued version (source, license,
  episode/frame counts, notes).
- **Gate pass-rate by version** — bar chart.
- **Task distribution** — per selected version.
- **Storage: peak concurrent raw vs budget** — from the scale-run manifests
  (`data/manifest/*.json`), with the measured `peak < budget < total` invariant.

Run it:

```bash
make report                                  # streamlit run dashboard/app.py (sqlite: data/catalog.db)
RLDE_CATALOG_BACKEND=postgres make report    # read the Postgres catalog instead
```

Config via env: `RLDE_CATALOG_BACKEND` (`sqlite`|`postgres`), `RLDE_CATALOG_DB`
(sqlite path), `RLDE_DATA_ROOT` (for manifests), plus the `POSTGRES_*` vars for the
postgres backend. `pandas` ships with Streamlit, so no extra dependency.
