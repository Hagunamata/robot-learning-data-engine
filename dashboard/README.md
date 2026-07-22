# Quality dashboard — placeholder (decision pending)

The dashboard must surface **gate pass-rate**, **task distribution**, and **storage
used** (disk-used-vs-budget over time).

**Open DECISION — Kibana vs Streamlit** (see [../docs/01-conception.md](../docs/01-conception.md) §4.8):

- **Kibana** — maximum continuity with the reused ELK stack; reads from Elasticsearch.
- **Streamlit** — lighter; reads directly from the Postgres `catalog` schema.

Default is **Kibana** if the human does not choose otherwise. Nothing is built here
until the decision lands; the chosen artifact (a Kibana export **or** a Streamlit app)
is added in **M6** (`make report`).
