"""End-to-end pipeline DAG (M0 SKELETON).

Task graph (implemented across M2–M5; wired here in M5):

    acquire  ->  canonicalize  ->  schema_gate  ->  signal_gate  ->  evict_raw
                                                           |
                                            augment (synthetic) --+
                                                           |
                                                     catalog_write

`make demo` triggers this DAG on DROID-100. Reused layout from the prior repo's
airflow/dags/. See docs/01-conception.md §4.6.
"""

# TODO(M5): define the DAG (LocalExecutor, single schedule) once the stage callables
# in acquisition/, ingest/, spark/jobs/, data_generator/, and catalog/ are implemented.
# Intentionally no DAG object at M0 so an Airflow scan finds nothing to run yet.
