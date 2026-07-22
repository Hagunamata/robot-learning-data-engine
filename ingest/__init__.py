"""Canonical ingest — confirm LeRobot format and schema-gate acquired datasets.

Public surface:
    - validate_schema, SchemaResult   (schema_gate)
    - ingest_source, IngestResult      (canonicalize)
    - load_quality_gates, QualityGates (config)

Engine: schema validation is PyArrow (M3); the M4 signal gates use Spark.
See docs/01-conception.md §4.2 and docs/02-development.md (M3). Implemented in M3.
"""

from .config import QualityGates, load_quality_gates
from .schema_gate import SchemaResult, validate_schema

__all__ = ["validate_schema", "SchemaResult", "load_quality_gates", "QualityGates"]
