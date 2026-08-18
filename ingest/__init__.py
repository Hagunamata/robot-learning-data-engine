"""Canonical ingest — confirm LeRobot format and schema-gate acquired datasets."""

from .config import QualityGates, load_quality_gates
from .schema_gate import SchemaResult, validate_schema

__all__ = ["validate_schema", "SchemaResult", "load_quality_gates", "QualityGates"]
