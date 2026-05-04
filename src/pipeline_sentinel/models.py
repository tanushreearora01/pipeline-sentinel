"""
Data models for pipeline_sentinel
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AnomalyType(str, Enum):
    ROW_COUNT_DROP = "ROW_COUNT_DROP"
    ROW_COUNT_SPIKE = "ROW_COUNT_SPIKE"
    NULL_RATE_SPIKE = "NULL_RATE_SPIKE"
    PROCESSING_TIME_SPIKE = "PROCESSING_TIME_SPIKE"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    DUPLICATE_SPIKE = "DUPLICATE_SPIKE"
    EMPTY_DATASET = "EMPTY_DATASET"


@dataclass
class PipelineRun:
    """Represents a single pipeline execution snapshot."""
    pipeline_name: str
    table_name: str
    run_id: str
    run_timestamp: datetime
    row_count: int
    processing_time_seconds: float
    null_rates: Dict[str, float] = field(default_factory=dict)       # col -> null %
    duplicate_count: int = 0
    schema_hash: Optional[str] = None
    column_names: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "table_name": self.table_name,
            "run_id": self.run_id,
            "run_timestamp": self.run_timestamp.isoformat(),
            "row_count": self.row_count,
            "processing_time_seconds": self.processing_time_seconds,
            "null_rates": json.dumps(self.null_rates),
            "duplicate_count": self.duplicate_count,
            "schema_hash": self.schema_hash,
            "column_names": json.dumps(self.column_names),
            "metadata": json.dumps(self.metadata, default=str),
        }


@dataclass
class Anomaly:
    """Represents a detected anomaly."""
    pipeline_name: str
    table_name: str
    run_id: str
    anomaly_type: AnomalyType
    severity: Severity
    message: str
    detected_at: datetime
    expected_value: Optional[float] = None
    actual_value: Optional[float] = None
    deviation_pct: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "table_name": self.table_name,
            "run_id": self.run_id,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "detected_at": self.detected_at.isoformat(),
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "deviation_pct": self.deviation_pct,
            "context": str(self.context),
        }

    def __str__(self) -> str:
        dev = f" ({self.deviation_pct:+.1f}%)" if self.deviation_pct is not None else ""
        return (
            f"[{self.severity.value}] {self.anomaly_type.value}{dev} | "
            f"{self.pipeline_name}/{self.table_name} | {self.message}"
        )
