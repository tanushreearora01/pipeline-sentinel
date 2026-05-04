"""pipeline_sentinel — lightweight data observability for PySpark pipelines."""

from .sentinel import PipelineSentinel
from .lineage import LineageTracker, LineageGraph
from .models import Anomaly, AnomalyType, PipelineRun, Severity

__all__ = [
    "PipelineSentinel",
    "LineageTracker",
    "LineageGraph",
    "Anomaly",
    "AnomalyType",
    "PipelineRun",
    "Severity",
]

__version__ = "0.1.0"
