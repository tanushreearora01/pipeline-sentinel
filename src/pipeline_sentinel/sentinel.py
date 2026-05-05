"""
PipelineSentinel — main orchestrator.

Usage (Fabric / Databricks notebook):
---------------------------------------
from pipeline_sentinel import PipelineSentinel

sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="gold.fact_sales",
    history_table_path="abfss://...",         # optional Delta path for history
    teams_webhook_url="https://...",           # optional Teams alert
    alert_min_severity="MEDIUM",
)

# After your transformation is done:
with sentinel.watch(run_id="run_20240501_001", df=output_df, processing_seconds=42.3):
    pass   # sentinel auto-profiles df and checks for anomalies

# Or call manually:
sentinel.record(df=output_df, processing_seconds=42.3, run_id="run_001")
"""

import logging
import statistics
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

from .alerts import AlertManager
from .detectors import AnomalyDetector
from .models import Anomaly, PipelineRun, Severity, schema_hash

logger = logging.getLogger("pipeline_sentinel")

if TYPE_CHECKING:
    from .lineage import LineageTracker


def _profile_df(df, check_duplicates: bool = False) -> Dict:
    """
    Profile a Spark or Pandas DataFrame.
    Returns dict: row_count, null_rates, duplicate_count, column_names.
    Duplicate counting requires a second full table scan; pass check_duplicates=True to enable it.
    """
    try:
        # Try PySpark first
        from pyspark.sql import DataFrame as SparkDF
        import pyspark.sql.functions as F

        if isinstance(df, SparkDF):
            cols = df.columns
            row_count = df.count()

            null_exprs = [
                F.mean(F.col(c).isNull().cast("int")).alias(c) for c in cols
            ]
            null_row = df.select(null_exprs).collect()[0]
            null_rates = {c: float(null_row[c] or 0.0) for c in cols}

            dup_count = (row_count - df.dropDuplicates().count()) if check_duplicates else 0

            return {
                "row_count": row_count,
                "null_rates": null_rates,
                "duplicate_count": dup_count,
                "column_names": cols,
            }
    except (ImportError, Exception):
        pass

    # Fallback: Pandas
    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            cols = df.columns.tolist()
            row_count = len(df)
            null_rates = (df.isnull().mean()).to_dict()
            dup_count = int(df.duplicated().sum()) if check_duplicates else 0
            return {
                "row_count": row_count,
                "null_rates": null_rates,
                "duplicate_count": dup_count,
                "column_names": cols,
            }
    except ImportError:
        pass

    raise TypeError("df must be a PySpark or Pandas DataFrame.")


class PipelineSentinel:
    """
    Main entry point for pipeline anomaly detection.

    Parameters
    ----------
    pipeline_name : str
        Logical name of your pipeline (e.g. 'sales_etl').
    table_name : str
        Target table being written (e.g. 'gold.fact_sales').
    history_runs : list[PipelineRun], optional
        In-memory history. Use this for testing or if you manage state yourself.
    history_table_path : str, optional
        Delta table path to load/save historical run metrics automatically.
    max_history : int
        Maximum number of historical runs to compare against (default 30).
    teams_webhook_url : str, optional
        Teams incoming webhook URL.
    alert_min_severity : str
        Minimum severity to trigger alerts: 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'.
    zscore_threshold : float
        Z-score cutoff to flag anomalies (default 2.5).
    null_rate_threshold : float
        Absolute null rate (0–1) to always flag (default 0.10).
    """

    def __init__(
        self,
        pipeline_name: str,
        table_name: str,
        history_runs: Optional[List[PipelineRun]] = None,
        history_table_path: Optional[str] = None,
        delta_anomaly_path: Optional[str] = None,
        max_history: int = 30,
        teams_webhook_url: Optional[str] = None,
        alert_min_severity: str = "MEDIUM",
        zscore_threshold: float = 2.5,
        null_rate_threshold: float = 0.10,
        check_duplicates: bool = False,
        lineage_tracker: Optional["LineageTracker"] = None,
    ):
        self.pipeline_name = pipeline_name
        self.table_name = table_name
        self.history_table_path = history_table_path
        self.delta_anomaly_path = delta_anomaly_path
        self.max_history = max_history

        self.check_duplicates = check_duplicates
        self.lineage_tracker = lineage_tracker

        self._history: List[PipelineRun] = list(history_runs or [])
        if history_table_path:
            self._load_history_from_delta()

        self.detector = AnomalyDetector(
            zscore_threshold=zscore_threshold,
            null_rate_threshold=null_rate_threshold,
        )
        self.alert_manager = AlertManager(
            teams_webhook_url=teams_webhook_url,
            delta_table_path=delta_anomaly_path,
            log_to_notebook=True,
            min_severity=Severity(alert_min_severity.upper()),
        )

        self._last_anomalies: List[Anomaly] = []

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def record(
        self,
        df,
        processing_seconds: float,
        run_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> List[Anomaly]:
        """
        Profile `df`, run anomaly detection, fire alerts, and persist the run.

        Returns the list of detected Anomaly objects.
        """
        run_id = run_id or str(uuid.uuid4())[:8]
        profile = _profile_df(df, check_duplicates=self.check_duplicates)

        current_run = PipelineRun(
            pipeline_name=self.pipeline_name,
            table_name=self.table_name,
            run_id=run_id,
            run_timestamp=datetime.now(timezone.utc),
            row_count=profile["row_count"],
            processing_time_seconds=processing_seconds,
            null_rates=profile["null_rates"],
            duplicate_count=profile["duplicate_count"],
            column_names=list(profile["column_names"]),
            schema_hash=schema_hash(profile["column_names"]),
            metadata=metadata or {},
        )

        anomalies = self.detector.detect(current_run, self._history[-self.max_history:])
        self._last_anomalies = anomalies

        if anomalies and self.lineage_tracker:
            downstream = self.lineage_tracker.impact(self.table_name)
            if downstream:
                for a in anomalies:
                    a.context["downstream_at_risk"] = downstream

        if anomalies:
            self.alert_manager.fire(anomalies)
        else:
            print(f"✅  pipeline_sentinel: No anomalies detected for run [{run_id}].")

        # Store run in history
        self._history.append(current_run)
        if self.history_table_path:
            self._save_run_to_delta(current_run)

        return anomalies

    @contextmanager
    def watch(self, df, run_id: Optional[str] = None, metadata: Optional[Dict] = None):
        """
        Context manager that also measures wall-clock processing time.

        Usage:
            with sentinel.watch(df=output_df) as ctx:
                pass
            print(ctx.anomalies)
        """
        start = time.time()
        yield self
        elapsed = time.time() - start
        self.record(df=df, processing_seconds=elapsed, run_id=run_id, metadata=metadata)

    @property
    def last_anomalies(self) -> List[Anomaly]:
        """Returns anomalies from the most recent record() call."""
        return self._last_anomalies

    @property
    def history(self) -> List[PipelineRun]:
        return list(self._history)

    def summary(self) -> str:
        """Print a summary of historical run metrics."""
        if not self._history:
            return "No historical runs recorded yet."

        lines = [f"\n{'═' * 55}"]
        lines.append(f"  Pipeline Sentinel — {self.pipeline_name} / {self.table_name}")
        lines.append(f"{'═' * 55}")
        lines.append(f"  Total runs tracked : {len(self._history)}")

        row_counts = [r.row_count for r in self._history]
        times      = [r.processing_time_seconds for r in self._history]
        lines.append(f"  Row count  avg/min/max : {statistics.mean(row_counts):,.0f} / {min(row_counts):,} / {max(row_counts):,}")
        lines.append(f"  Proc time  avg/min/max : {statistics.mean(times):.1f}s / {min(times):.1f}s / {max(times):.1f}s")
        lines.append(f"{'═' * 55}\n")
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    #  Delta persistence helpers                                           #
    # ------------------------------------------------------------------ #

    def _load_history_from_delta(self):
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                return
            df = spark.read.format("delta").load(self.history_table_path)
            rows = df.orderBy("run_timestamp", ascending=False).limit(self.max_history).collect()
            for row in reversed(rows):
                import json as _json
                self._history.append(PipelineRun(
                    pipeline_name=row["pipeline_name"],
                    table_name=row["table_name"],
                    run_id=row["run_id"],
                    run_timestamp=datetime.fromisoformat(row["run_timestamp"]),
                    row_count=int(row["row_count"]),
                    processing_time_seconds=float(row["processing_time_seconds"]),
                    null_rates=_json.loads(row["null_rates"]),
                    duplicate_count=int(row["duplicate_count"]),
                    schema_hash=row["schema_hash"] if "schema_hash" in row else None,
                    column_names=_json.loads(row["column_names"]),
                ))
        except Exception as exc:
            logger.warning("Could not load history from Delta (table may not exist yet): %s", exc)

    def _save_run_to_delta(self, run: PipelineRun):
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                return
            df = spark.createDataFrame([run.to_dict()])
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(self.history_table_path)
        except Exception as exc:
            logger.warning("Could not save run to Delta: %s", exc)
