"""
Anomaly detection engine for pipeline_sentinel.
Uses Z-score statistical methods to flag anomalies.
"""

import statistics
from datetime import datetime, timezone
from typing import List, Optional
from .models import PipelineRun, Anomaly, AnomalyType, Severity


def _zscore(value: float, history: List[float]) -> Optional[float]:
    """Return Z-score of value against history, or None if insufficient data."""
    if len(history) < 3:
        return None
    mu = statistics.mean(history)
    sigma = statistics.stdev(history)
    if sigma == 0:
        return 0.0
    return (value - mu) / sigma


def _deviation_pct(actual: float, expected: float) -> Optional[float]:
    if expected == 0:
        return None
    return ((actual - expected) / abs(expected)) * 100


def _severity_from_zscore(z: float) -> Severity:
    az = abs(z)
    if az >= 4.0:
        return Severity.CRITICAL
    if az >= 3.0:
        return Severity.HIGH
    if az >= 2.0:
        return Severity.MEDIUM
    return Severity.LOW


class AnomalyDetector:
    """
    Stateless anomaly detector. Pass in historical runs and the current run
    and it returns a list of detected Anomaly objects.

    Methods
    -------
    detect(current_run, history) -> List[Anomaly]
        Run all checks and return anomalies found.
    """

    def __init__(
        self,
        zscore_threshold: float = 2.5,
        null_rate_threshold: float = 0.10,   # absolute null rate to flag even without history
        min_history: int = 3,
    ):
        self.zscore_threshold = zscore_threshold
        self.null_rate_threshold = null_rate_threshold
        self.min_history = min_history

    def detect(
        self, current: PipelineRun, history: List[PipelineRun]
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []
        now = datetime.now(timezone.utc)

        anomalies += self._check_row_count(current, history, now)
        anomalies += self._check_processing_time(current, history, now)
        anomalies += self._check_null_rates(current, history, now)
        anomalies += self._check_duplicates(current, history, now)
        anomalies += self._check_schema_drift(current, history, now)
        anomalies += self._check_empty_dataset(current, now)

        return anomalies

    # ------------------------------------------------------------------ #
    #  Individual checks                                                   #
    # ------------------------------------------------------------------ #

    def _check_row_count(self, cur, hist, now) -> List[Anomaly]:
        results = []
        row_counts = [r.row_count for r in hist]

        if len(row_counts) >= self.min_history:
            z = _zscore(cur.row_count, row_counts)
            expected = statistics.mean(row_counts)
            dev = _deviation_pct(cur.row_count, expected)

            if z is not None and abs(z) >= self.zscore_threshold:
                atype = (
                    AnomalyType.ROW_COUNT_DROP
                    if cur.row_count < expected
                    else AnomalyType.ROW_COUNT_SPIKE
                )
                severity = _severity_from_zscore(z)
                results.append(Anomaly(
                    pipeline_name=cur.pipeline_name,
                    table_name=cur.table_name,
                    run_id=cur.run_id,
                    anomaly_type=atype,
                    severity=severity,
                    message=(
                        f"Row count {cur.row_count:,} deviates from historical mean "
                        f"{expected:,.0f} (Z={z:.2f})"
                    ),
                    detected_at=now,
                    expected_value=expected,
                    actual_value=float(cur.row_count),
                    deviation_pct=dev,
                ))
        return results

    def _check_processing_time(self, cur, hist, now) -> List[Anomaly]:
        results = []
        times = [r.processing_time_seconds for r in hist]

        if len(times) >= self.min_history:
            z = _zscore(cur.processing_time_seconds, times)
            expected = statistics.mean(times)
            dev = _deviation_pct(cur.processing_time_seconds, expected)

            if z is not None and z >= self.zscore_threshold:   # only flag spikes (not drops)
                results.append(Anomaly(
                    pipeline_name=cur.pipeline_name,
                    table_name=cur.table_name,
                    run_id=cur.run_id,
                    anomaly_type=AnomalyType.PROCESSING_TIME_SPIKE,
                    severity=_severity_from_zscore(z),
                    message=(
                        f"Processing time {cur.processing_time_seconds:.1f}s is abnormally "
                        f"high vs historical mean {expected:.1f}s (Z={z:.2f})"
                    ),
                    detected_at=now,
                    expected_value=expected,
                    actual_value=cur.processing_time_seconds,
                    deviation_pct=dev,
                ))
        return results

    def _check_null_rates(self, cur, hist, now) -> List[Anomaly]:
        results = []

        for col, null_rate in cur.null_rates.items():
            # Always flag high absolute null rate
            if null_rate >= self.null_rate_threshold:
                historical_rates = [
                    r.null_rates.get(col, 0.0) for r in hist if col in r.null_rates
                ]
                z = _zscore(null_rate, historical_rates) if len(historical_rates) >= self.min_history else None
                severity = _severity_from_zscore(z) if z is not None else (
                    Severity.HIGH if null_rate >= 0.3 else Severity.MEDIUM
                )
                dev = _deviation_pct(null_rate, statistics.mean(historical_rates)) if historical_rates else None

                results.append(Anomaly(
                    pipeline_name=cur.pipeline_name,
                    table_name=cur.table_name,
                    run_id=cur.run_id,
                    anomaly_type=AnomalyType.NULL_RATE_SPIKE,
                    severity=severity,
                    message=(
                        f"Column '{col}' null rate {null_rate:.1%} exceeds threshold "
                        f"{self.null_rate_threshold:.1%}"
                        + (f" (Z={z:.2f})" if z is not None else "")
                    ),
                    detected_at=now,
                    expected_value=statistics.mean(historical_rates) if historical_rates else None,
                    actual_value=null_rate,
                    deviation_pct=dev,
                    context={"column": col},
                ))
        return results

    def _check_duplicates(self, cur, hist, now) -> List[Anomaly]:
        results = []
        dup_counts = [r.duplicate_count for r in hist]

        if len(dup_counts) >= self.min_history and cur.duplicate_count > 0:
            z = _zscore(cur.duplicate_count, dup_counts)
            expected = statistics.mean(dup_counts)
            dev = _deviation_pct(cur.duplicate_count, expected)

            if z is not None and z >= self.zscore_threshold:
                results.append(Anomaly(
                    pipeline_name=cur.pipeline_name,
                    table_name=cur.table_name,
                    run_id=cur.run_id,
                    anomaly_type=AnomalyType.DUPLICATE_SPIKE,
                    severity=_severity_from_zscore(z),
                    message=(
                        f"Duplicate count {cur.duplicate_count:,} is abnormally high "
                        f"vs mean {expected:.0f} (Z={z:.2f})"
                    ),
                    detected_at=now,
                    expected_value=expected,
                    actual_value=float(cur.duplicate_count),
                    deviation_pct=dev,
                ))
        return results

    def _check_schema_drift(self, cur, hist, now) -> List[Anomaly]:
        results = []
        if not cur.column_names or not hist:
            return results

        last_run = hist[-1]
        if not last_run.column_names:
            return results

        prev_cols = set(last_run.column_names)
        curr_cols = set(cur.column_names)
        added = curr_cols - prev_cols
        removed = prev_cols - curr_cols

        if added or removed:
            parts = []
            if added:
                parts.append(f"Added: {sorted(added)}")
            if removed:
                parts.append(f"Removed: {sorted(removed)}")

            results.append(Anomaly(
                pipeline_name=cur.pipeline_name,
                table_name=cur.table_name,
                run_id=cur.run_id,
                anomaly_type=AnomalyType.SCHEMA_DRIFT,
                severity=Severity.HIGH if removed else Severity.MEDIUM,
                message=f"Schema changed since last run. {' | '.join(parts)}",
                detected_at=now,
                context={"added_columns": list(added), "removed_columns": list(removed)},
            ))
        return results

    def _check_empty_dataset(self, cur, now) -> List[Anomaly]:
        if cur.row_count == 0:
            return [Anomaly(
                pipeline_name=cur.pipeline_name,
                table_name=cur.table_name,
                run_id=cur.run_id,
                anomaly_type=AnomalyType.EMPTY_DATASET,
                severity=Severity.CRITICAL,
                message="Pipeline produced 0 rows — dataset is completely empty.",
                detected_at=now,
                actual_value=0.0,
            )]
        return []
