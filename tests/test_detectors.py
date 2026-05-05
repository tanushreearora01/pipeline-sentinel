"""
Unit tests for AnomalyDetector — pure Python, no Spark required.
"""

from datetime import datetime

import pytest

from pipeline_sentinel.detectors import AnomalyDetector, _zscore, _deviation_pct, _severity_from_zscore
from pipeline_sentinel.models import AnomalyType, PipelineRun, Severity


# ── Helpers ─────────────────────────────────────────────────────────────────

def make_run(
    row_count=1000,
    processing_time_seconds=10.0,
    null_rates=None,
    duplicate_count=0,
    column_names=None,
    schema_hash=None,
    run_id="test",
):
    return PipelineRun(
        pipeline_name="test_pipeline",
        table_name="test_table",
        run_id=run_id,
        run_timestamp=datetime(2024, 1, 1),
        row_count=row_count,
        processing_time_seconds=processing_time_seconds,
        null_rates=null_rates or {},
        duplicate_count=duplicate_count,
        column_names=column_names or ["a", "b", "c"],
        schema_hash=schema_hash,
    )


def stable_history(n=10, **kwargs):
    """Return n identical runs to serve as a flat baseline."""
    return [make_run(**kwargs, run_id=str(i)) for i in range(n)]


def varied_history(n=10, center=1000, spread=50, **kwargs):
    """Return n runs whose row_count varies ±spread around center (non-zero stdev)."""
    runs = []
    for i in range(n):
        offset = spread * ((-1) ** i) * ((i % 5) + 1) / 5
        runs.append(make_run(row_count=int(center + offset), run_id=str(i), **kwargs))
    return runs


def varied_time_history(n=10, center=10.0, spread=2.0, **kwargs):
    """Return n runs whose processing_time_seconds varies around center."""
    runs = []
    for i in range(n):
        offset = spread * ((-1) ** i) * ((i % 5) + 1) / 5
        runs.append(make_run(processing_time_seconds=center + offset, run_id=str(i), **kwargs))
    return runs


def varied_dup_history(n=10, center=10, spread=3, **kwargs):
    """Return n runs whose duplicate_count varies around center."""
    runs = []
    for i in range(n):
        offset = int(spread * ((-1) ** i) * ((i % 5) + 1) / 5)
        runs.append(make_run(duplicate_count=center + offset, run_id=str(i), **kwargs))
    return runs


# ── _zscore ──────────────────────────────────────────────────────────────────

def test_zscore_returns_none_with_insufficient_data():
    assert _zscore(100, [100, 200]) is None


def test_zscore_respects_custom_min_n():
    # With min_n=2, a 2-point history should produce a score, not None.
    assert _zscore(100, [9, 11], min_n=2) is not None


def test_zscore_zero_when_all_same():
    assert _zscore(5.0, [5.0, 5.0, 5.0, 5.0]) == 0.0


def test_zscore_positive_for_high_outlier():
    z = _zscore(100, [9, 10, 11, 10, 9, 11, 10])
    assert z is not None and z > 3


def test_zscore_negative_for_low_outlier():
    z = _zscore(-80, [9, 10, 11, 10, 9, 11, 10])
    assert z is not None and z < -3


# ── _deviation_pct ────────────────────────────────────────────────────────────

def test_deviation_pct_zero_expected():
    assert _deviation_pct(5, 0) is None


def test_deviation_pct_positive():
    assert _deviation_pct(150, 100) == pytest.approx(50.0)


def test_deviation_pct_negative():
    assert _deviation_pct(50, 100) == pytest.approx(-50.0)


# ── _severity_from_zscore ────────────────────────────────────────────────────

@pytest.mark.parametrize("z,expected", [
    (1.5, Severity.LOW),
    (2.5, Severity.MEDIUM),
    (3.5, Severity.HIGH),
    (4.5, Severity.CRITICAL),
    (-4.5, Severity.CRITICAL),
])
def test_severity_from_zscore(z, expected):
    assert _severity_from_zscore(z) == expected


# ── AnomalyDetector.detect ────────────────────────────────────────────────────

class TestEmptyDataset:
    def test_flags_zero_rows(self):
        detector = AnomalyDetector()
        current = make_run(row_count=0)
        anomalies = detector.detect(current, [])
        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == AnomalyType.EMPTY_DATASET
        assert anomalies[0].severity == Severity.CRITICAL

    def test_no_false_positive_for_nonzero(self):
        detector = AnomalyDetector()
        current = make_run(row_count=1)
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.EMPTY_DATASET]
        assert anomalies == []


class TestRowCount:
    def test_drop_flagged(self):
        detector = AnomalyDetector(zscore_threshold=2.0)
        history = varied_history(n=10, center=1000, spread=50)
        current = make_run(row_count=1)  # extreme drop
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.ROW_COUNT_DROP]
        assert len(anomalies) == 1
        assert anomalies[0].actual_value == 1.0

    def test_spike_flagged(self):
        detector = AnomalyDetector(zscore_threshold=2.0)
        history = varied_history(n=10, center=1000, spread=50)
        current = make_run(row_count=100_000)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.ROW_COUNT_SPIKE]
        assert len(anomalies) == 1

    def test_no_flag_with_insufficient_history(self):
        detector = AnomalyDetector()
        history = varied_history(n=2, center=1000, spread=50)
        current = make_run(row_count=1)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type in (AnomalyType.ROW_COUNT_DROP, AnomalyType.ROW_COUNT_SPIKE)]
        assert anomalies == []

    def test_min_history_two_fires_with_two_point_history(self):
        # min_history=2 must propagate into _zscore so a 2-point baseline works.
        # Previously _zscore had a hardcoded < 3 guard that silently blocked this.
        # History must vary so stdev > 0 (identical values give sigma=0 → z=0).
        detector = AnomalyDetector(zscore_threshold=2.0, min_history=2)
        history = [make_run(row_count=900, run_id="a"), make_run(row_count=1100, run_id="b")]
        current = make_run(row_count=1)  # extreme drop — z ≈ -7
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.ROW_COUNT_DROP]
        assert len(anomalies) == 1

    def test_no_flag_within_normal_range(self):
        detector = AnomalyDetector(zscore_threshold=2.5)
        history = varied_history(n=10, center=1000, spread=50)
        current = make_run(row_count=1001)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type in (AnomalyType.ROW_COUNT_DROP, AnomalyType.ROW_COUNT_SPIKE)]
        assert anomalies == []


class TestProcessingTime:
    def test_spike_flagged(self):
        detector = AnomalyDetector(zscore_threshold=2.0)
        history = varied_time_history(n=10, center=10.0, spread=1.0)
        current = make_run(processing_time_seconds=1000.0)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.PROCESSING_TIME_SPIKE]
        assert len(anomalies) == 1

    def test_drop_not_flagged(self):
        # Only spikes are flagged for processing time, not drops
        detector = AnomalyDetector(zscore_threshold=2.0)
        history = varied_time_history(n=10, center=1000.0, spread=50.0)
        current = make_run(processing_time_seconds=1.0)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.PROCESSING_TIME_SPIKE]
        assert anomalies == []


class TestNullRates:
    def test_absolute_threshold_flagged_without_history(self):
        detector = AnomalyDetector(null_rate_threshold=0.10)
        current = make_run(null_rates={"col_a": 0.50})
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.NULL_RATE_SPIKE]
        assert len(anomalies) == 1
        assert anomalies[0].context["column"] == "col_a"

    def test_below_threshold_not_flagged(self):
        detector = AnomalyDetector(null_rate_threshold=0.10)
        current = make_run(null_rates={"col_a": 0.05})
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.NULL_RATE_SPIKE]
        assert anomalies == []

    def test_high_null_rate_severity(self):
        detector = AnomalyDetector(null_rate_threshold=0.10)
        current = make_run(null_rates={"col_a": 0.90})
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.NULL_RATE_SPIKE]
        assert anomalies[0].severity == Severity.HIGH

    def test_multiple_columns_each_checked(self):
        detector = AnomalyDetector(null_rate_threshold=0.10)
        current = make_run(null_rates={"a": 0.50, "b": 0.60, "c": 0.01})
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.NULL_RATE_SPIKE]
        flagged_cols = {a.context["column"] for a in anomalies}
        assert flagged_cols == {"a", "b"}


class TestDuplicates:
    def test_spike_flagged(self):
        detector = AnomalyDetector(zscore_threshold=2.0)
        history = varied_dup_history(n=10, center=10, spread=3)
        current = make_run(duplicate_count=50_000)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.DUPLICATE_SPIKE]
        assert len(anomalies) == 1

    def test_zero_duplicates_not_flagged(self):
        detector = AnomalyDetector()
        history = stable_history(n=10, duplicate_count=100)
        current = make_run(duplicate_count=0)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.DUPLICATE_SPIKE]
        assert anomalies == []

    def test_first_occurrence_from_zero_baseline_flagged(self):
        # All history is 0 → sigma=0 → _zscore returns 0.0 → never crosses threshold.
        # A non-zero current value must still be flagged.
        detector = AnomalyDetector()
        history = stable_history(n=10, duplicate_count=0)
        current = make_run(duplicate_count=500)
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.DUPLICATE_SPIKE]
        assert len(anomalies) == 1
        assert anomalies[0].severity == Severity.MEDIUM


class TestSchemaDrift:
    def test_added_column_flagged(self):
        detector = AnomalyDetector()
        history = stable_history(n=3, column_names=["a", "b"])
        current = make_run(column_names=["a", "b", "c"])
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert len(anomalies) == 1
        assert anomalies[0].severity == Severity.MEDIUM
        assert "c" in anomalies[0].context["added_columns"]

    def test_removed_column_flagged_as_high(self):
        detector = AnomalyDetector()
        history = stable_history(n=3, column_names=["a", "b", "c"])
        current = make_run(column_names=["a", "b"])
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert len(anomalies) == 1
        assert anomalies[0].severity == Severity.HIGH
        assert "c" in anomalies[0].context["removed_columns"]

    def test_no_drift_when_schema_unchanged(self):
        detector = AnomalyDetector()
        history = stable_history(n=3, column_names=["a", "b", "c"])
        current = make_run(column_names=["a", "b", "c"])
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert anomalies == []

    def test_no_drift_with_empty_history(self):
        detector = AnomalyDetector()
        current = make_run(column_names=["a", "b"])
        anomalies = [a for a in detector.detect(current, []) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert anomalies == []

    def test_transient_blip_in_last_run_does_not_mask_drift(self):
        # The last run had a transient extra column "x"; the majority (9 of 10)
        # had ["a", "b", "c"]. Current run reverts to ["a", "b", "c"] — no drift.
        history = stable_history(n=9, column_names=["a", "b", "c"])
        history.append(make_run(column_names=["a", "b", "c", "x"], run_id="blip"))
        current = make_run(column_names=["a", "b", "c"])
        detector = AnomalyDetector()
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert anomalies == []

    def test_drift_detected_against_majority_not_last_run(self):
        # Last run accidentally had ["a", "b"] (transient drop); majority is ["a", "b", "c"].
        # Current run also has ["a", "b"] — that IS a real removal vs the majority.
        history = stable_history(n=9, column_names=["a", "b", "c"])
        history.append(make_run(column_names=["a", "b"], run_id="blip"))
        current = make_run(column_names=["a", "b"])
        detector = AnomalyDetector()
        anomalies = [a for a in detector.detect(current, history) if a.anomaly_type == AnomalyType.SCHEMA_DRIFT]
        assert len(anomalies) == 1
        assert "c" in anomalies[0].context["removed_columns"]


class TestDetectAll:
    def test_returns_list(self):
        detector = AnomalyDetector()
        current = make_run()
        result = detector.detect(current, [])
        assert isinstance(result, list)

    def test_clean_run_no_anomalies(self):
        detector = AnomalyDetector()
        history = varied_history(n=10, center=1000, spread=50)
        current = make_run(row_count=1000)  # within normal range
        anomalies = detector.detect(current, history)
        assert anomalies == []
