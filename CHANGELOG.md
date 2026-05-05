# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] — 2026-05-05

### Fixed
- `_load_history_from_delta`: replaced `row.get("schema_hash")` with an explicit key-check
  guard (`row["schema_hash"] if "schema_hash" in row else None`) to avoid swallowing
  `KeyError` on older Delta tables that pre-date the column.
- `_load_history_from_delta` and `_save_run_to_delta`: replaced silent `except: pass`
  with `logger.warning(...)` so Delta I/O failures are surfaced in logs instead of
  being swallowed silently.

### Changed
- `schema_hash()` helper consolidated into `models.py` (was duplicated in `sentinel.py`
  and `detectors.py`); both files now import it from there.
- `import statistics` moved to module level in `sentinel.py` (was lazily imported inside
  `summary()`).
- Duplicate counting in `_profile_df` is now **opt-in** via `check_duplicates=False`
  (default). Previously it always triggered a second full-table scan. Pass
  `check_duplicates=True` to `PipelineSentinel.__init__` to restore the old behaviour.
- Teams webhook in `AlertManager._send_teams` now retries up to **2 times** (3 attempts
  total) with exponential back-off (1 s, 2 s) before logging an error.
- `LineageTracker.track()` now acquires a process-level `threading.Lock` before patching
  PySpark class methods, preventing race conditions when multiple notebooks share a
  Spark session on a cluster.

### Removed
- Dead `_iqr_bounds()` function removed from `detectors.py` (was defined but never called).
- Dead `_schema_hash()` function removed from `detectors.py` (superseded by the canonical
  version now in `models.py`).

### Added
- Unit tests for `AnomalyDetector` (`tests/test_detectors.py`) — 32 tests, pure Python,
  no Spark dependency required.

## [0.1.0] — 2025-04-01

### Added
- Initial release: `PipelineSentinel` orchestrator, `AnomalyDetector`, `AlertManager`,
  `LineageTracker`, and `LineageGraph`.
- Z-score based anomaly detection for row count, processing time, null rates, duplicates,
  schema drift, and empty datasets.
- Microsoft Teams adaptive-card alerting via incoming webhook.
- Delta table persistence for run history and anomaly records.
- Automatic PySpark lineage tracking via class-level method patching.

[0.1.1]: https://github.com/tanushreearora01/pipeline-sentinel/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tanushreearora01/pipeline-sentinel/releases/tag/v0.1.0
