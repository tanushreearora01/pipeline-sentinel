# pipeline_sentinel

> Lightweight data observability for **Microsoft Fabric** and **Azure Databricks** — anomaly detection + automatic lineage tracking, zero dependencies, drop it into any notebook.

---

## What it does

**Anomaly detection** — after every pipeline run, sentinel profiles your output DataFrame and compares it against historical baselines using Z-score statistics. Bad data gets caught before it reaches downstream consumers.

**Lineage tracking** — patches PySpark's read/write operations automatically so you get a full dependency graph with zero code changes. When an anomaly fires, sentinel tells you exactly which downstream tables are at risk.

---

## Installation

### Option A — Fabric / Databricks workspace (recommended)

Upload the folder to your workspace Files, then import from any notebook:

```python
# In a Fabric or Databricks notebook cell:
import sys
sys.path.insert(0, "/lakehouse/default/Files/pipeline_sentinel")  # Fabric
# sys.path.insert(0, "/Workspace/Shared/pipeline_sentinel")       # Databricks

from pipeline_sentinel.sentinel import PipelineSentinel
from pipeline_sentinel.lineage import LineageTracker
```

### Option B — pip (once published to PyPI)

```bash
pip install pipeline-sentinel
```

### Option C — local / CI

```bash
git clone https://github.com/tanushreearora01/pipeline-sentinel
cd pipeline-sentinel
pip install -e .   # no dependencies beyond stdlib + optional pandas/pyspark
```

---

## Quick Start

### Anomaly detection only

```python
from pipeline_sentinel.sentinel import PipelineSentinel

sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="gold.fact_sales",
    alert_min_severity="MEDIUM",
)

# After your transformation:
anomalies = sentinel.record(df=output_df, processing_seconds=42.3)
```

That's it. No config files. No external services required.

### With lineage tracking

```python
from pipeline_sentinel.sentinel import PipelineSentinel
from pipeline_sentinel.lineage import LineageTracker

tracker = LineageTracker(pipeline_name="sales_etl")
sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="silver.orders",
    alert_min_severity="MEDIUM",
    lineage_tracker=tracker,        # attach tracker here
)

with tracker.track():
    raw   = spark.read.table("bronze.raw_orders")   # auto-recorded
    clean = transform(raw)
    clean.write.saveAsTable("silver.orders")        # auto-recorded
    anomalies = sentinel.record(df=clean, processing_seconds=35.1)

# If an anomaly fires, the alert automatically includes:
#   ⚠ Downstream at risk: gold.fact_sales, gold.kpi_daily
```

---

## Anomaly Detection

### Checks

| Check | Method | Description |
|---|---|---|
| **Row count drop** | Z-score | Flags unusual row count vs historical mean |
| **Row count spike** | Z-score | Flags unexpected volume increases |
| **Processing time spike** | Z-score | Catches slow jobs before SLA breach |
| **Null rate spike** | Z-score + absolute threshold | Per-column null rate monitoring |
| **Duplicate spike** | Z-score | Detects upstream deduplication failures — opt-in via `check_duplicates=True` (avoids a second full-table scan) |
| **Schema drift** | Column set diff | Added/removed columns since last run |
| **Empty dataset** | Absolute | Immediately flags 0-row outputs as CRITICAL |

### Severity levels

| Severity | Z-score range | Console colour |
|---|---|---|
| LOW | < 2.0 | Blue |
| MEDIUM | 2.0 – 2.9 | Yellow |
| HIGH | 3.0 – 3.9 | Red |
| CRITICAL | 4.0+ / always for empty datasets | Magenta |

### Alert sinks

| Sink | How to enable |
|---|---|
| Notebook console | On by default — colour-coded output |
| Microsoft Teams | Pass `teams_webhook_url` to `PipelineSentinel` |
| Delta table | Pass `delta_anomaly_path` to `PipelineSentinel` |

### Full configuration

```python
sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="gold.fact_sales",

    # Persist run history to Delta — improves detection over time
    history_table_path="abfss://container@storage.dfs.core.windows.net/sentinel/history/sales_etl",

    # Save all anomalies to a queryable Delta table
    delta_anomaly_path="abfss://container@storage.dfs.core.windows.net/sentinel/anomalies",

    # Teams channel alerts
    teams_webhook_url="https://your-org.webhook.office.com/...",

    # Severity filter: LOW | MEDIUM | HIGH | CRITICAL
    alert_min_severity="MEDIUM",

    # How many std deviations to flag (default 2.5)
    zscore_threshold=2.5,

    # Always flag columns with null rate above this (default 10%)
    null_rate_threshold=0.10,

    # How many historical runs to compare against (default 30)
    max_history=30,

    # Opt-in duplicate counting (requires a second full-table scan, default False)
    check_duplicates=False,

    # Optional lineage tracker (see Lineage section below)
    lineage_tracker=tracker,
)
```

### Usage patterns

**Simple record**

```python
anomalies = sentinel.record(
    df=output_df,
    processing_seconds=elapsed,
    run_id="run_20240501_001",   # optional — auto-generated if omitted
)
```

**Context manager — auto-times processing**

```python
with sentinel.watch(df=output_df, run_id="run_001"):
    pass

print(sentinel.last_anomalies)
```

**Pre-loading history (testing / backfill)**

```python
from pipeline_sentinel.models import PipelineRun
from datetime import datetime

history = [
    PipelineRun(
        pipeline_name="sales_etl",
        table_name="gold.fact_sales",
        run_id="run_000",
        run_timestamp=datetime(2024, 4, 30, 10, 0),
        row_count=100_000,
        processing_time_seconds=35.0,
        null_rates={"customer_id": 0.02},
        duplicate_count=10,
        column_names=["order_id", "customer_id", "amount"],
    ),
    # ... more runs
]

sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="gold.fact_sales",
    history_runs=history,
)
```

---

## Lineage Tracking

`LineageTracker` intercepts PySpark's `DataFrameReader` and `DataFrameWriter` inside a `with tracker.track():` block. No changes to your pipeline code are needed — reads and writes are captured automatically.

### How it works

```
with tracker.track():
    df = spark.read.table("bronze.raw_orders")      ← recorded as READ
    result = transform(df)
    result.write.saveAsTable("silver.orders")       ← recorded as WRITE

# Edges added: bronze.raw_orders → silver.orders
```

Intercepted operations:
- `spark.read.table(name)`
- `spark.read.format(...).load(path)`
- `df.write.saveAsTable(name)`
- `df.write.format(...).save(path)`
- `df.write.insertInto(name)`

`spark.sql(...)` is not auto-intercepted — use `tracker.record_read()` / `tracker.record_write()` manually for SQL cells.

### Visualise the graph

```python
tracker.show()
```

```
Lineage Graph  (5 table(s), 4 edge(s))
───────────────────────────────────────────────────────
bronze.raw_customers
└──► silver.orders_enriched
       ├──► gold.fact_sales
       └──► gold.kpi_daily

bronze.raw_orders
└──► silver.orders_enriched  (already shown)
```

### Impact and ancestry queries

```python
# What breaks if silver.orders_enriched has bad data?
tracker.impact("silver.orders_enriched")
# → ['gold.fact_sales', 'gold.kpi_daily']

# What feeds into gold.fact_sales?
tracker.ancestors("gold.fact_sales")
# → ['silver.orders_enriched', 'bronze.raw_orders', 'bronze.raw_customers']
```

### Workspace-wide lineage (shared graph)

Pass a single `LineageGraph` to all your pipeline trackers to build a cross-pipeline dependency map:

```python
from pipeline_sentinel.lineage import LineageGraph, LineageTracker

shared_graph = LineageGraph()

tracker_ingest  = LineageTracker("ingest_pipeline",   graph=shared_graph)
tracker_reports = LineageTracker("reports_pipeline",  graph=shared_graph)

# ... run both trackers over time ...

print(shared_graph.render())   # full workspace lineage tree
```

### Manual API (Pandas / SQL cells)

```python
with tracker.track():
    tracker.record_read("bronze.raw_orders")    # for spark.sql reads
    tracker.record_write("silver.orders")       # for spark.sql writes
```

---

## Querying the Anomaly Delta Table

```sql
-- Most recent critical / high anomalies
SELECT pipeline_name, table_name, anomaly_type, severity, message, detected_at
FROM delta.`abfss://container@storage.dfs.core.windows.net/sentinel/anomalies`
WHERE severity IN ('CRITICAL', 'HIGH')
ORDER BY detected_at DESC
LIMIT 100;

-- Row count trend for a specific pipeline
SELECT run_id, run_timestamp, row_count
FROM delta.`abfss://container@storage.dfs.core.windows.net/sentinel/history/sales_etl`
ORDER BY run_timestamp;
```

---

## Architecture

```
pipeline_sentinel/
├── sentinel.py      # PipelineSentinel — main orchestrator
├── lineage.py       # LineageTracker + LineageGraph — auto-lineage
├── detectors.py     # AnomalyDetector — Z-score logic
├── alerts.py        # AlertManager — notebook / Teams / Delta sinks
└── models.py        # PipelineRun, Anomaly dataclasses

usage_notebook.py    # Full usage example with synthetic data demo
```

---

## Zero Hard Dependencies

`pipeline_sentinel` uses only the Python standard library by default.

- PySpark support is auto-detected when running in Fabric / Databricks
- Pandas fallback works for local testing and CI
- Teams alerts use `urllib` — no `requests` needed
- Lineage patching restores all original methods on context exit, even on exception

---

## License

MIT
