# ============================================================
# pipeline_sentinel — Example Usage Notebook
# Works on: Microsoft Fabric | Azure Databricks
# ============================================================

# ── 1. Install (run once in your notebook) ──────────────────
# %pip install pipeline-sentinel   # once published to PyPI
# For now, add the pipeline_sentinel/ folder to your workspace
# and import directly.

# ── 2. Import ───────────────────────────────────────────────
from pipeline_sentinel import PipelineSentinel

# ── 3. Initialize ───────────────────────────────────────────
sentinel = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="gold.fact_sales",

    # Optional: persist run history to Delta so anomaly
    # detection improves with every pipeline run
    history_table_path="abfss://container@storage.dfs.core.windows.net/sentinel/history/sales_etl",

    # Optional: save all anomalies to a queryable Delta table
    delta_anomaly_path="abfss://container@storage.dfs.core.windows.net/sentinel/anomalies",

    # Optional: send alerts to a Teams channel
    teams_webhook_url="https://your-org.webhook.office.com/...",

    # Only alert on MEDIUM severity and above
    alert_min_severity="MEDIUM",

    # How sensitive the detector is (default 2.5 = flag 2.5 std deviations)
    zscore_threshold=2.5,

    # Flag any column with >10% nulls regardless of history
    null_rate_threshold=0.10,
)

# ── 4a. SIMPLE usage — call after your transformation ───────
# output_df = your pipeline's output DataFrame (Spark or Pandas)

anomalies = sentinel.record(
    df=output_df,
    processing_seconds=42.3,
    run_id="run_20240501_001",         # optional custom ID
)

# ── 4b. CONTEXT MANAGER — auto-measures processing time ─────
with sentinel.watch(df=output_df, run_id="run_20240501_002"):
    pass  # sentinel profiles df after the block exits

# ── 5. Inspect results ──────────────────────────────────────
for anomaly in sentinel.last_anomalies:
    print(anomaly)   # [HIGH] ROW_COUNT_DROP (-45.2%) | sales_etl/gold.fact_sales | ...

# ── 6. Print history summary ────────────────────────────────
print(sentinel.summary())

# ── 7. Query anomaly Delta table (SQL cell in Fabric/DBX) ───
# SELECT * FROM delta.`abfss://.../sentinel/anomalies`
# WHERE severity IN ('HIGH', 'CRITICAL')
# ORDER BY detected_at DESC
# LIMIT 50;


# ============================================================
# DEMO WITH SYNTHETIC DATA (no Spark needed — uses Pandas)
# ============================================================
import pandas as pd
import numpy as np
from pipeline_sentinel import PipelineSentinel
from pipeline_sentinel.models import PipelineRun
from datetime import datetime, timedelta

# Build fake history (15 normal runs)
def make_run(i, row_count, proc_time, nulls=0.01, dupes=5):
    return PipelineRun(
        pipeline_name="demo_pipeline",
        table_name="silver.orders",
        run_id=f"run_{i:03d}",
        run_timestamp=datetime.utcnow() - timedelta(hours=i),
        row_count=row_count,
        processing_time_seconds=proc_time,
        null_rates={"order_id": 0.0, "customer_id": nulls, "amount": 0.01},
        duplicate_count=dupes,
        column_names=["order_id", "customer_id", "amount", "status"],
    )

history = [make_run(i, row_count=10_000 + np.random.randint(-200, 200),
                    proc_time=30 + np.random.uniform(-2, 2)) for i in range(15, 0, -1)]

# Initialize with pre-loaded history
sentinel_demo = PipelineSentinel(
    pipeline_name="demo_pipeline",
    table_name="silver.orders",
    history_runs=history,
    alert_min_severity="LOW",
)

# Simulate an anomalous run: 60% row count drop + high nulls
bad_df = pd.DataFrame({
    "order_id":    range(4000),                           # 60% drop from 10k
    "customer_id": [None if i % 4 == 0 else i for i in range(4000)],  # 25% nulls
    "amount":      np.random.uniform(10, 500, 4000),
    "status":      ["shipped"] * 4000,
})

anomalies = sentinel_demo.record(df=bad_df, processing_seconds=28.1, run_id="bad_run_001")

print(f"\nTotal anomalies detected: {len(anomalies)}")
print(sentinel_demo.summary())


# ============================================================
# LINEAGE TRACKER — Auto-record table dependencies
# ============================================================
# On Fabric / Databricks: spark.read / df.write are intercepted
# automatically inside the `with tracker.track()` block.
# No changes to your pipeline code needed.
#
# On Pandas / local: use tracker.record_read() / record_write()
# as shown below.
# ============================================================

from pipeline_sentinel.lineage import LineageGraph, LineageTracker

# ── A. Standalone usage ─────────────────────────────────────
# Simulating a three-layer medallion pipeline (Pandas / local demo)

tracker = LineageTracker(pipeline_name="sales_etl")

with tracker.track(run_id="run_001"):
    # Auto-intercepted on Spark. Manual API for local testing:
    tracker.record_read("bronze.raw_orders")
    tracker.record_read("bronze.raw_customers")
    tracker.record_write("silver.orders_enriched")

with tracker.track(run_id="run_002"):
    tracker.record_read("silver.orders_enriched")
    tracker.record_write("gold.fact_sales")

with tracker.track(run_id="run_003"):
    tracker.record_read("silver.orders_enriched")
    tracker.record_write("gold.kpi_daily")

# Print the full lineage graph as an ASCII tree
tracker.show()
# Expected output:
#
# Lineage Graph  (5 table(s), 4 edge(s))
# ───────────────────────────────────────────────────────────
# bronze.raw_customers
# └──► silver.orders_enriched
#        ├──► gold.fact_sales
#        └──► gold.kpi_daily
#
# bronze.raw_orders
# └──► silver.orders_enriched  (already shown)

# ── B. Impact query ─────────────────────────────────────────
# "If silver.orders_enriched has bad data, what's at risk?"
at_risk = tracker.impact("silver.orders_enriched")
print(f"\nDownstream at risk: {at_risk}")
# → ['gold.fact_sales', 'gold.kpi_daily']

upstream = tracker.ancestors("gold.fact_sales")
print(f"Upstream sources : {upstream}")
# → ['silver.orders_enriched', 'bronze.raw_orders', 'bronze.raw_customers']

# ── C. Shared graph across pipelines ────────────────────────
# Build a workspace-wide lineage DAG by passing one shared graph
# to all your pipeline trackers.

shared_graph = LineageGraph()

tracker_ingest  = LineageTracker("ingest_pipeline",  graph=shared_graph)
tracker_reports = LineageTracker("reports_pipeline", graph=shared_graph)

with tracker_ingest.track():
    tracker_ingest.record_read("adls://raw/events")
    tracker_ingest.record_write("bronze.events")

with tracker_reports.track():
    tracker_reports.record_read("bronze.events")
    tracker_reports.record_write("gold.executive_report")

print(shared_graph.render())

# ── D. Integrated with PipelineSentinel ─────────────────────
# When an anomaly fires, sentinel automatically appends
# the downstream impact radius to each anomaly's context.

sentinel_with_lineage = PipelineSentinel(
    pipeline_name="sales_etl",
    table_name="silver.orders_enriched",
    history_runs=history,         # reuse history from demo above
    alert_min_severity="LOW",
    lineage_tracker=tracker,      # attach the tracker built above
)

# Anomalous run — alert output will include:
#   ⚠ Downstream at risk: gold.fact_sales, gold.kpi_daily
anomalies = sentinel_with_lineage.record(df=bad_df, processing_seconds=28.1)

# Inspect downstream impact directly from the anomaly object
for a in anomalies:
    if a.context.get("downstream_at_risk"):
        print(f"\n[{a.severity.value}] {a.anomaly_type.value}")
        print(f"  Downstream at risk: {a.context['downstream_at_risk']}")

# ── E. On real Spark (Fabric / Databricks) ──────────────────
# Exact same API — reads/writes are captured automatically:
#
# tracker = LineageTracker(pipeline_name="sales_etl")
# with tracker.track():
#     raw_df   = spark.read.table("bronze.raw_orders")       # auto-recorded
#     clean_df = transform(raw_df)
#     clean_df.write.saveAsTable("silver.orders_enriched")   # auto-recorded
#
# tracker.show()    # prints ASCII tree
# tracker.impact("bronze.raw_orders")   # → ["silver.orders_enriched", ...]
