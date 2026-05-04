"""
Lightweight auto-lineage tracker for PySpark pipelines.

Patches DataFrameReader/Writer at the class level during the context window
so zero instrumentation is needed in notebook code.

Usage (Spark — fully automatic):
    from pipeline_sentinel.lineage import LineageTracker

    tracker = LineageTracker(pipeline_name="sales_etl")
    with tracker.track():
        df = spark.read.table("silver.orders")
        result = transform(df)
        result.write.saveAsTable("gold.fact_sales")

    tracker.show()
    # bronze.raw_orders
    # └──► silver.orders
    #        └──► gold.fact_sales

    tracker.impact("silver.orders")   # → ["gold.fact_sales"]
    tracker.ancestors("gold.fact_sales")  # → ["silver.orders"]

Usage (Pandas / testing — manual API):
    with tracker.track():
        tracker.record_read("silver.orders")
        tracker.record_write("gold.fact_sales")

Share one graph across multiple pipelines to build a workspace-wide DAG:
    shared_graph = LineageGraph()
    tracker_a = LineageTracker("sales_etl",   graph=shared_graph)
    tracker_b = LineageTracker("kpi_pipeline", graph=shared_graph)

Known limitations:
  - spark.sql("SELECT ... FROM table") is not auto-intercepted (use record_read/write manually)
  - Class-level patching is not thread-safe; only one tracker.track() block at a time
"""

from __future__ import annotations

from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple


# ────────────────────────────────────────────────────────────────────────────
#  LineageGraph
# ────────────────────────────────────────────────────────────────────────────

class LineageGraph:
    """Directed graph of table/path dependencies.

    Nodes are table names or storage paths.
    Edges are read→write relationships recorded per pipeline run.
    """

    def __init__(self) -> None:
        self._adj:       Dict[str, Set[str]]              = defaultdict(set)
        self._radj:      Dict[str, Set[str]]              = defaultdict(set)
        self._edge_meta: Dict[Tuple[str, str], List[dict]] = defaultdict(list)

    # ── Mutation ────────────────────────────────────────────────────────── #

    def add_edge(self, source: str, target: str, meta: Optional[dict] = None) -> None:
        self._adj[source].add(target)
        self._radj[target].add(source)
        if meta:
            self._edge_meta[(source, target)].append(meta)

    # ── Queries ─────────────────────────────────────────────────────────── #

    def impact(self, table: str, max_depth: int = 10) -> List[str]:
        """All downstream tables reachable from `table` (BFS)."""
        return self._bfs(table, self._adj, max_depth)

    def ancestors(self, table: str, max_depth: int = 10) -> List[str]:
        """All upstream tables that feed into `table` (BFS)."""
        return self._bfs(table, self._radj, max_depth)

    def all_tables(self) -> Set[str]:
        return set(self._adj.keys()) | set(self._radj.keys())

    def edge_count(self) -> int:
        return sum(len(v) for v in self._adj.values())

    # ── Rendering ───────────────────────────────────────────────────────── #

    def render(self) -> str:
        """Return an ASCII tree of the full lineage graph."""
        tables = self.all_tables()
        if not tables:
            return "LineageGraph: no edges recorded yet."

        roots = sorted(set(self._adj.keys()) - set(self._radj.keys()))
        if not roots:
            roots = [sorted(tables)[0]]

        lines = [
            f"\nLineage Graph  ({len(tables)} table(s), {self.edge_count()} edge(s))",
            "─" * 55,
        ]
        visited: Set[str] = set()

        def _walk(node: str, prefix: str, is_root: bool, is_last: bool) -> None:
            if is_root:
                lines.append(node)
                child_prefix = ""
            else:
                connector = "└──► " if is_last else "├──► "
                lines.append(f"{prefix}{connector}{node}")
                child_prefix = prefix + ("       " if is_last else "│      ")

            if node in visited:
                lines[-1] += "  (already shown)"
                return
            visited.add(node)

            children = sorted(self._adj.get(node, set()))
            for i, child in enumerate(children):
                _walk(child, child_prefix, False, i == len(children) - 1)

        for i, root in enumerate(roots):
            if i > 0:
                lines.append("")
            _walk(root, "", True, True)

        return "\n".join(lines)

    # ── Serialisation ───────────────────────────────────────────────────── #

    def to_dict(self) -> dict:
        return {
            "nodes": sorted(self.all_tables()),
            "edges": [
                {"source": s, "target": t, "runs": meta}
                for (s, t), meta in self._edge_meta.items()
            ],
        }

    # ── Internal ────────────────────────────────────────────────────────── #

    @staticmethod
    def _bfs(start: str, adj: Dict[str, Set[str]], max_depth: int) -> List[str]:
        visited: Set[str] = set()
        queue: deque = deque([(start, 0)])
        result: List[str] = []
        while queue:
            node, depth = queue.popleft()
            if node in visited or depth > max_depth:
                continue
            visited.add(node)
            if node != start:
                result.append(node)
            for neighbor in adj.get(node, set()):
                queue.append((neighbor, depth + 1))
        return result


# ────────────────────────────────────────────────────────────────────────────
#  LineageTracker
# ────────────────────────────────────────────────────────────────────────────

class LineageTracker:
    """
    Records Spark read/write operations inside a ``with tracker.track()`` block
    and builds edges in a LineageGraph.

    PySpark DataFrameReader and DataFrameWriter class methods are temporarily
    patched during the block, so no changes to existing notebook code are needed.
    """

    def __init__(
        self,
        pipeline_name: str,
        graph: Optional[LineageGraph] = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.graph = graph if graph is not None else LineageGraph()
        self._current_reads:  Set[str] = set()
        self._current_writes: Set[str] = set()
        self._active = False

    # ── Context manager ─────────────────────────────────────────────────── #

    @contextmanager
    def track(self, run_id: Optional[str] = None):
        """
        Intercept PySpark reads/writes during this block and record lineage edges.
        Restores all patched methods on exit, even if an exception is raised.
        Falls back gracefully when PySpark isn't installed.

        Not reentrant: raises RuntimeError if called while a session is already
        active on this instance, preventing corrupted patches on shared Spark clusters
        where multiple notebooks may use the same session concurrently.
        """
        if self._active:
            raise RuntimeError(
                "LineageTracker.track() is not reentrant. A tracking session is "
                "already active on this instance. Use a separate LineageTracker "
                "instance per concurrent pipeline, or share a LineageGraph instead."
            )
        run_id = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._current_reads  = set()
        self._current_writes = set()
        self._active = True

        originals = self._patch_pyspark()
        try:
            yield self
        finally:
            self._restore_pyspark(originals)
            self._active = False
            self._commit(run_id)

    # ── Manual API (Pandas / testing / spark.sql notebooks) ─────────────── #

    def record_read(self, table: str) -> "LineageTracker":
        """Manually register a read — use when auto-patching can't capture it."""
        self._current_reads.add(table)
        return self

    def record_write(self, table: str) -> "LineageTracker":
        """Manually register a write — use when auto-patching can't capture it."""
        self._current_writes.add(table)
        return self

    # ── Graph queries ───────────────────────────────────────────────────── #

    def impact(self, table: str) -> List[str]:
        """Downstream tables at risk if `table` has bad data."""
        return self.graph.impact(table)

    def ancestors(self, table: str) -> List[str]:
        """Upstream tables that feed into `table`."""
        return self.graph.ancestors(table)

    # ── Display ─────────────────────────────────────────────────────────── #

    def show(self) -> str:
        """Print an ASCII tree of the lineage graph and return the string."""
        out = self.graph.render()
        print(out)
        return out

    # ── PySpark patching ────────────────────────────────────────────────── #

    def _patch_pyspark(self) -> dict:
        originals: dict = {}
        tracker = self

        try:
            from pyspark.sql import DataFrameReader, DataFrameWriter

            # DataFrameReader.table("schema.table")
            originals["reader_table"] = DataFrameReader.table

            def _reader_table(self_r, tableName, *args, **kwargs):
                tracker._current_reads.add(tableName)
                return originals["reader_table"](self_r, tableName, *args, **kwargs)

            DataFrameReader.table = _reader_table

            # DataFrameReader.load(path) — covers .format("delta").load(path)
            originals["reader_load"] = DataFrameReader.load

            def _reader_load(self_r, path=None, format=None, schema=None, **options):
                if path:
                    paths = [path] if isinstance(path, str) else list(path)
                    for p in paths:
                        tracker._current_reads.add(p)
                return originals["reader_load"](
                    self_r, path, format=format, schema=schema, **options
                )

            DataFrameReader.load = _reader_load

            # DataFrameWriter.saveAsTable("schema.table")
            originals["writer_save_as_table"] = DataFrameWriter.saveAsTable

            def _writer_save_as_table(self_w, name, format=None, mode=None, **options):
                tracker._current_writes.add(name)
                return originals["writer_save_as_table"](
                    self_w, name, format=format, mode=mode, **options
                )

            DataFrameWriter.saveAsTable = _writer_save_as_table

            # DataFrameWriter.save(path) — covers .format("delta").save(path)
            originals["writer_save"] = DataFrameWriter.save

            def _writer_save(self_w, path=None, format=None, mode=None,
                             partitionBy=None, **options):
                if path:
                    tracker._current_writes.add(path)
                return originals["writer_save"](
                    self_w, path, format=format, mode=mode,
                    partitionBy=partitionBy, **options
                )

            DataFrameWriter.save = _writer_save

            # DataFrameWriter.insertInto("schema.table")
            originals["writer_insert_into"] = DataFrameWriter.insertInto

            def _writer_insert_into(self_w, tableName, overwrite=False):
                tracker._current_writes.add(tableName)
                return originals["writer_insert_into"](self_w, tableName, overwrite=overwrite)

            DataFrameWriter.insertInto = _writer_insert_into

        except ImportError:
            pass  # PySpark not available — manual record_read/write API still works

        return originals

    def _restore_pyspark(self, originals: dict) -> None:
        try:
            from pyspark.sql import DataFrameReader, DataFrameWriter

            restore_map = {
                "reader_table":        (DataFrameReader, "table"),
                "reader_load":         (DataFrameReader, "load"),
                "writer_save_as_table":(DataFrameWriter, "saveAsTable"),
                "writer_save":         (DataFrameWriter, "save"),
                "writer_insert_into":  (DataFrameWriter, "insertInto"),
            }
            for key, (cls, attr) in restore_map.items():
                if key in originals:
                    setattr(cls, attr, originals[key])
        except ImportError:
            pass

    def _commit(self, run_id: str) -> None:
        """Build cross-product edges: every read table → every write table."""
        if not self._current_reads or not self._current_writes:
            return

        meta = {
            "pipeline":  self.pipeline_name,
            "run_id":    run_id,
            "timestamp": datetime.utcnow().isoformat(),
        }
        for src in self._current_reads:
            for tgt in self._current_writes:
                self.graph.add_edge(src, tgt, meta)

        n_edges = len(self._current_reads) * len(self._current_writes)
        print(
            f"📍 lineage [{self.pipeline_name}] — "
            f"{len(self._current_reads)} read(s) → {len(self._current_writes)} write(s) "
            f"({n_edges} edge(s) added)"
        )
