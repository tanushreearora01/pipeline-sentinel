"""
Alert manager for pipeline_sentinel.
Supports: notebook logging, Delta table persistence, Teams webhook.
"""

import json
import logging
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse
from .models import Anomaly, Severity

logger = logging.getLogger("pipeline_sentinel")


def _validate_https_url(url: str) -> None:
    """Raise ValueError if url is not an HTTPS URL.

    Prevents SSRF: without this check a caller could supply an http:// URL
    pointing at the cluster's Instance Metadata Service (169.254.169.254) to
    exfiltrate managed-identity credentials, or use file:// / ftp:// schemes.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(
            f"teams_webhook_url must use HTTPS (got scheme '{parsed.scheme}'). "
            "Non-HTTPS URLs can expose pipeline metadata in cleartext and allow "
            "requests to internal network endpoints (e.g. cloud metadata services)."
        )


# ANSI colors for notebook console output
_COLORS = {
    Severity.LOW:      "\033[94m",   # blue
    Severity.MEDIUM:   "\033[93m",   # yellow
    Severity.HIGH:     "\033[91m",   # red
    Severity.CRITICAL: "\033[95m",   # magenta
}
_RESET = "\033[0m"
_BOLD  = "\033[1m"


class AlertManager:
    """
    Routes detected anomalies to one or more sinks.

    Parameters
    ----------
    teams_webhook_url : str, optional
        Incoming webhook URL for Microsoft Teams channel.
    delta_table_path : str, optional
        Delta table path to persist anomalies (e.g. 'abfss://...').
        Requires an active SparkSession named `spark`.
    log_to_notebook : bool
        Pretty-print alerts to notebook stdout (default True).
    min_severity : Severity
        Only fire alerts at or above this level (default LOW = all).
    """

    def __init__(
        self,
        teams_webhook_url: Optional[str] = None,
        delta_table_path: Optional[str] = None,
        log_to_notebook: bool = True,
        min_severity: Severity = Severity.LOW,
    ):
        if teams_webhook_url is not None:
            _validate_https_url(teams_webhook_url)
        self.teams_webhook_url = teams_webhook_url
        self.delta_table_path = delta_table_path
        self.log_to_notebook = log_to_notebook
        self.min_severity = min_severity
        self._severity_order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]

    def fire(self, anomalies: List[Anomaly]) -> None:
        """Send all anomalies to configured sinks."""
        filtered = [a for a in anomalies if self._meets_threshold(a.severity)]
        if not filtered:
            return

        if self.log_to_notebook:
            self._log_to_notebook(filtered)

        if self.teams_webhook_url:
            self._send_teams(filtered)

        if self.delta_table_path:
            self._write_delta(filtered)

    # ------------------------------------------------------------------ #
    #  Sinks                                                               #
    # ------------------------------------------------------------------ #

    def _log_to_notebook(self, anomalies: List[Anomaly]) -> None:
        print(f"\n{'━' * 60}")
        print(f"{_BOLD}🚨  PIPELINE SENTINEL  —  {len(anomalies)} anomaly(ies) detected{_RESET}")
        print(f"{'━' * 60}")
        for a in anomalies:
            color = _COLORS.get(a.severity, "")
            badge = f"{color}[{a.severity.value}]{_RESET}"
            print(f"\n  {badge}  {_BOLD}{a.anomaly_type.value}{_RESET}")
            print(f"  Pipeline : {a.pipeline_name}")
            print(f"  Table    : {a.table_name}")
            print(f"  Run ID   : {a.run_id}")
            print(f"  Message  : {a.message}")
            if a.deviation_pct is not None:
                print(f"  Deviation: {a.deviation_pct:+.1f}%")
            downstream = a.context.get("downstream_at_risk")
            if downstream:
                print(f"  ⚠ Downstream at risk: {', '.join(downstream)}")
            print(f"  At       : {a.detected_at.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"\n{'━' * 60}\n")

    def _send_teams(self, anomalies: List[Anomaly]) -> None:
        """Send an adaptive card to a Teams channel via webhook."""
        try:
            import urllib.request
            highest = max(anomalies, key=lambda a: self._severity_order.index(a.severity))
            color_map = {
                Severity.LOW: "Good",
                Severity.MEDIUM: "Warning",
                Severity.HIGH: "Attention",
                Severity.CRITICAL: "Attention",
            }
            facts = []
            for a in anomalies[:10]:   # Teams card limit
                facts.append({"title": f"[{a.severity.value}] {a.anomaly_type.value}", "value": a.message})

            card = {
                "type": "message",
                "attachments": [{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": f"🚨 Pipeline Sentinel Alert — {len(anomalies)} anomaly(ies)",
                                "weight": "Bolder",
                                "size": "Medium",
                                "color": color_map.get(highest.severity, "Attention"),
                            },
                            {
                                "type": "TextBlock",
                                "text": f"Pipeline: **{highest.pipeline_name}** | Table: **{highest.table_name}**",
                                "wrap": True,
                            },
                            {"type": "FactSet", "facts": facts},
                            {
                                "type": "TextBlock",
                                "text": f"Detected at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                                "size": "Small",
                                "isSubtle": True,
                            },
                        ],
                    },
                }],
            }

            payload = json.dumps(card).encode("utf-8")
            req = urllib.request.Request(
                self.teams_webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 202):
                    logger.warning("Teams webhook returned status %s", resp.status)
        except Exception as exc:
            logger.error("Failed to send Teams alert: %s", exc)

    def _write_delta(self, anomalies: List[Anomaly]) -> None:
        """Persist anomalies to a Delta table. Requires active SparkSession."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark is None:
                logger.warning("No active SparkSession — skipping Delta write.")
                return

            rows = [a.to_dict() for a in anomalies]
            df = spark.createDataFrame(rows)
            (
                df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(self.delta_table_path)
            )
            logger.info("Wrote %d anomaly row(s) to %s", len(rows), self.delta_table_path)
        except ImportError:
            logger.warning("PySpark not available — Delta write skipped.")
        except Exception as exc:
            logger.error("Failed to write anomalies to Delta: %s", exc)

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _meets_threshold(self, severity: Severity) -> bool:
        return self._severity_order.index(severity) >= self._severity_order.index(self.min_severity)
