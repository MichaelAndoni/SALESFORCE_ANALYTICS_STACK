# lambda/pipe_poller/handler.py
#
# Lightweight Lambda designed to be called by the Step Functions WaitForSnowpipe
# state.  Returns a structured response the SFN Choice state can branch on.
#
# Two modes controlled by event["check_mode"]:
#
#   "ready_check"  (default, called by Step Functions in a poll loop)
#     → Queries SYSTEM$PIPE_STATUS for all configured pipes
#     → Returns { all_pipes_ready: bool, max_pending: int, pipes: [...] }
#     → Step Functions retries every 60s until all_pipes_ready = true
#
#   "health_report"  (called by the pipe monitor on its own 15-min schedule)
#     → Same as ready_check but also publishes CloudWatch metrics
#     → Used by the existing pipe monitor; this Lambda replaces it
#
# This Lambda consolidates the pipe monitor and the SFN poller into one function
# to reduce cold-start surface area and Snowflake connection overhead.

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import snowflake.connector

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

secrets_client = boto3.client("secretsmanager")
cw_client      = boto3.client("cloudwatch")

SNOWFLAKE_SECRET  = os.environ["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER    = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_ROLE    = os.environ.get("SNOWFLAKE_ROLE",    "LOADER")
SNOWFLAKE_WH      = os.environ.get("SNOWFLAKE_WAREHOUSE","LOADING_WH")
RAW_DATABASE      = os.environ.get("RAW_DATABASE",       "RAW")
RAW_SCHEMA        = os.environ.get("RAW_SCHEMA",         "SALESFORCE")
METRIC_NS         = os.environ.get("CW_METRIC_NAMESPACE","SalesforceIngest")

# All pipe names to check — must match Snowflake CREATE PIPE names
PIPE_NAMES = [
    "PIPE_ACCOUNT", "PIPE_CONTACT", "PIPE_LEAD",
    "PIPE_OPPORTUNITY", "PIPE_OPPORTUNITY_LINE_ITEM",
    "PIPE_CAMPAIGN", "PIPE_CAMPAIGN_MEMBER",
    "PIPE_TASK", "PIPE_EVENT", "PIPE_USER",
]

# A pipe with this many pending files is considered "not ready"
PENDING_THRESHOLD = 0


def _get_snowflake_conn():
    resp  = secrets_client.get_secret_value(SecretId=SNOWFLAKE_SECRET)
    creds = json.loads(resp["SecretString"])

    # Support both password and private-key auth
    connect_kwargs = dict(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WH,
        database=RAW_DATABASE,
        schema=RAW_SCHEMA,
        session_parameters={"QUERY_TAG": "pipe_poller_lambda"},
        network_timeout=10,
        login_timeout=15,
    )
    if "private_key_der" in creds:
        import base64
        connect_kwargs["private_key"] = base64.b64decode(creds["private_key_der"])
    else:
        connect_kwargs["password"] = creds["password"]

    return snowflake.connector.connect(**connect_kwargs)


def _check_one_pipe(cur, pipe_name: str) -> dict:
    """Query SYSTEM$PIPE_STATUS for a single pipe. Returns parsed status dict."""
    try:
        cur.execute(
            f"SELECT SYSTEM$PIPE_STATUS('{RAW_SCHEMA}.{pipe_name}')"
        )
        row    = cur.fetchone()
        status = json.loads(row[0])
        return {
            "pipe":           pipe_name,
            "state":          status.get("executionState", "UNKNOWN"),
            "pending":        int(status.get("pendingFileCount", 0)),
            "failed":         int(status.get("failedFileCount",  0)),
            "notification":   status.get("notificationChannelStatus", "UNKNOWN"),
            "last_ingest_ts": status.get("lastIngestedTimestamp"),
            "error":          None,
        }
    except Exception as e:
        log.error("Failed to check pipe %s: %s", pipe_name, e)
        return {
            "pipe":    pipe_name,
            "state":   "ERROR",
            "pending": -1,
            "failed":  -1,
            "error":   str(e),
        }


def _publish_metrics(pipe_results: list[dict]) -> None:
    """Publish per-pipe CloudWatch metrics."""
    metric_data = []
    now = datetime.now(tz=timezone.utc)

    for r in pipe_results:
        dims = [{"Name": "Pipe", "Value": r["pipe"]}]
        metric_data.extend([
            {
                "MetricName": "PendingFileCount",
                "Dimensions": dims,
                "Value":      max(r["pending"], 0),
                "Unit":       "Count",
                "Timestamp":  now,
            },
            {
                "MetricName": "FailedFileCount",
                "Dimensions": dims,
                "Value":      max(r["failed"], 0),
                "Unit":       "Count",
                "Timestamp":  now,
            },
            {
                "MetricName": "IsRunning",
                "Dimensions": dims,
                "Value":      1 if r["state"] == "RUNNING" else 0,
                "Unit":       "Count",
                "Timestamp":  now,
            },
        ])

    # CW accepts max 20 metrics per call
    for i in range(0, len(metric_data), 20):
        cw_client.put_metric_data(
            Namespace=METRIC_NS,
            MetricData=metric_data[i:i + 20],
        )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    check_mode = event.get("check_mode", "ready_check")
    log.info("Pipe poller invoked — mode: %s", check_mode)

    conn = _get_snowflake_conn()
    cur  = conn.cursor()

    try:
        pipe_results = [_check_one_pipe(cur, name) for name in PIPE_NAMES]
    finally:
        cur.close()
        conn.close()

    # Aggregate
    max_pending   = max((r["pending"] for r in pipe_results if r["pending"] >= 0),
                        default=0)
    any_errors    = any(r["state"] == "ERROR" for r in pipe_results)
    any_failed    = any(r["failed"] > 0 for r in pipe_results)
    all_running   = all(r["state"] == "RUNNING" for r in pipe_results
                        if r["state"] != "ERROR")
    all_ready     = (max_pending == 0 and not any_errors)

    response = {
        "all_pipes_ready": all_ready,
        "max_pending":     max_pending,
        "any_pipe_errors": any_errors,
        "any_failed_files": any_failed,
        "all_running":     all_running,
        "pipe_count":      len(pipe_results),
        "pipes":           pipe_results,
        "checked_at":      datetime.now(tz=timezone.utc).isoformat(),
    }

    log.info(
        "Pipe status: ready=%s pending=%d errors=%s failed_files=%s",
        all_ready, max_pending, any_errors, any_failed,
    )

    # Publish CW metrics for both modes (health_report and ready_check)
    try:
        _publish_metrics(pipe_results)
    except Exception as e:
        log.warning("Failed to publish CW metrics: %s", e)

    # Log individual stalled pipes for easy CloudWatch Insights queries
    for r in pipe_results:
        if r["pending"] > 0 or r["state"] not in ("RUNNING", "STOPPING"):
            log.warning(
                "PIPE_ATTENTION pipe=%s state=%s pending=%d failed=%d",
                r["pipe"], r["state"], r["pending"], r["failed"],
            )

    return response
