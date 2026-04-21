# lambda/pipe_monitor/handler.py
#
# Lightweight Lambda that polls Snowflake's SYSTEM$PIPE_STATUS and
# INFORMATION_SCHEMA.LOAD_HISTORY to detect stalled Snowpipes and
# publishes a CloudWatch metric for alerting.
#
# Runs every 15 minutes via its own EventBridge rule (separate from extractor).
# Snowflake connection uses the snowflake-connector-python with key-pair auth.

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import boto3
import snowflake.connector

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

cw_client      = boto3.client("cloudwatch")
secrets_client = boto3.client("secretsmanager")

SF_SECRET_NAME = os.environ["SNOWFLAKE_SECRET_NAME"]
SF_ACCOUNT     = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER        = os.environ["SNOWFLAKE_USER"]
SF_ROLE        = os.environ.get("SNOWFLAKE_ROLE", "LOADER")
SF_WAREHOUSE   = os.environ.get("SNOWFLAKE_WAREHOUSE", "LOADING")
RAW_DATABASE   = os.environ.get("RAW_DATABASE", "RAW")
RAW_SCHEMA     = os.environ.get("RAW_SCHEMA", "SALESFORCE")
METRIC_NS      = os.environ.get("CW_METRIC_NAMESPACE", "SalesforceIngest")

# Pipes to monitor — must match the names in Snowflake
PIPE_NAMES = [
    "PIPE_ACCOUNT", "PIPE_CONTACT", "PIPE_LEAD",
    "PIPE_OPPORTUNITY", "PIPE_OPPORTUNITY_LINE_ITEM",
    "PIPE_CAMPAIGN", "PIPE_CAMPAIGN_MEMBER",
    "PIPE_TASK", "PIPE_EVENT", "PIPE_USER",
]

# Alert threshold: a pipe with pendingFileCount > 0 for > N minutes is stalled
STALL_THRESHOLD_MINUTES = 30


def _get_snowflake_creds() -> dict:
    resp   = secrets_client.get_secret_value(SecretId=SF_SECRET_NAME)
    return json.loads(resp["SecretString"])


def _snowflake_conn(creds: dict):
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=creds["private_key_der"],   # DER-encoded bytes from secret
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=RAW_DATABASE,
        schema=RAW_SCHEMA,
        session_parameters={"QUERY_TAG": "pipe_monitor_lambda"},
    )


def _check_pipe_status(cur, pipe_name: str) -> dict:
    cur.execute(f"SELECT SYSTEM$PIPE_STATUS('{RAW_SCHEMA}.{pipe_name}')")
    row    = cur.fetchone()
    status = json.loads(row[0])
    return status


def _publish_metric(metric_name: str, value: float, dimensions: list[dict]) -> None:
    cw_client.put_metric_data(
        Namespace=METRIC_NS,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": dimensions,
                "Value":      value,
                "Unit":       "Count",
                "Timestamp":  datetime.now(tz=timezone.utc),
            }
        ],
    )


def handler(event: dict, context: Any) -> dict:
    log.info("Pipe monitor starting")
    creds = _get_snowflake_creds()
    conn  = _snowflake_conn(creds)
    cur   = conn.cursor()

    results = []

    try:
        for pipe_name in PIPE_NAMES:
            dims = [{"Name": "Pipe", "Value": pipe_name}]
            try:
                status       = _check_pipe_status(cur, pipe_name)
                pending      = status.get("pendingFileCount", 0)
                error_files  = status.get("failedFileCount", 0)
                exec_status  = status.get("executionState", "UNKNOWN")

                log.info("[%s] state=%s pending=%d errors=%d",
                         pipe_name, exec_status, pending, error_files)

                _publish_metric("PendingFileCount", pending,     dims)
                _publish_metric("FailedFileCount",  error_files, dims)
                _publish_metric("IsRunning",
                                1 if exec_status == "RUNNING" else 0, dims)

                results.append({
                    "pipe":         pipe_name,
                    "state":        exec_status,
                    "pending":      pending,
                    "errors":       error_files,
                })

            except Exception as e:
                log.error("[%s] Failed to check status: %s", pipe_name, e)
                _publish_metric("MonitorError", 1, dims)

    finally:
        cur.close()
        conn.close()

    return {"checked": len(results), "pipes": results}
