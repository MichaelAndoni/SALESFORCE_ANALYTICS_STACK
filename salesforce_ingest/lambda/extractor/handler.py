# lambda/extractor/handler.py
#
# Main Lambda entry point for the Salesforce incremental extractor.
#
# Invocation modes:
#   1. Scheduled (EventBridge rule) — extracts all configured objects
#   2. Single-object (manual / Step Functions) — event["objects"] overrides
#   3. Backfill (manual) — event["backfill_start"] overrides the watermark
#
# Event shape (all fields optional):
# {
#   "objects":        ["Account", "Opportunity"],   # subset; default = all
#   "backfill_start": "2025-01-01T00:00:00Z",       # override watermark (backfill)
#   "dry_run":        true                           # log SOQL but don't write to S3
# }
#
# Environment variables (all injected by CDK):
#   SALESFORCE_SECRET_NAME     — Secrets Manager secret containing SF creds
#   SSM_WATERMARK_PREFIX       — SSM prefix, e.g. /salesforce-ingest/prod/watermarks
#   S3_BUCKET                  — landing bucket name
#   S3_PREFIX                  — key prefix, e.g. salesforce/raw
#   SF_INSTANCE_URL            — https://yourorg.my.salesforce.com
#   SF_USERNAME                — service account username
#   SF_CLIENT_ID               — Connected App consumer key
#   SF_SANDBOX                 — "true" / "false"
#   LOOKBACK_MINUTES           — watermark lookback buffer (default 30)
#   OBJECTS                    — comma-separated object list to extract

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import boto3

from salesforce_client import SalesforceClient, SalesforceBulkError, OBJECT_QUERIES
from watermark import WatermarkManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS clients (initialized at cold-start for reuse across warm invocations)
# ---------------------------------------------------------------------------
secrets_client = boto3.client("secretsmanager")
ssm_client     = boto3.client("ssm")
s3_client      = boto3.client("s3")

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
SECRET_NAME       = os.environ["SALESFORCE_SECRET_NAME"]
SSM_PREFIX        = os.environ["SSM_WATERMARK_PREFIX"]
S3_BUCKET         = os.environ["S3_BUCKET"]
S3_PREFIX         = os.environ.get("S3_PREFIX", "salesforce/raw")
INSTANCE_URL      = os.environ["SF_INSTANCE_URL"]
SF_USERNAME       = os.environ["SF_USERNAME"]
SF_CLIENT_ID      = os.environ["SF_CLIENT_ID"]
SF_SANDBOX        = os.environ.get("SF_SANDBOX", "false").lower() == "true"
LOOKBACK_MINUTES  = int(os.environ.get("LOOKBACK_MINUTES", "30"))

# Default object list — can be overridden per-invocation via event["objects"]
DEFAULT_OBJECTS: list[str] = [
    o.strip()
    for o in os.environ.get("OBJECTS", ",".join(OBJECT_QUERIES.keys())).split(",")
    if o.strip()
]


def _load_sf_credentials() -> str:
    """
    Fetch the PEM private key from Secrets Manager.

    Secret JSON format:
    {
      "private_key_pem": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----"
    }
    """
    resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(resp["SecretString"])
    return secret["private_key_pem"]


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda handler.

    Returns a summary dict with extraction results for each object,
    which can be consumed by Step Functions or logged to CloudWatch.
    """
    run_ts = datetime.now(tz=timezone.utc)
    log.info("=== Salesforce extract run started at %s ===", run_ts.isoformat())
    log.info("Event: %s", json.dumps(event))

    # ── Parse invocation parameters ──────────────────────────────────────
    objects_to_run: list[str] = event.get("objects", DEFAULT_OBJECTS)
    backfill_start: str | None = event.get("backfill_start")
    dry_run: bool = event.get("dry_run", False)

    if dry_run:
        log.warning("DRY RUN — no S3 writes or watermark updates will occur")

    # ── Initialise clients ────────────────────────────────────────────────
    private_key_pem = _load_sf_credentials()
    wm_manager      = WatermarkManager(ssm_client, SSM_PREFIX)

    sf_client = SalesforceClient(
        instance_url=INSTANCE_URL,
        client_id=SF_CLIENT_ID,
        username=SF_USERNAME,
        private_key_pem=private_key_pem,
        sandbox=SF_SANDBOX,
    )
    sf_client.authenticate()

    # ── Extract each object ───────────────────────────────────────────────
    results: dict[str, Any] = {}
    MAX_RETRIES   = 2
    RETRY_BACKOFF = [30, 90]   # seconds to wait before retry 1, retry 2

    for sf_object in objects_to_run:
        obj_result: dict[str, Any] = {
            "status":         "pending",
            "records":        0,
            "watermark_used": None,
            "attempts":       0,
            "error":          None,
        }

        # Resolve watermark once — shared across retry attempts
        if backfill_start:
            wm = datetime.fromisoformat(backfill_start.replace("Z", "+00:00"))
            log.info("[%s] Backfill override: %s", sf_object, wm.isoformat())
        else:
            wm = wm_manager.get(sf_object)
            wm = wm_manager.apply_lookback(wm, LOOKBACK_MINUTES)

        obj_result["watermark_used"] = wm.isoformat()

        if dry_run:
            log.info("[%s] DRY RUN — would extract from %s", sf_object, wm.isoformat())
            obj_result["status"] = "dry_run"
            results[sf_object]   = obj_result
            continue

        for attempt in range(MAX_RETRIES + 1):
            obj_result["attempts"] = attempt + 1
            try:
                record_count = sf_client.extract_object(
                    sf_object=sf_object,
                    watermark=wm,
                    s3_client=s3_client,
                    s3_bucket=S3_BUCKET,
                    s3_prefix=S3_PREFIX,
                    run_ts=run_ts,
                )

                # Advance watermark only on success and not in backfill mode
                if not backfill_start:
                    wm_manager.set(sf_object, run_ts)

                obj_result["status"]  = "success"
                obj_result["records"] = record_count
                break   # success — stop retrying

            except SalesforceBulkError as e:
                if "[400]" in str(e):
                    # Bad SOQL — retrying will never fix it; fail immediately
                    log.error("[%s] Bad SOQL (400), will not retry: %s", sf_object, e)
                    obj_result["status"] = "error"
                    obj_result["error"]  = str(e)
                    break
                if attempt < MAX_RETRIES:
                    delay = RETRY_BACKOFF[attempt]
                    log.warning("[%s] Attempt %d failed, retrying in %ds: %s",
                                sf_object, attempt + 1, delay, e)
                    time.sleep(delay)
                else:
                    log.error("[%s] FAILED after %d attempts: %s",
                              sf_object, attempt + 1, e)
                    log.error(traceback.format_exc())
                    obj_result["status"] = "error"
                    obj_result["error"]  = str(e)

            except Exception as e:
                if attempt < MAX_RETRIES:
                    delay = RETRY_BACKOFF[attempt]
                    log.warning("[%s] Attempt %d failed, retrying in %ds: %s",
                                sf_object, attempt + 1, delay, e)
                    time.sleep(delay)
                else:
                    log.error("[%s] FAILED after %d attempts: %s",
                              sf_object, attempt + 1, e)
                    log.error(traceback.format_exc())
                    obj_result["status"] = "error"
                    obj_result["error"]  = str(e)

        results[sf_object] = obj_result

    # ── Summary ───────────────────────────────────────────────────────────
    total_records = sum(r.get("records", 0) for r in results.values())
    failures      = [o for o, r in results.items() if r["status"] == "error"]

    summary = {
        "run_timestamp":  run_ts.isoformat(),
        "objects_run":    len(objects_to_run),
        "total_records":  total_records,
        "failures":       failures,
        "results":        results,
    }

    log.info("=== Run complete. Total records: %d. Failures: %s ===",
             total_records, failures or "none")

    # Log a clear ERROR line for any failures so a CloudWatch metric filter
    # can trigger an alarm — but do NOT raise, which would cause Lambda to
    # retry the entire function and re-run objects that already succeeded.
    if failures:
        log.error("EXTRACT_FAILURES objects=%s", failures)

    return summary
