"""
glue/salesforce_bulk_extract.py

AWS Glue 4.0 (Python Shell / PySpark) job for large-volume Salesforce extracts.

When to use Glue instead of Lambda:
  Lambda:  hourly incremental pulls, < ~5M records per object per run, < 15 min
  Glue:    initial full historical backfill, very large objects (Opportunity on
           multi-year orgs can be 10M+ rows), parallel multi-object extraction,
           or when you want Spark for downstream transformations in the same job.

This job runs as a Python Shell job (not Spark) because:
  • Salesforce Bulk API is the bottleneck, not compute
  • Python Shell is cheaper and starts faster than a Spark cluster
  • No distributed compute is needed to write NDJSON to S3

Job parameters (passed via --JOB_PARAMS or Glue job arguments):
  --OBJECTS          comma-separated SF object list (or 'ALL')
  --BACKFILL_START   ISO datetime for full historical load
  --WATERMARK_SOURCE 'ssm' (incremental) or 'backfill_start' (full reload)
  --S3_BUCKET        landing bucket name
  --S3_PREFIX        key prefix (default: salesforce/raw)
  --SF_SECRET_NAME   Secrets Manager secret with PEM key
  --SSM_PREFIX       SSM watermark prefix
  --SF_INSTANCE_URL  Salesforce instance URL
  --SF_USERNAME      Salesforce service account
  --SF_CLIENT_ID     Connected App consumer key
  --SF_SANDBOX       'true' or 'false'
  --LOOKBACK_MINUTES watermark lookback buffer (default: 30)
  --PARALLEL_OBJECTS 'true' to extract objects concurrently (default: false)

Usage from ops CLI / Step Functions / Glue console:
  aws glue start-job-run \
    --job-name salesforce-bulk-extract-prod \
    --arguments '{
      "--OBJECTS": "Opportunity,Account,Lead",
      "--BACKFILL_START": "2020-01-01T00:00:00Z",
      "--WATERMARK_SOURCE": "backfill_start"
    }'
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

import boto3

# Glue passes job arguments as sys.argv; parse them with getResolvedOptions
# when running inside Glue, or fall back to environment for local testing.
try:
    from awsglue.utils import getResolvedOptions
    RUNNING_IN_GLUE = True
except ImportError:
    RUNNING_IN_GLUE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("salesforce_bulk_extract")

# ---------------------------------------------------------------------------
# Inline the shared modules (Glue Python Shell can't import from sibling dirs
# without a whl file; for production, package as a .whl in --extra-py-files)
# ---------------------------------------------------------------------------
# For a clean Glue deployment:
#   1. zip lambda/extractor/ → salesforce_extractor.zip
#   2. Upload to S3
#   3. Reference in Glue job: --extra-py-files s3://bucket/salesforce_extractor.zip
#
# For this standalone version we import directly:
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../lambda/extractor"))
from salesforce_client import SalesforceClient, OBJECT_QUERIES
from watermark import WatermarkManager


# ---------------------------------------------------------------------------
# Argument resolution
# ---------------------------------------------------------------------------
def _get_args() -> dict[str, str]:
    """Resolve Glue job arguments or fall back to environment variables."""
    if RUNNING_IN_GLUE:
        known = [
            "JOB_NAME", "OBJECTS", "BACKFILL_START", "WATERMARK_SOURCE",
            "S3_BUCKET", "S3_PREFIX", "SF_SECRET_NAME", "SSM_PREFIX",
            "SF_INSTANCE_URL", "SF_USERNAME", "SF_CLIENT_ID",
            "SF_SANDBOX", "LOOKBACK_MINUTES", "PARALLEL_OBJECTS",
        ]
        args = getResolvedOptions(sys.argv, known)
    else:
        # Local / unit-test fallback
        args = {k: os.environ.get(k, "") for k in [
            "OBJECTS", "S3_BUCKET", "S3_PREFIX", "SF_SECRET_NAME",
            "SSM_PREFIX", "SF_INSTANCE_URL", "SF_USERNAME", "SF_CLIENT_ID",
            "SF_SANDBOX", "LOOKBACK_MINUTES", "BACKFILL_START",
            "WATERMARK_SOURCE", "PARALLEL_OBJECTS",
        ]}
    return args


def _load_sf_private_key(secret_name: str) -> str:
    sm   = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])["private_key_pem"]


# ---------------------------------------------------------------------------
# Per-object extraction worker
# ---------------------------------------------------------------------------
def extract_one_object(
    sf_object: str,
    args: dict,
    watermark: datetime,
    run_ts: datetime,
) -> dict[str, Any]:
    """
    Extract a single Salesforce object.  Designed to run in a thread pool
    when PARALLEL_OBJECTS=true so multiple objects extract simultaneously.

    Returns a result dict consumed by the orchestrator for reporting and
    watermark updates.
    """
    s3 = boto3.client("s3")

    try:
        private_key_pem = _load_sf_private_key(args["SF_SECRET_NAME"])

        client = SalesforceClient(
            instance_url=args["SF_INSTANCE_URL"],
            client_id=args["SF_CLIENT_ID"],
            username=args["SF_USERNAME"],
            private_key_pem=private_key_pem,
            sandbox=args.get("SF_SANDBOX", "false").lower() == "true",
        )
        client.authenticate()

        record_count = client.extract_object(
            sf_object=sf_object,
            watermark=watermark,
            s3_client=s3,
            s3_bucket=args["S3_BUCKET"],
            s3_prefix=args.get("S3_PREFIX", "salesforce/raw"),
            run_ts=run_ts,
        )
        return {"status": "success", "records": record_count, "object": sf_object}

    except Exception as e:
        log.error("[%s] FAILED: %s\n%s", sf_object, e, traceback.format_exc())
        return {"status": "error", "records": 0, "object": sf_object, "error": str(e)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    run_ts = datetime.now(tz=timezone.utc)
    args   = _get_args()

    log.info("=== Glue Salesforce bulk extract started at %s ===", run_ts.isoformat())
    log.info("Args: %s", {k: v for k, v in args.items() if "SECRET" not in k})

    # ── Resolve objects to extract ────────────────────────────────────────
    raw_objects = args.get("OBJECTS", "ALL").strip()
    if raw_objects == "ALL":
        objects_to_run = list(OBJECT_QUERIES.keys())
    else:
        objects_to_run = [o.strip() for o in raw_objects.split(",") if o.strip()]

    log.info("Objects: %s", objects_to_run)

    # ── Resolve watermarks ────────────────────────────────────────────────
    ssm         = boto3.client("ssm")
    wm_manager  = WatermarkManager(ssm, args["SSM_PREFIX"])
    lookback_m  = int(args.get("LOOKBACK_MINUTES", "30"))
    wm_source   = args.get("WATERMARK_SOURCE", "ssm")

    watermarks: dict[str, datetime] = {}
    if wm_source == "backfill_start":
        raw_start = args.get("BACKFILL_START", "2020-01-01T00:00:00Z")
        start_dt  = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
        for obj in objects_to_run:
            watermarks[obj] = start_dt
        log.info("BACKFILL MODE — all objects extract from %s", start_dt.isoformat())
    else:
        for obj in objects_to_run:
            wm = wm_manager.get(obj)
            watermarks[obj] = wm_manager.apply_lookback(wm, lookback_m)

    # ── Extract (parallel or sequential) ─────────────────────────────────
    parallel = args.get("PARALLEL_OBJECTS", "false").lower() == "true"
    results: list[dict] = []

    if parallel:
        # Use a bounded thread pool — Salesforce limits concurrent Bulk API
        # jobs per org (typically 5 active jobs).  Stay under that.
        max_workers = min(5, len(objects_to_run))
        log.info("Parallel extraction: %d workers", max_workers)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    extract_one_object,
                    obj, args, watermarks[obj], run_ts
                ): obj
                for obj in objects_to_run
            }
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
    else:
        for obj in objects_to_run:
            result = extract_one_object(obj, args, watermarks[obj], run_ts)
            results.append(result)

    # ── Update watermarks (only for SSM-sourced runs, only on success) ────
    if wm_source == "ssm":
        for result in results:
            if result["status"] == "success":
                wm_manager.set(result["object"], run_ts)

    # ── Summary ───────────────────────────────────────────────────────────
    total     = sum(r["records"] for r in results)
    successes = [r for r in results if r["status"] == "success"]
    failures  = [r for r in results if r["status"] != "success"]

    log.info("=== Glue job complete ===")
    log.info("  Objects processed:  %d", len(results))
    log.info("  Total records:      %d", total)
    log.info("  Successes:          %d", len(successes))
    log.info("  Failures:           %d", len(failures))

    for r in successes:
        log.info("  ✅ %-30s %10d records", r["object"], r["records"])
    for r in failures:
        log.error("  ❌ %-30s ERROR: %s", r["object"], r.get("error", "unknown"))

    # Write a job summary JSON to S3 for downstream monitoring / Step Functions
    summary = {
        "run_timestamp":  run_ts.isoformat(),
        "watermark_mode": wm_source,
        "objects":        objects_to_run,
        "total_records":  total,
        "results":        results,
        "failed_objects": [r["object"] for r in failures],
    }
    s3 = boto3.client("s3")
    summary_key = (
        f"{args.get('S3_PREFIX', 'salesforce/raw')}/_job_summaries/"
        f"glue_{run_ts.strftime('%Y%m%dT%H%M%S')}.json"
    )
    s3.put_object(
        Bucket=args["S3_BUCKET"],
        Key=summary_key,
        Body=json.dumps(summary, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("Job summary written to s3://%s/%s", args["S3_BUCKET"], summary_key)

    # Fail the Glue job if any object errored — triggers retry / CloudWatch alarm
    if failures:
        raise RuntimeError(
            f"Glue extract failed for: {[r['object'] for r in failures]}. "
            "Check CloudWatch logs for per-object details."
        )


if __name__ == "__main__":
    main()
