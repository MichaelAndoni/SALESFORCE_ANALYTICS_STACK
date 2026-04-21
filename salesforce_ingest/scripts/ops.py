#!/usr/bin/env python3
# scripts/ops.py
#
# Operational CLI for the Salesforce ingest pipeline.
# Handles backfills, watermark resets, and pipe management.
#
# Usage:
#   python ops.py backfill --object Opportunity --start 2024-01-01
#   python ops.py reset-watermark --object Account --to 2025-06-01
#   python ops.py show-watermarks
#   python ops.py invoke-extractor --objects Account,Contact
#   python ops.py refresh-pipe --pipe PIPE_OPPORTUNITY

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import boto3

# ── Config ────────────────────────────────────────────────────────────────────
ENV                = os.environ.get("DEPLOY_ENV", "prod")
LAMBDA_FUNCTION    = f"sf-ingest-{ENV}-extractor"
SSM_PREFIX         = f"/salesforce-ingest/{ENV}/watermarks"
SNOWFLAKE_OBJECTS  = [
    "Account", "Contact", "Lead", "Opportunity",
    "OpportunityLineItem", "Campaign", "CampaignMember",
    "Task", "Event", "User",
]

ssm    = boto3.client("ssm")
lam    = boto3.client("lambda")


def show_watermarks(args):
    """Print all current watermarks from SSM."""
    names = [f"{SSM_PREFIX}/{obj}" for obj in SNOWFLAKE_OBJECTS]
    resp  = ssm.get_parameters(Names=names, WithDecryption=True)

    found  = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
    missing = [o for o in SNOWFLAKE_OBJECTS if o not in found]

    print("\nCurrent watermarks:")
    print(f"{'Object':<30} {'Watermark'}")
    print("-" * 60)
    for obj in SNOWFLAKE_OBJECTS:
        wm = found.get(obj, "⚠️  NOT SET (will use backfill start)")
        print(f"{obj:<30} {wm}")

    if missing:
        print(f"\n⚠️  Objects without watermarks: {missing}")
    print()


def reset_watermark(args):
    """Override a watermark in SSM."""
    obj = args.object
    dt  = datetime.fromisoformat(args.to).replace(tzinfo=timezone.utc)

    confirm = input(
        f"Reset watermark for {obj} to {dt.isoformat()}? "
        f"Next extract will re-pull from this point. [y/N] "
    )
    if confirm.lower() != "y":
        print("Aborted.")
        return

    ssm.put_parameter(
        Name=f"{SSM_PREFIX}/{obj}",
        Value=dt.isoformat(),
        Type="String",
        Overwrite=True,
    )
    print(f"✅ Watermark for {obj} set to {dt.isoformat()}")


def invoke_extractor(args):
    """
    Directly invoke the extractor Lambda.
    Useful for triggering out-of-schedule runs or targeted re-extracts.
    """
    objects  = args.objects.split(",") if args.objects else SNOWFLAKE_OBJECTS
    payload  = {"objects": objects}

    if args.backfill_start:
        payload["backfill_start"] = args.backfill_start
        print(f"⚠️  BACKFILL MODE — watermarks will NOT be updated.")
        print(f"    Objects: {objects}")
        print(f"    Start:   {args.backfill_start}")
    else:
        print(f"Invoking extractor for: {objects}")

    if args.dry_run:
        payload["dry_run"] = True
        print("DRY RUN — no S3 writes.")

    confirm = input("Proceed? [y/N] ")
    if confirm.lower() != "y":
        print("Aborted.")
        return

    resp = lam.invoke(
        FunctionName=LAMBDA_FUNCTION,
        InvocationType="RequestResponse",   # synchronous
        Payload=json.dumps(payload).encode(),
    )

    result   = json.loads(resp["Payload"].read())
    status   = resp["StatusCode"]
    fn_error = resp.get("FunctionError")

    if fn_error:
        print(f"❌ Lambda error ({fn_error}):")
        print(json.dumps(result, indent=2))
        sys.exit(1)
    else:
        print(f"\n✅ Extractor completed (HTTP {status})")
        print(f"   Total records: {result.get('total_records', 'n/a')}")
        print(f"   Failures:      {result.get('failures') or 'none'}")
        for obj, r in result.get("results", {}).items():
            status_icon = "✅" if r["status"] == "success" else "❌"
            print(f"   {status_icon} {obj:<30} {r.get('records', 0):>8,} records")


def backfill(args):
    """
    Convenience wrapper: invoke extractor with backfill_start set.
    Does NOT touch watermarks — safe to run multiple times.
    """
    args.objects       = args.object if hasattr(args, "object") and args.object else None
    args.backfill_start = args.start
    args.dry_run       = getattr(args, "dry_run", False)
    invoke_extractor(args)


def refresh_pipe(args):
    """
    Print the Snowflake SQL to manually refresh a stalled Snowpipe.
    (Runs ALTER PIPE ... REFRESH — you execute it in Snowflake.)
    """
    pipe = args.pipe
    db   = "RAW"
    schema = "SALESFORCE"
    print(f"\nRun the following in Snowflake to refresh {pipe}:")
    print("-" * 60)
    print(f"USE ROLE SYSADMIN;")
    print(f"ALTER PIPE {db}.{schema}.{pipe} REFRESH;")
    print(f"SELECT SYSTEM$PIPE_STATUS('{schema}.{pipe}');")
    print()
    print("If the pipe shows PAUSED, resume it first:")
    print(f"ALTER PIPE {db}.{schema}.{pipe} SET PIPE_EXECUTION_PAUSED = FALSE;")
    print()


# ── CLI wiring ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Salesforce ingest pipeline operator CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("show-watermarks", help="Print all SSM watermarks")

    p_reset = sub.add_parser("reset-watermark", help="Override a watermark")
    p_reset.add_argument("--object", required=True)
    p_reset.add_argument("--to",     required=True, help="ISO datetime, e.g. 2025-01-01")

    p_invoke = sub.add_parser("invoke-extractor", help="Trigger Lambda directly")
    p_invoke.add_argument("--objects",        help="Comma-separated list of SF objects")
    p_invoke.add_argument("--backfill-start", dest="backfill_start")
    p_invoke.add_argument("--dry-run",        action="store_true", dest="dry_run")

    p_backfill = sub.add_parser("backfill", help="Backfill from a specific date")
    p_backfill.add_argument("--object", help="Single object (omit for all)")
    p_backfill.add_argument("--start",  required=True, help="ISO datetime backfill start")
    p_backfill.add_argument("--dry-run", action="store_true", dest="dry_run")

    p_pipe = sub.add_parser("refresh-pipe", help="Print Snowflake SQL to refresh a pipe")
    p_pipe.add_argument("--pipe", required=True, help="e.g. PIPE_OPPORTUNITY")

    args = parser.parse_args()

    dispatch = {
        "show-watermarks":  show_watermarks,
        "reset-watermark":  reset_watermark,
        "invoke-extractor": invoke_extractor,
        "backfill":         backfill,
        "refresh-pipe":     refresh_pipe,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
