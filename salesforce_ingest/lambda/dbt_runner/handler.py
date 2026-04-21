# lambda/dbt_runner/handler.py
#
# Triggers an AWS CodeBuild project to run dbt commands inside the VPC.
#
# Why CodeBuild instead of running dbt directly in Lambda?
#   • dbt + snowflake-connector-python is ~300MB installed — too large for a
#     Lambda layer or deployment package.
#   • dbt runs can take 10–20 minutes on large projects; Lambda max is 15 min.
#   • CodeBuild has no size or duration constraint, runs in your VPC, and
#     streams logs to CloudWatch natively.
#   • Separation of concerns: Lambda orchestrates (lightweight), CodeBuild builds.
#
# This Lambda is called by Step Functions after Snowpipe drains.
# It starts a CodeBuild build, polls until completion, and returns a
# structured result that Step Functions can branch on.
#
# Environment variables:
#   CODEBUILD_PROJECT_NAME   — name of the dbt CodeBuild project
#   DBT_REPO_URL             — Git clone URL for the dbt project
#   DBT_BRANCH               — branch to build (default: main)
#   SNOWFLAKE_SECRET_NAME    — Secrets Manager secret for dbt profile

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

cb_client = boto3.client("codebuild")

PROJECT_NAME  = os.environ["CODEBUILD_PROJECT_NAME"]
DBT_BRANCH    = os.environ.get("DBT_BRANCH", "main")
POLL_INTERVAL = 15     # seconds between build status polls
MAX_WAIT_SECS = 1800   # 30-minute hard timeout for the poller loop


def _start_build(command: str, select: str, target: str,
                 execution_name: str) -> str:
    """Start a CodeBuild build and return the build ID."""
    resp = cb_client.start_build(
        projectName=PROJECT_NAME,
        sourceVersion=DBT_BRANCH,
        environmentVariablesOverride=[
            {"name": "DBT_COMMAND", "value": command,        "type": "PLAINTEXT"},
            {"name": "DBT_SELECT",  "value": select or "",   "type": "PLAINTEXT"},
            {"name": "DBT_TARGET",  "value": target,         "type": "PLAINTEXT"},
            # Tag the build with the SFN execution name for traceability
            {"name": "SFN_EXECUTION", "value": execution_name, "type": "PLAINTEXT"},
        ],
    )
    build_id = resp["build"]["id"]
    log.info("Started CodeBuild build: %s", build_id)
    return build_id


def _poll_build(build_id: str) -> dict:
    """
    Poll CodeBuild until the build reaches a terminal state.
    Returns the final build object.
    """
    deadline = time.time() + MAX_WAIT_SECS

    while time.time() < deadline:
        resp  = cb_client.batch_get_builds(ids=[build_id])
        build = resp["builds"][0]
        phase = build["buildStatus"]

        log.info("Build %s status: %s", build_id, phase)

        if phase in ("SUCCEEDED", "FAILED", "FAULT", "TIMED_OUT", "STOPPED"):
            return build

        time.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"CodeBuild build {build_id} did not complete within {MAX_WAIT_SECS}s"
    )


def _parse_build_result(build: dict) -> dict:
    """
    Extract key metrics from the CodeBuild build object.
    Parses the build's exported variables or log output to count
    models built and tests failed.
    """
    status     = build["buildStatus"]
    succeeded  = status == "SUCCEEDED"
    start_time = build.get("startTime", datetime.now(tz=timezone.utc))
    end_time   = build.get("endTime",   datetime.now(tz=timezone.utc))

    if hasattr(start_time, "timestamp"):
        duration_seconds = int((end_time - start_time).total_seconds())
    else:
        duration_seconds = 0

    # CodeBuild can export environment variables from buildspec
    # We read dbt's run_results.json from S3 artifacts if configured,
    # or fall back to inferring from build status.
    exported = {v["name"]: v["value"]
                for v in build.get("exportedEnvironmentVariables", [])}

    return {
        "build_id":        build["id"],
        "build_status":    status,
        "succeeded":       succeeded,
        "duration_seconds": duration_seconds,
        "models_built":    int(exported.get("DBT_MODELS_BUILT", 0)),
        "tests_passed":    int(exported.get("DBT_TESTS_PASSED",  0)),
        "tests_failed":    int(exported.get("DBT_TESTS_FAILED",  0)),
        "log_url": (
            f"https://console.aws.amazon.com/codesuite/codebuild/projects/"
            f"{PROJECT_NAME}/build/{build['id'].split(':')[-1]}"
        ),
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Step Functions calls this Lambda after Snowpipe has drained.

    Input event:
    {
      "command":        "build",          # dbt command: build | run | test
      "select":         "tag:marts",      # optional --select expression
      "target":         "prod",           # dbt --target
      "execution_name": "sfn-exec-123"   # for traceability
    }

    Returns:
    {
      "succeeded":       true/false,
      "models_built":    42,
      "tests_failed":    0,
      "tests_passed":    120,
      "build_status":    "SUCCEEDED",
      "duration_seconds": 480,
      "log_url":         "https://..."
    }
    """
    command        = event.get("command",        "build")
    select         = event.get("select",         "")
    target         = event.get("target",         "prod")
    execution_name = event.get("execution_name", "manual")

    log.info(
        "dbt runner: command=%s select=%r target=%s execution=%s",
        command, select, target, execution_name,
    )

    build_id = _start_build(command, select, target, execution_name)

    try:
        final_build = _poll_build(build_id)
        result      = _parse_build_result(final_build)
    except TimeoutError as e:
        log.error("Build timed out: %s", e)
        result = {
            "build_id":     build_id,
            "build_status": "TIMED_OUT",
            "succeeded":    False,
            "models_built": 0,
            "tests_failed": -1,
            "tests_passed": 0,
            "log_url":      "",
        }

    log.info("dbt run result: %s", json.dumps(result))

    if not result["succeeded"]:
        raise RuntimeError(
            f"dbt {command} failed (status={result['build_status']}) — "
            f"see CodeBuild: {result.get('log_url', build_id)}"
        )

    return result
