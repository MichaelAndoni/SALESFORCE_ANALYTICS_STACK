# tests/integration/test_pipeline_e2e.py
#
# End-to-end integration smoke tests.
# These tests hit REAL AWS resources — only run in a controlled environment
# with valid credentials and after deploying the CDK stack.
#
# Skipped in CI unless INTEGRATION_TESTS=true is set.
# Usage:
#   INTEGRATION_TESTS=true DEPLOY_ENV=dev pytest tests/integration/ -v -m integration

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
import pytest

# Skip all integration tests unless explicitly enabled
pytestmark = pytest.mark.skipif(
    os.environ.get("INTEGRATION_TESTS") != "true",
    reason="Integration tests disabled. Set INTEGRATION_TESTS=true to run.",
)

# ── Config from environment ───────────────────────────────────────────────────
DEPLOY_ENV     = os.environ.get("DEPLOY_ENV", "dev")
S3_BUCKET      = os.environ.get("S3_BUCKET",  f"sf-ingest-{DEPLOY_ENV}-landing-ACCOUNT_ID")
SSM_PREFIX     = f"/salesforce-ingest/{DEPLOY_ENV}/watermarks"
LAMBDA_NAME    = f"sf-ingest-{DEPLOY_ENV}-extractor"
SFN_ARN        = os.environ.get("SFN_ARN", "")   # Step Functions state machine ARN

s3  = boto3.client("s3")
ssm = boto3.client("ssm")
lam = boto3.client("lambda")
sfn = boto3.client("stepfunctions")


@pytest.mark.integration
class TestWatermarkRoundTrip:
    """Verify SSM watermark read/write against real Parameter Store."""

    def test_set_and_get_watermark(self):
        """Write a watermark to SSM and read it back."""
        param_name = f"{SSM_PREFIX}/IntegrationTest"
        test_ts    = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        ssm.put_parameter(
            Name=param_name,
            Value=test_ts.isoformat(),
            Type="String",
            Overwrite=True,
        )

        resp = ssm.get_parameter(Name=param_name)
        stored = datetime.fromisoformat(resp["Parameter"]["Value"])

        assert stored == test_ts

        # Cleanup
        ssm.delete_parameter(Name=param_name)


@pytest.mark.integration
class TestExtractorLambda:
    """
    Smoke test the extractor Lambda with a dry-run invocation.
    Verifies the Lambda can auth, read SSM, and return without errors.
    """

    def test_dry_run_invocation(self):
        """Invoke the Lambda in dry_run mode — no S3 writes, no watermark changes."""
        payload = {"objects": ["User"], "dry_run": True}

        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )

        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp or resp.get("FunctionError") is None

        result = json.loads(resp["Payload"].read())
        assert "results"       in result
        assert result["failures"] == []

        user_result = result["results"].get("User", {})
        assert user_result.get("status") == "dry_run"

    def test_single_object_extract_writes_to_s3(self):
        """
        Extract a single small object (User) and verify files land in S3.
        User is the safest choice — small cardinality, rarely > 100 records.
        """
        # Record existing file count
        before = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="salesforce/raw/user/")
        before_count = before.get("KeyCount", 0)

        payload = {"objects": ["User"]}
        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )

        assert resp["StatusCode"] == 200
        result = json.loads(resp["Payload"].read())
        assert result.get("failures") == [], f"Extract failed: {result}"

        # Verify new files appeared in S3
        after = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="salesforce/raw/user/")
        after_count = after.get("KeyCount", 0)

        assert after_count > before_count, (
            "No new files in S3 after User extract — Snowpipe may not be receiving events"
        )


@pytest.mark.integration
class TestSnowpipeConnectivity:
    """
    Verify Snowpipe auto-ingest is receiving S3 event notifications.
    Does not check Snowflake load results — just the SQS notification path.
    """

    def test_s3_put_triggers_sqs_message(self):
        """
        Put a test file in the landing bucket and verify SQS message count increases.
        (Requires SQS queue URL — set SQS_QUEUE_URL env var.)
        """
        sqs_url = os.environ.get("SQS_QUEUE_URL")
        if not sqs_url:
            pytest.skip("SQS_QUEUE_URL not set — skipping Snowpipe notification test")

        sqs = boto3.client("sqs")

        before = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]["ApproximateNumberOfMessages"]

        # Put a test file
        test_key = f"salesforce/raw/user/date=2026-04-20/integration_test_{int(time.time())}.json"
        test_record = json.dumps({
            "Id": "005TEST000000001",
            "Name": "Test User",
            "_extract_timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "_source_object": "User",
        })
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=test_key,
            Body=test_record.encode(),
            ContentType="application/x-ndjson",
        )

        # Give S3 event notification a moment to propagate to SQS
        time.sleep(5)

        after = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]["ApproximateNumberOfMessages"]

        assert int(after) > int(before), (
            "SQS message count did not increase after S3 PUT — "
            "check S3 event notification configuration"
        )

        # Cleanup test file
        s3.delete_object(Bucket=S3_BUCKET, Key=test_key)


@pytest.mark.integration
class TestStateMachineExecution:
    """Test the Step Functions pipeline state machine with a dry-run execution."""

    def test_sfn_execution_completes(self):
        """Start the state machine and verify it completes within 10 minutes."""
        if not SFN_ARN:
            pytest.skip("SFN_ARN not set")

        run_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        exec_name = f"integration-test-{run_ts}"

        # Start with dry_run so no real data is moved
        resp = sfn.start_execution(
            stateMachineArn=SFN_ARN,
            name=exec_name,
            input=json.dumps({"dry_run": True, "objects": ["User"]}),
        )
        exec_arn = resp["executionArn"]

        # Poll until terminal state (max 10 min)
        deadline = time.time() + 600
        status   = "RUNNING"

        while time.time() < deadline and status == "RUNNING":
            desc   = sfn.describe_execution(executionArn=exec_arn)
            status = desc["status"]
            if status == "RUNNING":
                time.sleep(15)

        assert status == "SUCCEEDED", (
            f"State machine execution ended in {status} — "
            f"check execution history: {exec_arn}"
        )
