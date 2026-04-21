# tests/conftest.py
#
# Shared pytest fixtures available to all test modules.
# Fixtures defined here are auto-discovered by pytest — no import needed.

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

# ── Make lambda source importable from tests/ ────────────────────────────────
# Add each Lambda package to sys.path so tests can `import salesforce_client`
# without installing the package.
REPO_ROOT = Path(__file__).parent.parent
for lambda_dir in (REPO_ROOT / "lambda").iterdir():
    if lambda_dir.is_dir():
        sys.path.insert(0, str(lambda_dir))

# ── AWS credential stubs (prevent accidental real API calls) ─────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION",    "us-east-1")
os.environ.setdefault("AWS_SECURITY_TOKEN",    "testing")
os.environ.setdefault("AWS_SESSION_TOKEN",     "testing")

# ── Common constants ─────────────────────────────────────────────────────────
SSM_PREFIX      = "/salesforce-ingest/test/watermarks"
S3_BUCKET       = "test-sf-landing-bucket"
SECRET_NAME     = "test/salesforce-credentials"
TEST_REGION     = "us-east-1"
TEST_ACCOUNT    = "123456789012"

FAKE_PRIVATE_KEY_PEM = """\
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0Z3VS5JJcds3xHn/ygWep4PAtEsHAqOL3+q0ZdKhJWzUbE56
FAKE_KEY_FOR_TESTING_ONLY_NOT_VALID_CRYPTOGRAPHIC_MATERIAL_AAAAAAAAAA
-----END RSA PRIVATE KEY-----"""


# ── AWS resource fixtures ────────────────────────────────────────────────────

@pytest.fixture
def aws_mock():
    """Start moto mock for S3, SSM, and Secrets Manager together."""
    with mock_aws():
        yield


@pytest.fixture
def s3_client(aws_mock):
    client = boto3.client("s3", region_name=TEST_REGION)
    client.create_bucket(Bucket=S3_BUCKET)
    return client


@pytest.fixture
def ssm_client(aws_mock):
    return boto3.client("ssm", region_name=TEST_REGION)


@pytest.fixture
def secrets_client(aws_mock):
    client = boto3.client("secretsmanager", region_name=TEST_REGION)
    client.create_secret(
        Name=SECRET_NAME,
        SecretString=json.dumps({"private_key_pem": FAKE_PRIVATE_KEY_PEM}),
    )
    return client


@pytest.fixture
def lambda_client(aws_mock):
    return boto3.client("lambda", region_name=TEST_REGION)


# ── Watermark fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def watermark_manager(ssm_client):
    """Pre-configured WatermarkManager pointing at test SSM prefix."""
    from watermark import WatermarkManager
    return WatermarkManager(ssm_client, SSM_PREFIX)


@pytest.fixture
def seeded_watermarks(ssm_client, watermark_manager):
    """
    Pre-populate SSM with known watermarks for all SF objects.
    Returns the seeded timestamps dict for assertion in tests.
    """
    from salesforce_client import OBJECT_QUERIES
    base_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    seeds   = {}

    for obj in OBJECT_QUERIES:
        watermark_manager.set(obj, base_ts)
        seeds[obj] = base_ts

    return seeds


# ── Salesforce API mock ──────────────────────────────────────────────────────

@pytest.fixture
def mock_sf_client():
    """
    Mock SalesforceClient that returns fake records without hitting the API.
    Patch this into tests that exercise the handler without real Salesforce.
    """
    client = MagicMock()
    client.authenticate.return_value = None
    client.extract_object.return_value = 42  # 42 records per call
    return client


@pytest.fixture
def mock_sf_records():
    """Sample Salesforce record payloads for testing column mapping."""
    return {
        "Account": [
            {
                "Id":               "001000000000001AAA",
                "Name":             "Acme Corp",
                "Type":             "Customer",
                "Industry":         "Technology",
                "AnnualRevenue":    "5000000",
                "NumberOfEmployees":"250",
                "BillingCity":      "Chicago",
                "BillingState":     "IL",
                "BillingCountry":   "US",
                "OwnerId":          "005000000000001AAA",
                "IsDeleted":        "false",
                "CreatedDate":      "2024-01-15T10:30:00.000+0000",
                "SystemModstamp":   "2026-04-01T08:00:00.000+0000",
                "_extract_timestamp": "2026-04-20T12:00:00+00:00",
                "_source_object":   "Account",
            }
        ],
        "Opportunity": [
            {
                "Id":               "006000000000001AAA",
                "Name":             "Acme — Enterprise Deal",
                "AccountId":        "001000000000001AAA",
                "OwnerId":          "005000000000001AAA",
                "StageName":        "Proposal/Price Quote",
                "Amount":           "150000",
                "Probability":      "60",
                "CloseDate":        "2026-06-30",
                "ForecastCategory": "Pipeline",
                "IsClosed":         "false",
                "IsWon":            "false",
                "IsDeleted":        "false",
                "CreatedDate":      "2026-01-10T09:00:00.000+0000",
                "SystemModstamp":   "2026-04-19T14:22:00.000+0000",
                "_extract_timestamp": "2026-04-20T12:00:00+00:00",
                "_source_object":   "Opportunity",
            }
        ],
    }


# ── Handler environment fixtures ─────────────────────────────────────────────

@pytest.fixture
def handler_env(monkeypatch, aws_mock):
    """
    Set all environment variables required by the extractor handler.
    Patches them into os.environ for the duration of the test.
    """
    env_vars = {
        "SALESFORCE_SECRET_NAME": SECRET_NAME,
        "SSM_WATERMARK_PREFIX":   SSM_PREFIX,
        "S3_BUCKET":              S3_BUCKET,
        "S3_PREFIX":              "salesforce/raw",
        "SF_INSTANCE_URL":        "https://test.salesforce.com",
        "SF_USERNAME":            "test@example.com",
        "SF_CLIENT_ID":           "fake_consumer_key",
        "SF_SANDBOX":             "true",
        "LOOKBACK_MINUTES":       "30",
        "OBJECTS":                "Account,Opportunity",
    }
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)

    # Create S3 bucket and Secrets Manager secret
    s3_c = boto3.client("s3", region_name=TEST_REGION)
    s3_c.create_bucket(Bucket=S3_BUCKET)

    sm_c = boto3.client("secretsmanager", region_name=TEST_REGION)
    sm_c.create_secret(
        Name=SECRET_NAME,
        SecretString=json.dumps({"private_key_pem": FAKE_PRIVATE_KEY_PEM}),
    )

    return env_vars
