# tests/unit/test_watermark.py
#
# Unit tests for the watermark manager using moto (mocked AWS).
# Run: pytest tests/unit/ -v

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import boto3
import pytest
from moto import mock_aws

from lambda.extractor.watermark import WatermarkManager, DEFAULT_BACKFILL_START


SSM_PREFIX = "/salesforce-ingest/test/watermarks"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """Prevent accidental real AWS calls during tests."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID",     "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION",    "us-east-1")


@mock_aws
class TestWatermarkManager:

    def _make_manager(self):
        ssm = boto3.client("ssm", region_name="us-east-1")
        return WatermarkManager(ssm, SSM_PREFIX), ssm

    # ------------------------------------------------------------------
    def test_get_returns_default_when_parameter_missing(self):
        manager, _ = self._make_manager()
        result = manager.get("Opportunity")
        assert result == DEFAULT_BACKFILL_START

    def test_get_returns_stored_watermark(self):
        manager, ssm = self._make_manager()
        expected = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        ssm.put_parameter(
            Name=f"{SSM_PREFIX}/Opportunity",
            Value=expected.isoformat(),
            Type="String",
        )
        result = manager.get("Opportunity")
        assert result == expected

    def test_set_stores_and_get_retrieves(self):
        manager, _ = self._make_manager()
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        manager.set("Account", ts)
        assert manager.get("Account") == ts

    def test_set_is_idempotent(self):
        manager, _ = self._make_manager()
        ts1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ts2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        manager.set("Lead", ts1)
        manager.set("Lead", ts2)
        assert manager.get("Lead") == ts2

    def test_apply_lookback_subtracts_minutes(self):
        manager, _ = self._make_manager()
        base = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        result = manager.apply_lookback(base, lookback_minutes=30)
        assert result == datetime(2026, 4, 20, 11, 30, 0, tzinfo=timezone.utc)

    def test_apply_lookback_default_is_30_minutes(self):
        manager, _ = self._make_manager()
        base     = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        expected = base - timedelta(minutes=30)
        assert manager.apply_lookback(base) == expected

    def test_get_adds_utc_tzinfo_when_missing(self):
        manager, ssm = self._make_manager()
        # Store without timezone info
        ssm.put_parameter(
            Name=f"{SSM_PREFIX}/Contact",
            Value="2025-06-01T00:00:00",
            Type="String",
        )
        result = manager.get("Contact")
        assert result.tzinfo is not None

    def test_get_all_returns_dict_for_all_objects(self):
        manager, ssm = self._make_manager()
        objects = ["Account", "Contact", "Lead"]
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        for obj in objects[:2]:   # set only 2; third should fall back to default
            ssm.put_parameter(
                Name=f"{SSM_PREFIX}/{obj}",
                Value=ts.isoformat(),
                Type="String",
            )

        result = manager.get_all(objects)
        assert len(result) == 3
        assert result["Account"] == ts
        assert result["Contact"] == ts
        assert result["Lead"]    == DEFAULT_BACKFILL_START


# tests/unit/test_handler.py
# ---------------------------------------------------------------------------
from unittest.mock import MagicMock, patch


@mock_aws
class TestHandlerOrchestration:

    def _make_env(self, monkeypatch, tmp_bucket="test-bucket"):
        monkeypatch.setenv("SALESFORCE_SECRET_NAME", "test-secret")
        monkeypatch.setenv("SSM_WATERMARK_PREFIX",   SSM_PREFIX)
        monkeypatch.setenv("S3_BUCKET",              tmp_bucket)
        monkeypatch.setenv("S3_PREFIX",              "salesforce/raw")
        monkeypatch.setenv("SF_INSTANCE_URL",        "https://test.salesforce.com")
        monkeypatch.setenv("SF_USERNAME",            "test@example.com")
        monkeypatch.setenv("SF_CLIENT_ID",           "fake_client_id")
        monkeypatch.setenv("SF_SANDBOX",             "true")
        monkeypatch.setenv("LOOKBACK_MINUTES",       "30")
        monkeypatch.setenv("OBJECTS",                "Account,Opportunity")

    def test_dry_run_returns_without_s3_write(self, monkeypatch):
        self._make_env(monkeypatch)

        with patch("lambda.extractor.handler._load_sf_credentials", return_value="FAKE_PEM"), \
             patch("lambda.extractor.handler.SalesforceClient") as mock_sf:

            mock_sf.return_value.authenticate.return_value = None
            mock_sf.return_value.extract_object.return_value = 0

            # Import here to pick up monkeypatched env vars
            import importlib
            import lambda.extractor.handler as h
            importlib.reload(h)

            result = h.handler({"dry_run": True}, None)

            assert result["failures"] == []
            # extract_object should NOT be called in dry_run mode
            mock_sf.return_value.extract_object.assert_not_called()

    def test_object_failure_does_not_block_others(self, monkeypatch):
        """A failing object should not prevent other objects from running."""
        self._make_env(monkeypatch)

        import importlib
        import lambda.extractor.handler as h
        importlib.reload(h)

        call_count = {"n": 0}

        def side_effect(**kwargs):
            call_count["n"] += 1
            if kwargs.get("sf_object") == "Account":
                raise RuntimeError("Simulated API failure")
            return 100

        with patch("lambda.extractor.handler._load_sf_credentials", return_value="FAKE_PEM"), \
             patch("lambda.extractor.handler.SalesforceClient") as mock_sf, \
             patch("lambda.extractor.handler.WatermarkManager"):

            mock_sf.return_value.authenticate.return_value = None
            mock_sf.return_value.extract_object.side_effect = side_effect

            with pytest.raises(RuntimeError, match="Account"):
                h.handler({"objects": ["Account", "Opportunity"]}, None)

            # Both objects were attempted despite Account failing
            assert call_count["n"] == 2
