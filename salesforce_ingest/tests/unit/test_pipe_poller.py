# tests/unit/test_pipe_poller.py
#
# Unit tests for the pipe poller Lambda.
# Uses moto for AWS mocks and unittest.mock for Snowflake.

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import boto3
import pytest
from moto import mock_aws


# ── Pipe status fixtures ──────────────────────────────────────────────────────

def _make_pipe_status(state="RUNNING", pending=0, failed=0):
    return json.dumps({
        "executionState":           state,
        "pendingFileCount":         pending,
        "failedFileCount":          failed,
        "notificationChannelStatus": "CONNECTED",
        "lastIngestedTimestamp":    "2026-04-20T11:50:00Z",
    })


class TestPipePollerReadyCheck:
    """Tests for the ready_check mode used by Step Functions."""

    @mock_aws
    def test_all_pipes_ready_returns_true(self, monkeypatch):
        """When all pipes have 0 pending files, all_pipes_ready should be True."""
        monkeypatch.setenv("SNOWFLAKE_SECRET_NAME", "test-secret")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT",     "test.us-east-1")
        monkeypatch.setenv("SNOWFLAKE_USER",        "test_user")
        monkeypatch.setenv("RAW_DATABASE",          "RAW")
        monkeypatch.setenv("RAW_SCHEMA",            "SALESFORCE")
        monkeypatch.setenv("CW_METRIC_NAMESPACE",   "TestIngest")

        # Create secrets
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="test-secret",
            SecretString=json.dumps({"password": "test_pass"}),
        )

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (_make_pipe_status(pending=0),)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("lambda.pipe_poller.handler._get_snowflake_conn",
                   return_value=mock_conn), \
             patch("lambda.pipe_poller.handler._publish_metrics"):

            import importlib
            import lambda.pipe_poller.handler as h
            importlib.reload(h)

            result = h.handler({"check_mode": "ready_check"}, None)

        assert result["all_pipes_ready"] is True
        assert result["max_pending"]     == 0
        assert result["any_pipe_errors"] is False

    @mock_aws
    def test_pending_files_returns_not_ready(self, monkeypatch):
        """When any pipe has pending files, all_pipes_ready should be False."""
        monkeypatch.setenv("SNOWFLAKE_SECRET_NAME", "test-secret2")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT",     "test.us-east-1")
        monkeypatch.setenv("SNOWFLAKE_USER",        "test_user")
        monkeypatch.setenv("RAW_DATABASE",          "RAW")
        monkeypatch.setenv("RAW_SCHEMA",            "SALESFORCE")
        monkeypatch.setenv("CW_METRIC_NAMESPACE",   "TestIngest")

        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="test-secret2",
            SecretString=json.dumps({"password": "test"}),
        )

        call_count = {"n": 0}
        def mock_fetchone():
            call_count["n"] += 1
            # First pipe has 5 pending files
            if call_count["n"] == 1:
                return (_make_pipe_status(pending=5),)
            return (_make_pipe_status(pending=0),)

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = mock_fetchone

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("lambda.pipe_poller.handler._get_snowflake_conn",
                   return_value=mock_conn), \
             patch("lambda.pipe_poller.handler._publish_metrics"):

            import importlib
            import lambda.pipe_poller.handler as h
            importlib.reload(h)

            result = h.handler({"check_mode": "ready_check"}, None)

        assert result["all_pipes_ready"] is False
        assert result["max_pending"]     == 5


class TestWatermarkEdgeCases:
    """Edge case tests for the watermark manager."""

    @mock_aws
    def test_lookback_crosses_day_boundary(self):
        """Lookback from midnight should correctly roll back to previous day."""
        from watermark import WatermarkManager
        from datetime import timedelta

        ssm = boto3.client("ssm", region_name="us-east-1")
        mgr = WatermarkManager(ssm, "/test/wm")

        midnight = datetime(2026, 4, 20, 0, 15, 0, tzinfo=timezone.utc)
        result   = mgr.apply_lookback(midnight, lookback_minutes=30)

        assert result.day  == 19     # rolled back to April 19
        assert result.hour == 23
        assert result.minute == 45

    @mock_aws
    def test_set_naive_datetime_gets_utc_tzinfo(self):
        """set() should attach UTC tzinfo to naive datetimes."""
        from watermark import WatermarkManager

        ssm = boto3.client("ssm", region_name="us-east-1")
        mgr = WatermarkManager(ssm, "/test/wm")

        naive = datetime(2026, 4, 20, 12, 0, 0)   # no tzinfo
        mgr.set("Account", naive)

        result = mgr.get("Account")
        assert result.tzinfo is not None
        assert result.year  == 2026
        assert result.month == 4

    @mock_aws
    def test_get_all_batch_respects_10_param_limit(self):
        """get_all() should handle > 10 objects (SSM limit per call)."""
        from watermark import WatermarkManager

        ssm = boto3.client("ssm", region_name="us-east-1")
        mgr = WatermarkManager(ssm, "/test/wm")

        # 12 objects — forces two SSM GetParameters calls
        objects = [f"Object{i}" for i in range(12)]
        result  = mgr.get_all(objects)

        assert len(result) == 12
        # All should return DEFAULT_BACKFILL_START since none were set
        from watermark import DEFAULT_BACKFILL_START
        for obj in objects:
            assert result[obj] == DEFAULT_BACKFILL_START


class TestSalesforceClientSoql:
    """Tests for SOQL query construction (no real API calls)."""

    def test_all_objects_have_soql_templates(self):
        """Every object in OBJECT_QUERIES must have a valid SOQL template."""
        from salesforce_client import OBJECT_QUERIES

        expected_objects = {
            "Account", "Contact", "Lead", "Opportunity",
            "OpportunityLineItem", "Campaign", "CampaignMember",
            "Task", "Event", "User",
        }
        assert set(OBJECT_QUERIES.keys()) == expected_objects

    def test_soql_templates_contain_watermark_placeholder(self):
        """Every SOQL template must include {watermark} for incremental filtering."""
        from salesforce_client import OBJECT_QUERIES

        for obj, soql in OBJECT_QUERIES.items():
            assert "{watermark}" in soql, (
                f"SOQL template for {obj} is missing {{watermark}} placeholder"
            )

    def test_soql_templates_include_systemmodstamp(self):
        """SystemModstamp must be selected in every object query (drives watermark)."""
        from salesforce_client import OBJECT_QUERIES

        for obj, soql in OBJECT_QUERIES.items():
            assert "SystemModstamp" in soql, (
                f"SOQL for {obj} is missing SystemModstamp — watermark tracking broken"
            )

    def test_watermark_formats_correctly_into_soql(self):
        """The watermark placeholder should format to a valid Salesforce datetime."""
        from salesforce_client import OBJECT_QUERIES

        dt = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        wm_str = dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

        for obj, soql in OBJECT_QUERIES.items():
            formatted = soql.format(watermark=wm_str)
            assert "2026-04-20T12:00:00.000+0000" in formatted, (
                f"Watermark did not format correctly in SOQL for {obj}"
            )
            assert "{watermark}" not in formatted, (
                f"Unformatted placeholder still present in SOQL for {obj}"
            )
