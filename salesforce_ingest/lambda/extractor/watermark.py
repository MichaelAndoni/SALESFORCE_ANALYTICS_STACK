# lambda/extractor/watermark.py
#
# Watermark (high-water mark) management using SSM Parameter Store.
#
# Why SSM over DynamoDB for this use-case?
#   • Watermarks are simple scalar timestamps — no need for a full table
#   • SSM has built-in versioning / history for free (SecureString)
#   • Cheaper: SSM standard parameters are free; DynamoDB has per-read cost
#   • Fewer moving parts in the Lambda execution environment
#
# Parameter naming convention:
#   /salesforce-ingest/{env}/watermarks/{SfObject}
#   e.g.  /salesforce-ingest/prod/watermarks/Opportunity
#
# The watermark is stored as an ISO-8601 UTC string.
# On the very first run, it defaults to a configurable backfill start date.

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# Returned when no watermark exists yet — triggers a full historical load.
DEFAULT_BACKFILL_START = datetime(2020, 1, 1, tzinfo=timezone.utc)


class WatermarkManager:
    """
    Reads and writes per-object watermarks in SSM Parameter Store.

    Thread safety: each Lambda invocation is single-threaded; no locking needed.
    """

    def __init__(self, ssm_client, param_prefix: str) -> None:
        """
        Args:
            ssm_client:   boto3 SSM client
            param_prefix: e.g. "/salesforce-ingest/prod/watermarks"
        """
        self._ssm    = ssm_client
        self._prefix = param_prefix.rstrip("/")

    def _param_name(self, sf_object: str) -> str:
        return f"{self._prefix}/{sf_object}"

    def get(
        self,
        sf_object: str,
        default: datetime = DEFAULT_BACKFILL_START,
    ) -> datetime:
        """
        Return the last successful extract timestamp for sf_object.
        Falls back to `default` if no watermark exists yet.
        """
        try:
            resp = self._ssm.get_parameter(
                Name=self._param_name(sf_object),
                WithDecryption=True,
            )
            raw = resp["Parameter"]["Value"]
            wm  = datetime.fromisoformat(raw)
            if wm.tzinfo is None:
                wm = wm.replace(tzinfo=timezone.utc)
            log.info("[%s] Watermark: %s", sf_object, wm.isoformat())
            return wm

        except ClientError as e:
            if e.response["Error"]["Code"] == "ParameterNotFound":
                log.warning(
                    "[%s] No watermark found — using backfill start: %s",
                    sf_object,
                    default.isoformat(),
                )
                return default
            raise

    def set(self, sf_object: str, value: datetime) -> None:
        """
        Persist a new watermark.  Uses put_parameter with Overwrite=True
        so repeated calls are idempotent.
        """
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        self._ssm.put_parameter(
            Name=self._param_name(sf_object),
            Value=value.isoformat(),
            Type="String",       # not SecureString — timestamps aren't sensitive
            Overwrite=True,
            Description=f"Salesforce incremental extract watermark for {sf_object}",
        )
        log.info("[%s] Watermark updated → %s", sf_object, value.isoformat())

    def get_all(self, objects: list[str]) -> dict[str, datetime]:
        """Batch-read watermarks for multiple objects in one SSM call."""
        names  = [self._param_name(o) for o in objects]
        result = {}
        # SSM GetParameters accepts up to 10 names per call
        for i in range(0, len(names), 10):
            batch = names[i:i + 10]
            resp  = self._ssm.get_parameters(Names=batch, WithDecryption=True)

            for param in resp["Parameters"]:
                obj_name = param["Name"].split("/")[-1]
                raw      = param["Value"]
                dt       = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                result[obj_name] = dt

            # Fill in missing (first-run) objects
            for name, obj in zip(names[i:i + 10], objects[i:i + 10]):
                if obj not in result:
                    log.warning("[%s] No watermark — using backfill start", obj)
                    result[obj] = DEFAULT_BACKFILL_START

        return result

    def apply_lookback(
        self,
        watermark: datetime,
        lookback_minutes: int = 30,
    ) -> datetime:
        """
        Subtract a lookback buffer from the watermark to guard against:
          • Clock skew between Salesforce servers and AWS
          • Records that are updated slightly before the previous watermark
            due to trigger execution order
          • Fivetran-style "reprocess recent window" behavior

        30 minutes is conservative and safe for most orgs.
        """
        adjusted = watermark - timedelta(minutes=lookback_minutes)
        log.debug("Watermark %s → adjusted %s (-%dm lookback)",
                  watermark.isoformat(), adjusted.isoformat(), lookback_minutes)
        return adjusted
