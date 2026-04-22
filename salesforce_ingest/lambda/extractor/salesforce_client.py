# lambda/extractor/salesforce_client.py
#
# Salesforce Bulk API 2.0 client.
#
# Bulk API 2.0 vs REST API:
#   REST:  ~200 records/request, rate-limited, good for small/real-time pulls
#   Bulk:  up to 150M records/job, async, no governor limit on rows — the right
#          tool for full or large incremental extracts of Opportunity, Lead, etc.
#
# Auth:  JWT Bearer token flow (Connected App + X.509 cert).
#        No interactive login, no refresh token rotation — ideal for Lambda.
#
# Flow:
#   1. Exchange JWT for access token  (POST /services/oauth2/token)
#   2. Create a Bulk Query job        (POST /services/data/vXX/jobs/query)
#   3. Poll until job is Complete     (GET  /services/data/vXX/jobs/query/{id})
#   4. Stream result pages to S3      (GET  /services/data/vXX/jobs/query/{id}/results)
#   5. Close / abort the job          (DELETE)

from __future__ import annotations

import csv
import io
import json
import logging
import time
from datetime import datetime, timezone
from typing import Generator, Optional

import boto3
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import jwt  # PyJWT

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
API_VERSION    = "59.0"
BULK_ENDPOINT  = "/services/data/v{ver}/jobs/query"
TOKEN_ENDPOINT = "/services/oauth2/token"
POLL_INTERVAL  = 10    # seconds between job status checks
MAX_POLL_WAIT  = 3600  # 1 hour timeout for very large jobs
PAGE_SIZE      = 50_000  # rows per result page

# ---------------------------------------------------------------------------
# Objects and their SOQL field lists
# Extend this dict to add new Salesforce objects.
# SystemModstamp is always included — it drives incremental filtering.
# ---------------------------------------------------------------------------
OBJECT_QUERIES: dict[str, str] = {
    "Account": """
        SELECT
            Id, Name, Type, Industry, Rating, AccountSource,
            Description, Website, Phone, Fax,
            AnnualRevenue, NumberOfEmployees,
            BillingStreet, BillingCity, BillingState, BillingPostalCode,
            BillingCountry, BillingLatitude, BillingLongitude,
            ParentId, MasterRecordId, OwnerId,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp
        FROM Account
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Contact": """
        SELECT
            Id, FirstName, LastName, Name, Salutation, Title, Department,
            AccountId, ReportsToId, OwnerId,
            Email, Phone, MobilePhone, Fax,
            LeadSource, MailingStreet, MailingCity, MailingState,
            MailingPostalCode, MailingCountry,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp,
            LastActivityDate
        FROM Contact
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Lead": """
        SELECT
            Id, FirstName, LastName, Name, Salutation, Title,
            Email, Phone, MobilePhone,
            Company, Industry, AnnualRevenue, NumberOfEmployees, Website,
            Status, LeadSource, Rating,
            Street, City, State, PostalCode, Country,
            IsConverted, ConvertedDate, ConvertedAccountId,
            ConvertedContactId, ConvertedOpportunityId,
            OwnerId,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp,
            LastActivityDate
        FROM Lead
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Opportunity": """
        SELECT
            Id, Name, Type, Description, NextStep, AccountId, OwnerId, CampaignId,
            StageName, ForecastCategory, Amount, ExpectedRevenue, Probability,
            TotalOpportunityQuantity, CloseDate, FiscalQuarter, FiscalYear,
            IsClosed, IsWon, HasOpportunityLineItem, LeadSource,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp,
            LastActivityDate
        FROM Opportunity
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "OpportunityLineItem": """
        SELECT
            Id, OpportunityId, Product2Id, PricebookEntryId,
            Name, ProductCode, Description, Quantity, UnitPrice,
            ListPrice, TotalPrice, ServiceDate,
            SortOrder, IsDeleted, CreatedDate, SystemModstamp
        FROM OpportunityLineItem
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Campaign": """
        SELECT
            Id, Name, Type, Status, ParentId, OwnerId,
            StartDate, EndDate, BudgetedCost, ActualCost,
            ExpectedRevenue, ExpectedResponse, NumberSent,
            NumberOfLeads, NumberOfConvertedLeads, NumberOfContacts,
            NumberOfResponses, NumberOfOpportunities, NumberOfWonOpportunities,
            AmountAllOpportunities, AmountWonOpportunities,
            IsActive, IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp
        FROM Campaign
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "CampaignMember": """
        SELECT
            Id, CampaignId, LeadId, ContactId,
            Status, HasResponded, FirstRespondedDate,
            IsDeleted, CreatedDate, SystemModstamp
        FROM CampaignMember
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Task": """
        SELECT
            Id, Subject, Status, Priority, WhoId, WhatId, OwnerId,
            ActivityDate, Description, IsHighPriority,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp
        FROM Task
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "Event": """
        SELECT
            Id, Subject, WhoId, WhatId, OwnerId,
            ActivityDateTime, EndDateTime, DurationInMinutes,
            Location, Description, IsAllDayEvent, IsPrivate,
            IsDeleted, CreatedDate, LastModifiedDate, SystemModstamp
        FROM Event
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,

    "User": """
        SELECT
            Id, FirstName, LastName, Name, Email, Username, Alias,
            Title, Department, Division, Phone, MobilePhone,
            ManagerId, UserRoleId, ProfileId,
            TimeZoneSidKey, LocaleSidKey,
            IsActive, CreatedDate, LastModifiedDate,
            SystemModstamp, LastLoginDate
        FROM User
        WHERE SystemModstamp >= {watermark}
        ORDER BY SystemModstamp ASC
    """,
}


class SalesforceAuthError(Exception):
    pass


class SalesforceBulkError(Exception):
    pass


class SalesforceClient:
    """
    Thin wrapper around Salesforce Bulk API 2.0.

    Instantiate once per Lambda invocation; the access token lasts 1 hour
    which is plenty for a single incremental extract.
    """

    def __init__(
        self,
        instance_url: str,
        client_id: str,
        username: str,
        private_key_pem: str,   # PEM string, pulled from Secrets Manager
        sandbox: bool = False,
    ) -> None:
        self.instance_url  = instance_url.rstrip("/")
        self.client_id     = client_id
        self.username      = username
        self.private_key   = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        self.sandbox       = sandbox
        self.access_token: Optional[str] = None
        self._session      = requests.Session()

    # ------------------------------------------------------------------
    # Auth: JWT Bearer flow
    # ------------------------------------------------------------------
    def authenticate(self) -> None:
        """
        Exchange a signed JWT assertion for a Salesforce access token.
        No interactive login, no refresh tokens — perfect for Lambda.
        """
        audience = (
            "https://test.salesforce.com"
            if self.sandbox
            else "https://login.salesforce.com"
        )
        now = int(time.time())

        payload = {
            "iss": self.client_id,
            "sub": self.username,
            "aud": audience,
            "exp": now + 300,   # 5-minute assertion TTL
        }

        # Sign the JWT with the private key from Secrets Manager
        assertion = jwt.encode(
            payload,
            self.private_key,
            algorithm="RS256",
        )

        response = self._session.post(
            f"{audience}{TOKEN_ENDPOINT}",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            },
            timeout=30,
        )
        if not response.ok:
            raise SalesforceAuthError(
                f"Auth failed [{response.status_code}]: {response.text}"
            )

        token_data        = response.json()
        self.access_token = token_data["access_token"]
        # Use the instance URL returned by auth — may differ from configured one
        # (e.g. My Domain URLs)
        self.instance_url = token_data.get("instance_url", self.instance_url)
        log.info("Salesforce auth successful. Instance: %s", self.instance_url)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

    def _bulk_url(self, path: str = "") -> str:
        base = BULK_ENDPOINT.format(ver=API_VERSION)
        return f"{self.instance_url}{base}{path}"

    def _raise_for_status(self, response: requests.Response) -> None:
        if not response.ok:
            raise SalesforceBulkError(
                f"Bulk API error [{response.status_code}] "
                f"on {response.url}: {response.text[:500]}"
            )

    # ------------------------------------------------------------------
    # Bulk API 2.0 job lifecycle
    # ------------------------------------------------------------------
    def _create_query_job(self, soql: str) -> str:
        """Submit a Bulk Query job and return the job ID."""
        body = {
            "operation":     "query",
            "query":         soql,
            "contentType":   "CSV",     # CSV is most universally compatible
            "columnDelimiter": "COMMA",
            "lineEnding":    "LF",
        }
        resp = self._session.post(
            self._bulk_url(), headers=self._headers(), json=body, timeout=30
        )
        self._raise_for_status(resp)
        job_id = resp.json()["id"]
        log.info("Created Bulk job %s", job_id)
        return job_id

    def _poll_job(self, job_id: str) -> dict:
        """Poll until job reaches a terminal state; return final job info."""
        deadline = time.time() + MAX_POLL_WAIT
        while time.time() < deadline:
            resp = self._session.get(
                self._bulk_url(f"/{job_id}"),
                headers=self._headers(),
                timeout=30,
            )
            self._raise_for_status(resp)
            info  = resp.json()
            state = info.get("state")

            if state == "JobComplete":
                log.info(
                    "Job %s complete. Rows: %s",
                    job_id,
                    info.get("numberRecordsProcessed"),
                )
                return info
            elif state in ("Failed", "Aborted"):
                raise SalesforceBulkError(
                    f"Bulk job {job_id} ended in state {state}: "
                    f"{info.get('errorMessage', 'no detail')}"
                )
            else:
                log.debug("Job %s state: %s — waiting %ss", job_id, state, POLL_INTERVAL)
                time.sleep(POLL_INTERVAL)

        raise SalesforceBulkError(f"Job {job_id} timed out after {MAX_POLL_WAIT}s")

    def _stream_results(self, job_id: str) -> Generator[list[dict], None, None]:
        """
        Yield pages of records (each page is a list of dicts) from the
        Bulk API result set.  Pages are streamed to avoid loading the
        entire result set into Lambda memory.
        """
        locator: Optional[str] = None

        while True:
            params = {"maxRecords": PAGE_SIZE}
            if locator:
                params["locator"] = locator

            resp = self._session.get(
                self._bulk_url(f"/{job_id}/results"),
                headers={**self._headers(), "Accept": "text/csv"},
                params=params,
                timeout=120,
                stream=True,
            )
            self._raise_for_status(resp)

            # Parse CSV page
            content = resp.content.decode("utf-8-sig")  # strip BOM if present
            reader  = csv.DictReader(io.StringIO(content))
            rows    = list(reader)
            if rows:
                yield rows

            # Check for next page
            locator = resp.headers.get("Sforce-Locator")
            if not locator or locator == "null":
                break

    def _close_job(self, job_id: str) -> None:
        self._session.delete(
            self._bulk_url(f"/{job_id}"),
            headers=self._headers(),
            timeout=30,
        )
        log.info("Deleted Bulk job %s", job_id)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def extract_object(
        self,
        sf_object: str,
        watermark: datetime,
        s3_client,
        s3_bucket: str,
        s3_prefix: str,
        run_ts: datetime,
    ) -> int:
        """
        Extract one Salesforce object incrementally and write Newline-Delimited
        JSON (NDJSON) pages to S3.

        Returns the total number of records written.
        """
        if sf_object not in OBJECT_QUERIES:
            raise ValueError(f"No SOQL template defined for object: {sf_object}")

        # Format watermark as Salesforce datetime literal  e.g. 2026-04-17T00:00:00.000Z
        wm_str = watermark.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
        soql   = OBJECT_QUERIES[sf_object].format(watermark=wm_str).strip()
        log.info("[%s] SOQL: %s", sf_object, soql[:200])

        job_id  = self._create_query_job(soql)
        job_info = self._poll_job(job_id)

        total_records = 0
        page_num      = 0

        try:
            for page_rows in self._stream_results(job_id):
                page_num += 1

                # Enrich each row with pipeline metadata
                enriched = []
                for row in page_rows:
                    row["_extract_timestamp"] = run_ts.isoformat()
                    row["_source_object"]     = sf_object
                    enriched.append(row)

                # Write NDJSON page to S3
                # Partition layout:  {prefix}/{object}/date={YYYY-MM-DD}/{object}_{page:04d}.json
                date_partition = run_ts.strftime("%Y-%m-%d")
                s3_key = (
                    f"{s3_prefix.rstrip('/')}/{sf_object.lower()}/"
                    f"date={date_partition}/"
                    f"{sf_object.lower()}_{run_ts.strftime('%H%M%S')}_{page_num:04d}.json"
                )

                ndjson_body = "\n".join(json.dumps(r, default=str) for r in enriched)

                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=ndjson_body.encode("utf-8"),
                    ContentType="application/x-ndjson",
                    Metadata={
                        "sf-object":    sf_object,
                        "run-ts":       run_ts.isoformat(),
                        "watermark":    wm_str,
                        "record-count": str(len(enriched)),
                        "page":         str(page_num),
                    },
                )

                total_records += len(enriched)
                log.info("[%s] Page %d → s3://%s/%s (%d rows)", sf_object, page_num, s3_bucket, s3_key, len(enriched))

        finally:
            self._close_job(job_id)

        log.info("[%s] Total extracted: %d records in %d pages", sf_object, total_records, page_num)
        return total_records
