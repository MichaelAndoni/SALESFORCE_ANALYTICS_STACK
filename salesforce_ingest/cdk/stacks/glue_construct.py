# cdk/stacks/glue_construct.py
#
# Adds a Glue Python Shell job to the ingestion stack for high-volume extracts.
# This is a separate construct so it can be optionally added to the main stack:
#
#   from stacks.glue_construct import GlueBulkExtractJob
#   GlueBulkExtractJob(self, "GlueExtract", landing_bucket=..., sf_secret=..., ...)

from __future__ import annotations

from aws_cdk import (
    Duration,
    aws_glue         as glue,
    aws_glue_alpha   as glue_alpha,   # L2 constructs (requires aws-cdk.aws-glue-alpha)
    aws_iam          as iam,
    aws_s3           as s3,
    aws_s3_assets    as assets,
    aws_secretsmanager as sm,
)
from constructs import Construct

SALESFORCE_OBJECTS = [
    "Account", "Contact", "Lead", "Opportunity",
    "OpportunityLineItem", "Campaign", "CampaignMember",
    "Task", "Event", "User",
]


class GlueBulkExtractJob(Construct):
    """
    Glue Python Shell job for large-volume / backfill Salesforce extracts.

    Designed to be invoked manually or from Step Functions, not on a schedule.
    The hourly Lambda handles routine incremental pulls; Glue handles:
      • Initial historical backfill (2020-present, all objects in parallel)
      • Out-of-schedule forced re-extracts
      • Objects too large for a 15-minute Lambda window
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name:       str,
        landing_bucket: s3.Bucket,
        sf_secret:      sm.Secret,
        ssm_prefix:     str,
        sf_instance_url: str,
        sf_username:    str,
        sf_client_id:   str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        prefix = f"sf-ingest-{env_name}"
        stack  = scope  # parent stack for account/region

        # ── IAM role for the Glue job ─────────────────────────────────────
        glue_role = iam.Role(
            self, "GlueRole",
            role_name=f"{prefix}-glue-bulk-extract",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # S3: write to landing bucket + read Glue script from assets bucket
        landing_bucket.grant_read_write(glue_role)

        # SSM: read/write watermarks
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter", "ssm:GetParameters", "ssm:PutParameter"
                ],
                resources=[
                    f"arn:aws:ssm:*:*:parameter{ssm_prefix}/*"
                ],
            )
        )

        # Secrets Manager: Salesforce private key
        sf_secret.grant_read(glue_role)

        # CloudWatch: write custom metrics
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # ── Upload the Glue script as a CDK asset ─────────────────────────
        # CDK automatically uploads the script to a CDK-managed S3 bucket.
        script_asset = assets.Asset(
            self, "GlueScript",
            path="../glue/salesforce_bulk_extract.py",
        )
        script_asset.grant_read(glue_role)

        # ── Lambda extractor code as extra-py-files (shared Bulk API client) ─
        extractor_asset = assets.Asset(
            self, "ExtractorAsset",
            path="../lambda/extractor",
        )
        extractor_asset.grant_read(glue_role)

        # ── Glue job definition (Python Shell, not Spark) ─────────────────
        # Python Shell jobs run on a single instance — cheaper for I/O-bound work.
        # Max DPUs for Python Shell = 1 (0.0625 DPU = 1/16 DPU, costs ~$0.044/hr).
        self.glue_job = glue.CfnJob(
            self, "BulkExtractJob",
            name=f"{prefix}-bulk-extract",
            description=(
                "Salesforce Bulk API 2.0 → S3 extractor for large volumes / backfills. "
                "Use this instead of Lambda when extracting > 5M records."
            ),
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",           # Python Shell (not Spark)
                python_version="3",
                script_location=f"s3://{script_asset.s3_bucket_name}/{script_asset.s3_object_key}",
            ),
            default_arguments={
                "--S3_BUCKET":          landing_bucket.bucket_name,
                "--S3_PREFIX":          "salesforce/raw",
                "--SF_SECRET_NAME":     sf_secret.secret_name,
                "--SSM_PREFIX":         ssm_prefix,
                "--SF_INSTANCE_URL":    sf_instance_url,
                "--SF_USERNAME":        sf_username,
                "--SF_CLIENT_ID":       sf_client_id,
                "--SF_SANDBOX":         "true" if env_name != "prod" else "false",
                "--OBJECTS":            "ALL",
                "--WATERMARK_SOURCE":   "ssm",
                "--LOOKBACK_MINUTES":   "30",
                "--PARALLEL_OBJECTS":   "true",
                # Glue standard args
                "--enable-job-insights":       "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics":            "true",
                "--extra-py-files":            f"s3://{extractor_asset.s3_bucket_name}/{extractor_asset.s3_object_key}",
                "--additional-python-modules": "requests==2.32.3,PyJWT==2.9.0,cryptography==43.0.3",
            },
            max_capacity=1,              # 1 DPU for Python Shell
            max_retries=1,
            timeout=180,                 # 3 hours max (for very large full loads)
            glue_version="4.0",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1    # Prevent parallel runs colliding on watermarks
            ),
        )
