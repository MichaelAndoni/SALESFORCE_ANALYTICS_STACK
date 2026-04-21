#!/usr/bin/env python3
# cdk/app.py
#
# CDK v2 app entry point.
#
# Usage:
#   cd cdk
#   pip install -r requirements.txt
#   cdk deploy --all --context env=prod

import aws_cdk as cdk
from stacks.ingestion_stack import SalesforceIngestionStack

app = cdk.App()

env_name = app.node.try_get_context("env") or "dev"

SalesforceIngestionStack(
    app,
    f"SalesforceIngest-{env_name.capitalize()}",
    env_name=env_name,
    env=cdk.Environment(
        account=app.node.try_get_context("account")
                or cdk.Aws.ACCOUNT_ID,
        region=app.node.try_get_context("region")
               or cdk.Aws.REGION,
    ),
    description=(
        f"Salesforce → S3 → Snowpipe incremental ingest pipeline ({env_name})"
    ),
)

app.synth()
