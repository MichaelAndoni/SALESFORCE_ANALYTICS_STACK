# salesforce_ingest — AWS-native Salesforce → Snowflake pipeline

Self-managed alternative to Fivetran/Airbyte.  
Full incremental extract pipeline using Salesforce Bulk API 2.0, Lambda, S3, and Snowpipe.

```
Salesforce Bulk API 2.0
        │
        ▼
Lambda (hourly, EventBridge)
  • JWT Bearer auth (Connected App + X.509)
  • Watermark per object → SSM Parameter Store
  • 30-min lookback buffer on every run
        │
        ▼
S3 Landing Bucket  (NDJSON, partitioned by object/date)
  s3://sf-ingest-prod-landing-{account}/salesforce/raw/
    opportunity/date=2026-04-20/opportunity_120000_0001.json
    account/date=2026-04-20/account_120000_0001.json
        │  (S3 event → SQS)
        ▼
Snowpipe  (AUTO_INGEST via SQS, near real-time)
  COPY INTO RAW.SALESFORCE.OPPORTUNITY ...
        │
        ▼
Dedup Views  (RAW.SALESFORCE.V_OPPORTUNITY)
  Latest row per ID; ROW_NUMBER() on SYSTEMMODSTAMP
        │
        ▼
dbt staging models  (read from V_* views)
  stg_salesforce__opportunities → ... → fct_bookings
```

---

## Prerequisites

- AWS account with CDK bootstrapped (`cdk bootstrap`)
- Snowflake account with `SYSADMIN` / `ACCOUNTADMIN` access
- Salesforce org with API access (Connected App configured)
- Python 3.12

---

## Setup sequence

### 1. Salesforce Connected App (one-time)

Generate a self-signed certificate and configure a Connected App in Salesforce:

```bash
# Generate RSA private key and self-signed cert
openssl genrsa -out sf_private_key.pem 2048
openssl req -new -x509 -key sf_private_key.pem \
    -out sf_certificate.crt -days 3650 \
    -subj "/CN=salesforce-extractor"
```

In Salesforce Setup → App Manager → New Connected App:
- Enable OAuth
- Select "Use digital signatures" → upload `sf_certificate.crt`
- Scopes: `api`, `refresh_token`
- Enable "JWT Bearer Token Flow"
- Note the **Consumer Key** (= `SF_CLIENT_ID`)

Pre-authorize the service account user:
- Setup → Connected Apps OAuth Usage → Manage Profiles → add the service user's profile

### 2. Deploy CDK infrastructure

```bash
cd cdk
pip install -r requirements.txt
cdk bootstrap aws://ACCOUNT_ID/us-east-1

# Required context values (or set as env vars and reference in CDK)
cdk deploy \
  --context env=prod \
  --context sf_instance_url=https://yourorg.my.salesforce.com \
  --context sf_username=svc-extractor@yourorg.com \
  --context sf_client_id=3MVG9... \
  --context snowflake_account=xy12345.us-east-1 \
  --context snowflake_user=dbt_loader
```

Note the CDK outputs:
- `LandingBucketName`
- `SnowflakeRoleArn`
- `SnowpipeQueueArn`

### 3. Store Salesforce private key in Secrets Manager

```bash
# The secret must be JSON: {"private_key_pem": "-----BEGIN RSA PRIVATE KEY-----\n..."}
python -c "
import json, boto3
key = open('sf_private_key.pem').read()
boto3.client('secretsmanager').update_secret(
    SecretId='sf-ingest-prod/salesforce-credentials',
    SecretString=json.dumps({'private_key_pem': key})
)
print('Secret updated')
"
```

### 4. Set up Snowflake

Run the SQL files in order:

```bash
# Using SnowSQL CLI
snowsql -a YOUR_ACCOUNT -u ACCOUNTADMIN_USER \
    -f snowflake/01_storage_integration.sql

# After step 1: DESCRIBE INTEGRATION SF_S3_INTEGRATION;
# → Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
# → Update the CDK SnowflakeRole trust policy (see AWS Console → IAM)
# → Re-deploy: cdk deploy (or update trust policy manually)

snowsql -a YOUR_ACCOUNT -u SYSADMIN_USER \
    -f snowflake/02_schema_stage_format.sql \
    -f snowflake/03_raw_tables.sql \
    -f snowflake/04_snowpipes.sql \
    -f snowflake/05_dedup_tasks.sql \
    -f snowflake/06_dedup_views.sql
```

Verify Snowpipe connectivity:
```sql
SELECT SYSTEM$PIPE_STATUS('SALESFORCE.PIPE_OPPORTUNITY');
-- Expected: {"executionState":"RUNNING","pendingFileCount":0}
```

### 5. Update dbt sources

Replace the Fivetran `_sources.yml` in your dbt project with
`snowflake/07_dbt_sources_update.yml` — it points `identifier` at the `V_*`
dedup views instead of raw tables.

### 6. Initial backfill

```bash
# Dry run first — logs SOQL but writes nothing
cd scripts
python ops.py invoke-extractor --dry-run

# Full historical backfill from 2020-01-01
python ops.py backfill --start 2020-01-01T00:00:00Z

# Single object backfill
python ops.py backfill --object Opportunity --start 2020-01-01T00:00:00Z
```

### 7. Wire dbt and verify end-to-end

```bash
cd ../salesforce_dbt
dbt deps
dbt seed
dbt snapshot
dbt run --select staging
dbt test --select staging
dbt run
dbt test
```

---

## How incremental loading works

```
Run N:   watermark = 2026-04-19T12:00:00Z
  → Lambda applies 30-min lookback: extracts from 2026-04-19T11:30:00Z
  → Writes files to S3 (records modified after 11:30)
  → Snowpipe loads files into RAW.SALESFORCE.*
  → Lambda writes new watermark: 2026-04-19T13:00:00Z (run_ts)

Run N+1: watermark = 2026-04-19T13:00:00Z
  → Lookback: extracts from 2026-04-19T12:30:00Z
  → 30-minute overlap with Run N intentional: catches late-arriving records
  → Snowpipe COPY INTO skips files already loaded (idempotent by content hash)
  → Dedup views resolve duplicates: latest SYSTEMMODSTAMP wins
```

---

## Operational commands

```bash
# Check all watermarks
python scripts/ops.py show-watermarks

# Reset a watermark (triggers re-extract from that point on next run)
python scripts/ops.py reset-watermark --object Opportunity --to 2026-01-01

# Trigger an immediate out-of-schedule extract
python scripts/ops.py invoke-extractor --objects Opportunity,Account

# Get Snowflake SQL to manually refresh a stalled pipe
python scripts/ops.py refresh-pipe --pipe PIPE_OPPORTUNITY
```

---

## Cost profile (rough estimates, prod scale)

| Component | Cost driver | Est. monthly |
|---|---|---|
| Lambda | 720 invocations/month × 5min avg × 1GB | ~$5 |
| S3 | 50GB raw data + PUT requests | ~$5 |
| SQS | ~720 messages/month (one per run) | <$1 |
| Snowpipe | Credits consumed per file loaded | $10–50 |
| Snowflake dedup task | XSMALL × ~5min/hour | ~$15 |
| CloudWatch | Logs + metrics | ~$5 |
| **Total** | | **~$40–80/mo** |

Compare: Fivetran Starter plan starts at ~$500/month for similar volume.

---

## Adding a new Salesforce object

1. Add SOQL query to `OBJECT_QUERIES` in `lambda/extractor/salesforce_client.py`
2. Add the object name to `SNOWFLAKE_OBJECTS` in `cdk/stacks/ingestion_stack.py`
3. Create raw table in `snowflake/03_raw_tables.sql`
4. Create Snowpipe in `snowflake/04_snowpipes.sql`
5. Create dedup view in `snowflake/06_dedup_views.sql`
6. Add dbt staging model and source entry
7. `cdk deploy && dbt run --select stg_salesforce__new_object`

---

## Monitoring

- **Lambda errors** → CloudWatch alarm → SNS → email/PagerDuty
- **SQS DLQ depth** → CloudWatch alarm (means S3→SQS notifications failing)
- **Snowpipe lag** → `SYSTEM$PIPE_STATUS` via pipe monitor Lambda → CloudWatch custom metrics
- **dbt freshness** → `dbt source freshness` checks `_load_timestamp` on all sources
- **Data quality** → dbt tests run after every `dbt build`

---

## Files

```
salesforce_ingest/
├── lambda/
│   ├── extractor/
│   │   ├── handler.py              Lambda entry point
│   │   ├── salesforce_client.py    Bulk API 2.0 client + SOQL definitions
│   │   ├── watermark.py            SSM watermark read/write
│   │   └── requirements.txt
│   └── pipe_monitor/
│       └── handler.py              Snowpipe health → CloudWatch metrics
├── cdk/
│   ├── app.py                      CDK app entry point
│   ├── requirements.txt
│   └── stacks/
│       └── ingestion_stack.py      Full AWS infrastructure (S3, SQS, Lambda,
│                                   EventBridge, IAM, Secrets, SSM, Alarms)
├── snowflake/
│   ├── 01_storage_integration.sql  Storage + SQS integrations (ACCOUNTADMIN)
│   ├── 02_schema_stage_format.sql  RAW.SALESFORCE schema, stage, file format
│   ├── 03_raw_tables.sql           10 raw tables (VARIANT + typed columns)
│   ├── 04_snowpipes.sql            10 Snowpipes with AUTO_INGEST
│   ├── 05_dedup_tasks.sql          Optional: Snowflake Tasks for in-place dedup
│   ├── 06_dedup_views.sql          V_* views — latest row per ID
│   └── 07_dbt_sources_update.yml   Drop-in _sources.yml for dbt project
└── scripts/
    └── ops.py                      CLI: backfills, watermark resets, pipe ops
```
