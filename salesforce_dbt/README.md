# salesforce_dbt

A production-grade dbt project that incrementally loads Salesforce data into
Snowflake, transforming raw CRM records into an analytics-ready dimensional
model covering the full sales and marketing funnel.

---

## Architecture overview

```
RAW.SALESFORCE          (Fivetran / Airbyte lands Salesforce here)
        │
        ▼
ANALYTICS.STG_SALESFORCE        Staging layer  — incremental, merge strategy
        │                        Light transforms: rename, cast, exclude deletes
        │
        ├──────────────────────► ANALYTICS.SNAPSHOTS
        │                        SCD Type-2 history (dbt snapshots)
        │                        snap_salesforce__accounts
        │                        snap_salesforce__contacts
        │                        snap_salesforce__leads
        │                        snap_salesforce__opportunities
        │
        ▼
    [ephemeral]                  Intermediate layer — inlined at compile time
    int_salesforce__*            Enrichment joins (opportunity + account +
                                 owner + campaign + line items)
        │
        ▼
ANALYTICS.MARTS_CORE             Dimension tables (full-refresh tables)
        dim_account              Firmographic segmentation, hierarchy
        dim_contact              Contact → Account enrichment
        dim_lead                 Lead with campaign attribution
        dim_user                 Rep / manager hierarchy
        dim_campaign             Budget, schedule, UTM metadata
        dim_date                 Calendar + fiscal spine 2020–2030

        ▼
ANALYTICS.MARTS_SALES            Sales fact tables (incremental merge)
        fct_sales_pipeline       Open pipeline, weighted amounts, aging
        fct_bookings             Closed-won bookings ledger (immutable)
        fct_win_rate             All closed opps for win/loss analysis

        ▼
ANALYTICS.MARTS_MARKETING        Marketing fact tables (incremental merge)
        fct_campaign_performance Campaign-grain: reach, cost, pipeline ROI
        fct_demand_generation    Member-grain: full MQL → SQL → Customer funnel
```

---

## Quickstart

### Prerequisites

- dbt-core >= 1.7
- dbt-snowflake adapter
- Python >= 3.9
- Snowflake account with a dedicated transformer role and warehouse

### 1. Install dbt

```bash
pip install dbt-snowflake
```

### 2. Configure your profile

Copy `profiles.yml` to `~/.dbt/profiles.yml` and set environment variables:

```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=dbt_transformer
export SNOWFLAKE_PASSWORD=••••••••
```

### 3. Install packages

```bash
cd salesforce_dbt
dbt deps
```

### 4. Seed reference data

```bash
dbt seed
```

### 5. Run snapshots (SCD2 history)

Snapshots must run **before** marts so history is current:

```bash
dbt snapshot
```

### 6. Run the full pipeline

```bash
dbt run
```

### 7. Test

```bash
dbt test
```

### 8. Generate docs

```bash
dbt docs generate && dbt docs serve
```

---

## Incremental strategy

All staging and mart models use Snowflake's **MERGE** strategy:

```sql
-- Incremental filter pattern used in every staging model
{% if is_incremental() %}
  where system_modstamp >= (
    select dateadd('day', -{{ var('incremental_lookback_days') }},
                   max(updated_at))
    from {{ this }}
  )
{% endif %}
```

The `incremental_lookback_days` variable (default: **3**) creates an overlap
window that reprocesses recently modified records.  This guards against:
- Fivetran batch re-syncs
- Late-arriving CDC events
- Clock skew between Salesforce and Fivetran

### Full refresh

To rebuild a model from scratch (e.g. after a schema change):

```bash
dbt run --full-refresh --select stg_salesforce__opportunities
```

To rebuild everything:

```bash
dbt run --full-refresh
```

---

## SCD Type-2 Snapshots

Snapshots capture a full history of changes for the four most analytically
important Salesforce objects.

| Snapshot | Unique key | Key use-cases |
|---|---|---|
| `snap_salesforce__accounts` | account_id | Owner / industry changes over time |
| `snap_salesforce__contacts` | contact_id | Account reassignment history |
| `snap_salesforce__leads` | lead_id | Status progression, conversion timing |
| `snap_salesforce__opportunities` | opportunity_id | Stage progression, pipeline waterfall, forecast accuracy |

Each snapshot row has dbt-generated metadata:

| Column | Meaning |
|---|---|
| `dbt_scd_id` | Unique surrogate per snapshot row |
| `dbt_valid_from` | When this version became current |
| `dbt_valid_to` | When this version was superseded (NULL = current) |
| `dbt_updated_at` | Source `updated_at` that triggered the new row |

**Point-in-time join pattern:**

```sql
-- What stage was opportunity X in on 2025-03-31?
select *
from analytics.snapshots.snap_salesforce__opportunities
where opportunity_id = '0062X000...'
  and '2025-03-31' between dbt_valid_from and coalesce(dbt_valid_to, current_date())
```

---

## Fact tables

### fct_sales_pipeline
Open pipeline snapshot. Rebuilt incrementally on each run.  
**Grain:** one row per open opportunity.  
**Key metrics:** `amount`, `weighted_amount`, `days_to_close`, `is_current_quarter_close`, `is_stale`.

### fct_bookings
Immutable closed-won bookings ledger.  
**Grain:** one row per won opportunity.  
**Key metrics:** `booking_amount`, `arr_booked`, `tcv_booked`, `days_to_close`, `booking_category`.

### fct_win_rate
All closed (won + lost) opportunities.  
**Grain:** one row per closed opportunity.  
**Usage:** `select sum(won_count) / count(*) as win_rate, owner_name from fct_win_rate group by owner_name`.

### fct_campaign_performance
Campaign-level aggregates.  
**Grain:** one row per campaign.  
**Key metrics:** `response_rate_pct`, `cost_per_lead`, `pipeline_roi`, `revenue_roi`.

### fct_demand_generation
Full MQL → SQL → Customer funnel at person × campaign grain.  
**Grain:** one row per campaign member (lead or contact).  
**Key metrics:** `is_mql`, `is_sql`, `is_customer`, `days_to_respond`, `days_member_to_opportunity`.

---

## Dimension tables

| Dimension | Grain | Key enrichments |
|---|---|---|
| `dim_account` | Account | Employee/revenue segmentation, parent hierarchy, owner |
| `dim_contact` | Contact | Account name/industry, owner, opt-out flags |
| `dim_lead` | Lead | Multi-touch campaign attribution, conversion chain |
| `dim_user` | User | Manager hierarchy, department |
| `dim_campaign` | Campaign | Budget variance, channel, UTM, parent campaign |
| `dim_date` | Date | Fiscal calendar, rolling period helpers, boolean flags |

---

## Surrogate keys

All dimension tables use `dbt_utils.generate_surrogate_key` to produce
stable MD5-based surrogate keys.  Fact tables carry both the surrogate key FK
(`account_sk`, `owner_sk`, etc.) **and** the Salesforce natural ID
(`account_id`, `owner_id`) for convenience.

---

## Seeds

| Seed | Purpose |
|---|---|
| `salesforce_stage_order` | Canonical stage ordering, category, open/closed flags |
| `bookings_targets` | Quarterly bookings and ARR targets — update each planning cycle |

---

## Custom macros

| Macro | Purpose |
|---|---|
| `generate_schema_name` | Routes models to correct Snowflake schemas (avoids `target_schema_custom_schema` default) |
| `incremental_filter` | Reusable incremental WHERE clause with lookback window |
| `safe_amount` | Null-safe numeric cast to `number(38,2)` |
| `grant_select_to_reporters` | Post-hook GRANT for Snowflake RBAC |
| `grant_all_schemas_to_role` | One-shot grant for all mart schemas |
| `test_not_negative` | Generic dbt test for non-negative numeric columns |

---

## Custom tests

| Test | Assertion |
|---|---|
| `assert_no_amount_without_close_date` | Every pipeline deal with amount > 0 must have a close date |
| `assert_closed_won_not_in_pipeline` | Won bookings must not appear in open pipeline |
| `assert_bookings_in_win_rate` | Every booking must have a matching win_rate row |

---

## Customizing for your Salesforce org

### Custom fields

Salesforce custom fields appear in Fivetran as `field_name__c` (double
underscore).  They are aliased in staging models:

```sql
lead_score__c::integer   as lead_score,
lead_grade__c            as lead_grade,
arr__c::number(38, 2)    as arr,
```

Add your org's custom fields to the relevant staging model.  If a field does
not exist in your org, simply remove the line — the incremental merge will
handle the schema change on the next `dbt run --full-refresh`.

### Fiscal year offset

If your fiscal year starts in a month other than January, update `dim_date.sql`:

```sql
-- Example: fiscal year starts in February (offset = 1 month)
month_add(date_day, -1)  -- subtract 1 month before extracting month/quarter/year
```

Or use `dbt_date`'s fiscal calendar functions directly.

### Opportunity stages

Edit `seeds/salesforce_stage_order.csv` to match your org's exact stage names,
then `dbt seed --select salesforce_stage_order`.

### Warehouse sizing

Large full-refresh runs (e.g. first-time historical load) should use a larger
warehouse.  Override at runtime:

```bash
dbt run --full-refresh --vars '{"snowflake_warehouse": "TRANSFORMING_LARGE"}'
```

---

## DAG overview (simplified)

```
sources (RAW.SALESFORCE)
  └─► stg_salesforce__accounts         ──────────────────────────────┐
  └─► stg_salesforce__contacts                                        │
  └─► stg_salesforce__leads            ──────────────────────────────┤
  └─► stg_salesforce__opportunities    ──────────────────────────────┤
  └─► stg_salesforce__opportunity_line_items                          │
  └─► stg_salesforce__campaigns        ──────────────────────────────┤
  └─► stg_salesforce__campaign_members                                │
  └─► stg_salesforce__users            ──────────────────────────────┤
                                                                      │
         ┌─────────────────────────────────────────────────────────  ┘
         │ snapshots: snap_salesforce__*  (SCD2)
         │
         ├─► int_salesforce__opportunity_enriched  (ephemeral)
         │   ├─► fct_sales_pipeline
         │   ├─► fct_bookings
         │   └─► fct_win_rate
         │
         ├─► int_salesforce__lead_enriched  (ephemeral)
         │   └─► dim_lead
         │
         ├─► dim_account
         ├─► dim_contact
         ├─► dim_user
         ├─► dim_campaign
         ├─► dim_date
         │
         ├─► fct_campaign_performance
         └─► fct_demand_generation
```

---

## Run order (CI / orchestration)

Recommended run order for Airflow / MWAA / Astronomer:

```
1. dbt deps
2. dbt seed
3. dbt snapshot              # SCD2 history — must precede marts
4. dbt run --exclude tag:marts
5. dbt run --select tag:marts
6. dbt test
7. dbt docs generate
```

Or in a single command (dbt handles DAG ordering automatically):

```bash
dbt build   # runs seeds + snapshots + models + tests in dependency order
```
