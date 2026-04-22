-- models/marts/core/dim_account.sql
--
-- Current-state Account dimension.  Full-refresh table materialization —
-- dimensions are relatively small and a full rebuild on each run is safer
-- than trying to merge-manage surrogate keys.
--
-- For point-in-time historical lookups use snap_salesforce__accounts.
-- The account_sk surrogate key is stable: it is a hash of account_id only,
-- so it never changes even if account attributes change.

{{
  config(materialized = 'table')
}}

with accounts as (
  select * from {{ ref('stg_salesforce__accounts') }}
),

owners as (
  select
    user_id,
    full_name    as owner_name,
    department   as owner_department,
    is_active    as owner_is_active
  from {{ ref('stg_salesforce__users') }}
),

parent_accounts as (
  -- Self-join to denormalize parent account name one level up
  select
    account_id     as parent_account_id,
    account_name   as parent_account_name,
    industry       as parent_industry
  from {{ ref('stg_salesforce__accounts') }}
),

final as (

  select
    -- ── Surrogate key ─────────────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['a.account_id']) }}
                                                          as account_sk,

    -- ── Natural key ───────────────────────────────────────────────────────
    a.account_id,

    -- ── Hierarchy ─────────────────────────────────────────────────────────
    a.parent_id                                           as parent_account_id,
    p.parent_account_name,

    -- ── Core descriptors ──────────────────────────────────────────────────
    a.account_name,
    a.account_type,
    a.industry,
    null                                                  as sub_industry,
    a.rating,
    a.account_source,
    a.website,
    a.phone,

    -- ── Firmographics ─────────────────────────────────────────────────────
    a.annual_revenue,
    a.number_of_employees,

    -- ── Segmentation (derived) ────────────────────────────────────────────
    case
      when a.number_of_employees < 50     then 'SMB'
      when a.number_of_employees < 500    then 'Mid-Market'
      when a.number_of_employees < 5000   then 'Enterprise'
      when a.number_of_employees >= 5000  then 'Strategic'
      else 'Unknown'
    end                                                   as employee_segment,

    case
      when a.annual_revenue < 1000000     then 'SMB'
      when a.annual_revenue < 50000000    then 'Mid-Market'
      when a.annual_revenue < 1000000000  then 'Enterprise'
      when a.annual_revenue >= 1000000000 then 'Strategic'
      else 'Unknown'
    end                                                   as revenue_segment,

    -- ── Geography ─────────────────────────────────────────────────────────
    a.billing_street,
    a.billing_city,
    a.billing_state,
    a.billing_postal_code,
    a.billing_country,
    a.billing_latitude,
    a.billing_longitude,

    -- ── Ownership ─────────────────────────────────────────────────────────
    a.owner_id,
    o.owner_name,
    o.owner_department,
    o.owner_is_active,

    -- ── Timestamps ────────────────────────────────────────────────────────
    a.created_at,
    a.last_modified_at,
    a.updated_at,

    -- ── Metadata ──────────────────────────────────────────────────────────
    current_timestamp()                                   as dbt_updated_at

  from accounts a
  left join owners         o on a.owner_id  = o.user_id
  left join parent_accounts p on a.parent_id = p.parent_account_id

)

select * from final
