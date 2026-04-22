-- models/intermediate/salesforce/int_salesforce__lead_enriched.sql
--
-- Enriches Lead with owner context and campaign attribution.
-- Materialized as EPHEMERAL.

with leads as (
  select * from {{ ref('stg_salesforce__leads') }}
),

users as (
  select
    user_id,
    full_name    as owner_name,
    department   as owner_department
  from {{ ref('stg_salesforce__users') }}
),

campaigns as (
  select
    campaign_id,
    campaign_name,
    campaign_type,
    null as channel
  from {{ ref('stg_salesforce__campaigns') }}
),

-- Find the most recent campaign membership for each lead
latest_campaign_membership as (
  select
    lead_id,
    campaign_id                                           as last_campaign_id,
    member_status                                         as last_campaign_status,
    has_responded,
    created_at                                            as campaign_member_created_at,
    row_number() over (
      partition by lead_id
      order by created_at desc
    )                                                     as rn
  from {{ ref('stg_salesforce__campaign_members') }}
  where lead_id is not null
),

enriched as (
  select
    -- ── Lead core ────────────────────────────────────────────────────────
    l.lead_id,
    l.full_name,
    l.first_name,
    l.last_name,
    l.email,
    l.phone,
    l.title,
    l.company,
    l.industry,
    l.annual_revenue,
    l.number_of_employees,
    l.lead_source,
    l.status,
    l.rating,
    null                                                  as lead_score,
    null                                                  as lead_grade,
    l.city,
    l.state,
    l.country,

    -- ── Conversion ────────────────────────────────────────────────────────
    l.is_converted,
    l.converted_date,
    l.converted_account_id,
    l.converted_contact_id,
    l.converted_opportunity_id,

    -- ── Timestamps ────────────────────────────────────────────────────────
    l.created_at,
    l.updated_at,
    l.last_activity_date,
    -- Days from lead creation to conversion (null if not yet converted)
    datediff('day', l.created_at, l.converted_date)      as days_to_convert,

    -- ── Owner context ─────────────────────────────────────────────────────
    l.owner_id,
    u.owner_name,
    u.owner_department,

    -- ── Original campaign attribution ─────────────────────────────────────
    null                                                  as original_campaign_id,
    null                                                  as original_campaign_name,
    null                                                  as original_campaign_type,
    null                                                  as original_campaign_channel,

    -- ── Most recent campaign touch ─────────────────────────────────────────
    lcm.last_campaign_id,
    c_last.campaign_name                                  as last_campaign_name,
    lcm.last_campaign_status,
    lcm.has_responded                                     as responded_to_last_campaign

  from leads l
  left join users      u     on l.owner_id             = u.user_id
  left join latest_campaign_membership lcm
    on l.lead_id = lcm.lead_id and lcm.rn = 1
  left join campaigns c_last on lcm.last_campaign_id   = c_last.campaign_id
)

select * from enriched
