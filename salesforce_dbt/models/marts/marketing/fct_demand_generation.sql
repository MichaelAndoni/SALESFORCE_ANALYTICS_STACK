-- models/marts/marketing/fct_demand_generation.sql
--
-- GRAIN: One row per Campaign Member (person × campaign interaction).
--
-- Purpose: Full top-of-funnel demand generation tracking — from first
-- campaign touch through MQL → SQL → Opportunity creation → Conversion.
-- This is the foundation for funnel velocity, stage conversion, and
-- multi-touch attribution analysis.
--
-- Key design choices:
--  • Lead and Contact members are unioned under a single person model.
--  • We carry the conversion outcome on the lead row even if the person
--    became a contact (by joining to the lead's conversion metadata).
--  • Opportunity is attributed if the converted_opportunity_id matches
--    an opportunity with the same campaign_id, OR if the opportunity
--    directly references this campaign.

{{
  config(
    unique_key = 'demand_gen_id'
  )
}}

with campaign_members as (
  select * from {{ ref('stg_salesforce__campaign_members') }}
),

leads as (
  select * from {{ ref('stg_salesforce__leads') }}
),

contacts as (
  select * from {{ ref('stg_salesforce__contacts') }}
),

campaigns as (
  select * from {{ ref('stg_salesforce__campaigns') }}
),

opportunities as (
  select * from {{ ref('stg_salesforce__opportunities') }}
),

-- Resolve the person record for each member (lead or contact)
members_with_person as (
  select
    cm.campaign_member_id,
    cm.campaign_id,
    cm.lead_id,
    cm.contact_id,
    cm.person_id,
    cm.member_type,
    cm.member_status,
    cm.has_responded,
    cm.first_responded_date,
    cm.created_at                                         as member_created_at,

    -- ── Lead attributes (null for contact members) ────────────────────────
    l.email                                               as lead_email,
    l.full_name                                           as lead_name,
    l.company                                             as lead_company,
    l.title                                               as lead_title,
    l.industry                                            as lead_industry,
    l.annual_revenue                                      as lead_annual_revenue,
    l.number_of_employees                                 as lead_employees,
    l.lead_source,
    l.status                                              as lead_status,
    l.rating                                              as lead_rating,
    l.lead_score,
    l.country                                             as lead_country,
    l.state                                               as lead_state,
    l.owner_id                                            as lead_owner_id,
    l.original_campaign_id,
    l.created_at                                          as lead_created_at,

    -- ── Lead conversion ────────────────────────────────────────────────────
    l.is_converted,
    l.converted_date,
    l.converted_opportunity_id,
    l.converted_account_id,
    l.converted_contact_id,

    -- ── Contact attributes (null for lead members) ────────────────────────
    c.account_id                                          as contact_account_id,
    c.full_name                                           as contact_name,
    c.email                                               as contact_email,
    c.title                                               as contact_title,
    c.lead_source                                         as contact_lead_source,
    c.owner_id                                            as contact_owner_id,
    c.created_at                                          as contact_created_at

  from campaign_members cm
  left join leads    l on cm.lead_id    = l.lead_id
  left join contacts c on cm.contact_id = c.contact_id
),

-- Join campaign context
members_with_campaign as (
  select
    mwp.*,
    ca.campaign_name,
    ca.campaign_type,
    ca.channel,
    ca.utm_source,
    ca.utm_medium,
    ca.start_date                                         as campaign_start_date,
    ca.end_date                                           as campaign_end_date,
    ca.budgeted_cost                                      as campaign_budgeted_cost,
    ca.actual_cost                                        as campaign_actual_cost
  from members_with_person mwp
  join campaigns ca on mwp.campaign_id = ca.campaign_id
),

-- Find attributed opportunities
opportunity_attribution as (
  select
    mwc.*,
    -- Opportunity attributed from lead conversion
    o_conv.opportunity_id                                 as converted_opportunity_id_attr,
    o_conv.amount                                         as converted_opp_amount,
    o_conv.arr                                            as converted_opp_arr,
    o_conv.is_won                                         as converted_opp_is_won,
    o_conv.close_date                                     as converted_opp_close_date,
    o_conv.stage_name                                     as converted_opp_stage,
    -- Days from member join to opportunity creation
    datediff('day', mwc.member_created_at,
             o_conv.created_at)                           as days_member_to_opportunity,
    -- Days from member join to close
    datediff('day', mwc.member_created_at,
             o_conv.close_date)                           as days_member_to_close
  from members_with_campaign mwc
  left join opportunities o_conv
    on mwc.converted_opportunity_id = o_conv.opportunity_id
),

final as (

  select
    -- ── Surrogate key ─────────────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['campaign_member_id']) }}
                                                          as demand_gen_id,

    -- ── Natural keys ──────────────────────────────────────────────────────
    campaign_member_id,
    campaign_id,
    lead_id,
    contact_id,
    person_id,

    -- ── Member attributes ─────────────────────────────────────────────────
    member_type,                                          -- 'Lead' or 'Contact'
    member_status,
    has_responded,
    first_responded_date,
    member_created_at,

    -- ── Unified person fields (coalesce lead/contact) ─────────────────────
    coalesce(lead_email,   contact_email)                 as email,
    coalesce(lead_name,    contact_name)                  as full_name,
    coalesce(lead_company, null)                          as company,          -- contacts derive from account
    coalesce(lead_title,   contact_title)                 as title,
    lead_industry                                         as industry,
    lead_annual_revenue                                   as annual_revenue,
    lead_employees                                        as number_of_employees,
    coalesce(lead_source,  contact_lead_source)           as lead_source,
    coalesce(lead_country, null)                          as country,
    coalesce(lead_state,   null)                          as state,
    coalesce(lead_owner_id,contact_owner_id)              as owner_id,

    -- Lead qualification
    lead_status,
    lead_rating,
    lead_score,

    -- ── Campaign context ──────────────────────────────────────────────────
    campaign_name,
    campaign_type,
    channel,
    utm_source,
    utm_medium,
    campaign_start_date,
    campaign_end_date,
    campaign_budgeted_cost,
    campaign_actual_cost,

    -- ── Funnel stage flags ────────────────────────────────────────────────
    has_responded                                         as is_mql,             -- Responded = Marketing Qualified
    (converted_opportunity_id_attr is not null)::boolean  as is_sql,             -- Has attributed opp = Sales Qualified
    coalesce(converted_opp_is_won, false)                 as is_customer,        -- Won opp = Converted to Customer

    -- ── Conversion chain ─────────────────────────────────────────────────
    is_converted                                          as lead_is_converted,
    converted_date,
    converted_account_id,
    converted_opportunity_id_attr,
    converted_opp_amount,
    converted_opp_arr,
    converted_opp_is_won,
    converted_opp_close_date,
    converted_opp_stage,

    -- ── Velocity metrics ──────────────────────────────────────────────────
    -- Days from campaign join to response
    datediff('day', member_created_at,
             first_responded_date)                        as days_to_respond,
    days_member_to_opportunity,
    days_member_to_close,

    -- ── Metadata ──────────────────────────────────────────────────────────
    current_timestamp()                                   as dbt_updated_at

  from opportunity_attribution

)

select * from final
