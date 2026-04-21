-- models/marts/marketing/fct_campaign_performance.sql
--
-- GRAIN: One row per Campaign.
--
-- Aggregates all campaign metrics into a single facts row per campaign:
-- reach, engagement, MQL/SQL pipeline generated, and revenue influenced.
-- Both the Salesforce roll-up fields AND derived counts from CampaignMember
-- and Opportunity are included so discrepancies can be surfaced.
--
-- Key metrics:
--   • Cost per lead / cost per opportunity / ROI
--   • Member response rate
--   • Pipeline influenced (any opp touched by campaign)
--   • Revenue influenced (won opps touched by campaign)

{{
  config(
    unique_key = 'campaign_performance_id'
  )
}}

with campaigns as (
  select * from {{ ref('stg_salesforce__campaigns') }}
),

campaign_members as (
  select * from {{ ref('stg_salesforce__campaign_members') }}
),

opportunities as (
  select * from {{ ref('stg_salesforce__opportunities') }}
),

-- Member counts per campaign (independently computed vs Salesforce roll-ups)
member_agg as (
  select
    campaign_id,
    count(*)                                              as total_members,
    count(case when member_type = 'Lead'    then 1 end)  as lead_members,
    count(case when member_type = 'Contact' then 1 end)  as contact_members,
    count(case when has_responded = true    then 1 end)  as responded_members,
    min(created_at)                                       as first_member_added_at,
    max(created_at)                                       as last_member_added_at
  from campaign_members
  group by 1
),

-- Opportunity pipeline attributed to each campaign
opp_agg as (
  select
    campaign_id,
    count(*)                                              as total_opportunities,
    count(case when not is_closed                        then 1 end) as open_opportunities,
    count(case when is_won                               then 1 end) as won_opportunities,
    count(case when is_lost                              then 1 end) as lost_opportunities,
    sum(amount)                                           as total_pipeline_amount,
    sum(case when not is_closed then amount else 0 end)  as open_pipeline_amount,
    sum(case when is_won        then amount else 0 end)  as won_amount,
    sum(case when is_won        then arr    else 0 end)  as won_arr,
    min(case when is_won        then close_date end)      as first_won_date,
    max(case when is_won        then close_date end)      as last_won_date
  from opportunities
  where campaign_id is not null
  group by 1
),

final as (

  select
    -- ── Surrogate / natural keys ───────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['c.campaign_id']) }}
                                                          as campaign_performance_id,
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.campaign_status,
    c.channel,
    c.utm_source,
    c.utm_medium,

    -- ── Schedule ──────────────────────────────────────────────────────────
    c.start_date,
    c.end_date,
    datediff('day', c.start_date, c.end_date)             as campaign_duration_days,

    -- ── Investment ────────────────────────────────────────────────────────
    c.budgeted_cost,
    c.actual_cost,
    c.number_sent,

    -- ── Reach & Engagement (derived) ──────────────────────────────────────
    coalesce(ma.total_members,    0)                      as total_members,
    coalesce(ma.lead_members,     0)                      as lead_members,
    coalesce(ma.contact_members,  0)                      as contact_members,
    coalesce(ma.responded_members,0)                      as responded_members,

    -- Response rate
    case
      when coalesce(ma.total_members, 0) = 0 then null
      else round(ma.responded_members / ma.total_members * 100, 2)
    end                                                   as response_rate_pct,

    -- ── Salesforce native roll-ups (for reconciliation) ───────────────────
    c.number_of_leads                                     as sf_number_of_leads,
    c.number_of_converted_leads                           as sf_converted_leads,
    c.number_of_contacts                                  as sf_number_of_contacts,
    c.number_of_responses                                 as sf_number_of_responses,
    c.number_of_opportunities                             as sf_opportunities,
    c.number_of_won_opportunities                         as sf_won_opportunities,
    c.amount_all_opportunities                            as sf_pipeline_amount,
    c.amount_won_opportunities                            as sf_won_amount,

    -- ── Derived pipeline attribution ──────────────────────────────────────
    coalesce(oa.total_opportunities,  0)                  as total_opportunities,
    coalesce(oa.open_opportunities,   0)                  as open_opportunities,
    coalesce(oa.won_opportunities,    0)                  as won_opportunities,
    coalesce(oa.lost_opportunities,   0)                  as lost_opportunities,
    coalesce(oa.total_pipeline_amount,0)                  as total_pipeline_amount,
    coalesce(oa.open_pipeline_amount, 0)                  as open_pipeline_amount,
    coalesce(oa.won_amount,           0)                  as won_amount,
    coalesce(oa.won_arr,              0)                  as won_arr,
    oa.first_won_date,
    oa.last_won_date,

    -- ── Efficiency metrics ────────────────────────────────────────────────
    -- Cost per lead
    case
      when coalesce(ma.lead_members, 0) = 0 then null
      else round(c.actual_cost / ma.lead_members, 2)
    end                                                   as cost_per_lead,

    -- Cost per opportunity
    case
      when coalesce(oa.total_opportunities, 0) = 0 then null
      else round(c.actual_cost / oa.total_opportunities, 2)
    end                                                   as cost_per_opportunity,

    -- Pipeline ROI: pipeline generated per $ spent
    case
      when coalesce(c.actual_cost, 0) = 0 then null
      else round(oa.total_pipeline_amount / c.actual_cost, 2)
    end                                                   as pipeline_roi,

    -- Revenue ROI: revenue won per $ spent
    case
      when coalesce(c.actual_cost, 0) = 0 then null
      else round(oa.won_amount / c.actual_cost, 2)
    end                                                   as revenue_roi,

    -- ── Timestamps ────────────────────────────────────────────────────────
    c.created_at,
    c.updated_at,
    ma.first_member_added_at,
    ma.last_member_added_at,

    current_timestamp()                                   as dbt_updated_at

  from campaigns c
  left join member_agg ma on c.campaign_id = ma.campaign_id
  left join opp_agg    oa on c.campaign_id = oa.campaign_id

)

select * from final
