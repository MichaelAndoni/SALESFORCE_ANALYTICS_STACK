-- models/marts/core/dim_campaign.sql

{{
  config(materialized = 'table')
}}

with campaigns as (
  select * from {{ ref('stg_salesforce__campaigns') }}
),

owners as (
  select user_id, full_name as owner_name
  from {{ ref('stg_salesforce__users') }}
),

parent_campaigns as (
  select
    campaign_id as parent_campaign_id,
    campaign_name as parent_campaign_name,
    campaign_type as parent_campaign_type
  from {{ ref('stg_salesforce__campaigns') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['c.campaign_id']) }}  as campaign_sk,

    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.campaign_status,

    -- Hierarchy
    c.parent_campaign_id,
    p.parent_campaign_name,
    p.parent_campaign_type,

    -- Ownership
    c.owner_id,
    o.owner_name,

    -- Schedule
    c.start_date,
    c.end_date,
    datediff('day', c.start_date, c.end_date)             as campaign_duration_days,

    -- Budget
    c.budgeted_cost,
    c.actual_cost,
    c.budgeted_cost - c.actual_cost                       as budget_variance,

    -- Targets
    c.expected_revenue,
    c.expected_response_pct,
    c.number_sent,

    -- UTM / channel
    null                                                  as utm_source,
    null                                                  as utm_medium,
    null                                                  as utm_content,
    null                                                  as channel,

    -- Flags
    c.is_active,

    -- Timestamps
    c.created_at,
    c.updated_at,

    current_timestamp() as dbt_updated_at

  from campaigns c
  left join owners          o on c.owner_id          = o.user_id
  left join parent_campaigns p on c.parent_campaign_id = p.parent_campaign_id
)

select * from final
