-- models/marts/core/dim_lead.sql

{{
  config(materialized = 'table')
}}

with leads as (
  select * from {{ ref('int_salesforce__lead_enriched') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['lead_id']) }}   as lead_sk,

    lead_id,
    full_name,
    first_name,
    last_name,
    email,
    phone,
    title,
    company,
    industry,
    annual_revenue,
    number_of_employees,
    lead_source,
    status,
    rating,
    lead_score,
    lead_grade,
    city,
    state,
    country,

    -- Segmentation (mirrors dim_account for pre-conversion leads)
    case
      when number_of_employees < 50    then 'SMB'
      when number_of_employees < 500   then 'Mid-Market'
      when number_of_employees < 5000  then 'Enterprise'
      when number_of_employees >= 5000 then 'Strategic'
      else 'Unknown'
    end                                                   as employee_segment,

    -- Conversion
    is_converted,
    converted_date,
    converted_account_id,
    converted_contact_id,
    converted_opportunity_id,
    days_to_convert,

    -- Ownership
    owner_id,
    owner_name,
    owner_department,

    -- Campaign attribution
    original_campaign_id,
    original_campaign_name,
    original_campaign_type,
    original_campaign_channel,
    last_campaign_id,
    last_campaign_name,
    last_campaign_status,
    responded_to_last_campaign,

    -- Timestamps
    created_at,
    updated_at,
    last_activity_date,

    current_timestamp() as dbt_updated_at

  from leads
)

select * from final
