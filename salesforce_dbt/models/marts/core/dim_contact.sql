-- models/marts/core/dim_contact.sql

{{
  config(materialized = 'table')
}}

with contacts as (
  select * from {{ ref('stg_salesforce__contacts') }}
),

accounts as (
  select account_id, account_name, industry, billing_country
  from {{ ref('stg_salesforce__accounts') }}
),

owners as (
  select user_id, full_name as owner_name, department as owner_department
  from {{ ref('stg_salesforce__users') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['c.contact_id']) }}  as contact_sk,
    c.contact_id,
    c.account_id,
    a.account_name,
    a.industry,
    a.billing_country,

    c.first_name,
    c.last_name,
    c.full_name,
    c.salutation,
    c.title,
    c.department,
    c.email,
    c.phone,
    c.mobile_phone,
    c.linkedin_url,
    c.lead_source,

    -- Address
    c.mailing_city,
    c.mailing_state,
    c.mailing_country,

    -- Opt-out flags
    c.has_opted_out_of_email,
    c.do_not_call,

    -- Ownership
    c.owner_id,
    o.owner_name,
    o.owner_department,

    -- Timestamps
    c.created_at,
    c.last_modified_at,
    c.updated_at,
    c.last_activity_date,
    c.birth_date,

    current_timestamp() as dbt_updated_at

  from contacts c
  left join accounts a on c.account_id = a.account_id
  left join owners   o on c.owner_id   = o.user_id
)

select * from final
