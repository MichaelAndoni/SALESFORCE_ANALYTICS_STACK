-- models/staging/salesforce/stg_salesforce__campaign_members.sql
--
-- CampaignMember is the junction object between Campaign and Lead/Contact.
-- A member can be a Lead OR a Contact — never both simultaneously.

{{
  config(
    unique_key = 'campaign_member_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'campaign_member') }}

  {% if is_incremental() %}
    where system_modstamp >= (
      select dateadd('day', -{{ var('incremental_lookback_days') }},
                     max(updated_at))
      from {{ this }}
    )
  {% endif %}

),

cleaned as (

  select
    id                                                    as campaign_member_id,
    campaign_id,

    -- Exactly one of lead_id / contact_id will be populated per row
    lead_id,
    contact_id,

    -- Derived: is this member a lead or contact?
    case
      when lead_id    is not null then 'Lead'
      when contact_id is not null then 'Contact'
    end                                                   as member_type,

    -- Single nullable person ID — convenient for joining
    coalesce(lead_id, contact_id)                         as person_id,

    -- ── Engagement ────────────────────────────────────────────────────────
    status                                                as member_status,      -- Sent, Opened, Registered, Attended, etc.
    has_responded::boolean                                as has_responded,
    first_responded_date::date                            as first_responded_date,

    -- ── Flags ─────────────────────────────────────────────────────────────
    is_deleted::boolean                                   as is_deleted,
    _fivetran_deleted::boolean                            as is_fivetran_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    created_date::timestamp_ntz                           as created_at,
    system_modstamp::timestamp_ntz                        as updated_at,
    _fivetran_synced::timestamp_ntz                       as fivetran_synced_at

  from source

  where coalesce(is_deleted, false) = false
    and coalesce(_fivetran_deleted, false) = false

)

select * from cleaned
