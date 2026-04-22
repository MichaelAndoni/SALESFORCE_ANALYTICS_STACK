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
    where systemmodstamp >= (
      select dateadd('day', -{{ var('incremental_lookback_days') }},
                     max(updated_at))
      from {{ this }}
    )
  {% endif %}

),

cleaned as (

  select
    id                                                    as campaign_member_id,
    campaignid                                         as campaign_id,

    -- Exactly one of lead_id / contact_id will be populated per row
    leadid                                             as lead_id,
    contactid                                          as contact_id,

    -- Derived: is this member a lead or contact?
    case
      when leadid    is not null then 'Lead'
      when contactid is not null then 'Contact'
    end                                                   as member_type,

    -- Single nullable person ID — convenient for joining
    coalesce(leadid, contactid)                         as person_id,

    -- ── Engagement ────────────────────────────────────────────────────────
    status                                                as member_status,
    hasresponded::boolean                                as has_responded,
    firstrespondeddate::date                            as first_responded_date,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
