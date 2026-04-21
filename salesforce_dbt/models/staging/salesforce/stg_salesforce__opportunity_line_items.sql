-- models/staging/salesforce/stg_salesforce__opportunity_line_items.sql

{{
  config(
    unique_key = 'opportunity_line_item_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'opportunity_line_item') }}

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
    id                                                    as opportunity_line_item_id,
    opportunity_id,
    product_2_id                                          as product_id,
    pricebook_entry_id,

    -- ── Product details ───────────────────────────────────────────────────
    name                                                  as product_name,
    product_code,
    description,

    -- ── Pricing ───────────────────────────────────────────────────────────
    quantity::number(18, 4)                               as quantity,
    unit_price::number(38, 2)                             as unit_price,
    list_price::number(38, 2)                             as list_price,
    total_price::number(38, 2)                            as total_price,
    -- Discount as a percentage (0–100)
    discount::number(5, 2)                                as discount_pct,

    -- ── Revenue type (common custom fields) ───────────────────────────────
    revenue_type__c                                       as revenue_type,       -- e.g. ARR, Services, One-time
    service_date::date                                    as service_date,

    -- ── Ordering ──────────────────────────────────────────────────────────
    sort_order::integer                                   as sort_order,

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
