-- models/staging/salesforce/stg_salesforce__opportunity_line_items.sql

{{
  config(
    unique_key = 'opportunity_line_item_id'
  )
}}

with source as (

  select * from {{ source('salesforce', 'opportunity_line_item') }}

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
    id                                                    as opportunity_line_item_id,
    opportunityid                                      as opportunity_id,
    product2id                                          as product_id,
    pricebookentryid                                   as pricebook_entry_id,

    -- ── Product details ───────────────────────────────────────────────────
    name                                                  as product_name,
    productcode                                        as product_code,
    description,

    -- ── Pricing ───────────────────────────────────────────────────────────
    quantity::number(18, 4)                               as quantity,
    unitprice::number(38, 2)                             as unit_price,
    listprice::number(38, 2)                             as list_price,
    totalprice::number(38, 2)                            as total_price,

    -- ── Schedule ──────────────────────────────────────────────────────────
    servicedate::date                                    as service_date,
    sortorder::integer                                   as sort_order,

    -- ── Flags ─────────────────────────────────────────────────────────────
    isdeleted::boolean                                   as is_deleted,

    -- ── Timestamps ────────────────────────────────────────────────────────
    createddate::timestamp_ntz                           as created_at,
    systemmodstamp::timestamp_ntz                        as updated_at

  from source

  where coalesce(isdeleted, false) = false

)

select * from cleaned
