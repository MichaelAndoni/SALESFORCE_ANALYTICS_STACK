-- models/marts/core/dim_user.sql

{{
  config(materialized = 'table')
}}

with users as (
  select * from {{ ref('stg_salesforce__users') }}
),

-- Self-join to get manager name
managers as (
  select user_id, full_name as manager_name
  from {{ ref('stg_salesforce__users') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['u.user_id']) }}  as user_sk,

    u.user_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.email,
    u.username,
    u.alias,
    u.title,
    u.department,
    u.division,
    u.phone,
    u.mobile_phone,

    -- Hierarchy
    u.manager_id,
    m.manager_name,
    u.user_role_id,
    u.profile_id,

    -- Locale
    u.timezone,
    u.locale,

    -- Status
    u.is_active,

    -- Timestamps
    u.created_at,
    u.last_login_at,
    u.updated_at,

    current_timestamp() as dbt_updated_at

  from users u
  left join managers m on u.manager_id = m.user_id
)

select * from final
