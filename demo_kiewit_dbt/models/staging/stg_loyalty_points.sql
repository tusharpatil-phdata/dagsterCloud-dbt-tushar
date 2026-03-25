{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB_CLOUD',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

with source as (
    select * from {{ ref('raw_loyalty_points') }}
),

cleaned as (
    select
        loyalty_points_id,
        customer_id,
        coalesce(points_balance, 0)  as points_balance,
        coalesce(points_earned, 0)   as points_earned,
        coalesce(points_redeemed, 0) as points_redeemed,
        as_of_date
    from source
    where loyalty_points_id is not null
      and customer_id is not null
),

deduped as (
    select *,
        row_number() over (
            partition by loyalty_points_id
            order by loyalty_points_id
        ) as _rn
    from cleaned
)

select
    loyalty_points_id,
    customer_id,
    points_balance,
    points_earned,
    points_redeemed,
    as_of_date
from deduped
where _rn = 1