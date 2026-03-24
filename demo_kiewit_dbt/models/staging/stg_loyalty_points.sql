{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB_CLOUD',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}

select
    loyalty_points_id,
    customer_id,
    points_balance,
    points_earned,
    points_redeemed,
    coalesce(points_balance, 0) as points_balance_clean,
    as_of_date,
    _loaded_at,
    _source_file
from {{ ref('raw_loyalty_points') }}
where loyalty_points_id is not null
  and customer_id is not null