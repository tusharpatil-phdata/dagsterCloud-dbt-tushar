{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB_CLOUD',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}

select
    loyalty_points_id as loyalty_points_key,
    customer_id,
    points_balance,
    points_earned,
    points_redeemed,
    as_of_date,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _updated_at
from {{ ref('stg_loyalty_points') }}