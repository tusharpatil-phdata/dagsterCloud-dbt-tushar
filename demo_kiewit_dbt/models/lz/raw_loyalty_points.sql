{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB_CLOUD',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}

select
    ID              as loyalty_points_id,
    CUSTOMER_ID     as customer_id,
    try_to_number(POINTS_BALANCE)  as points_balance,
    try_to_number(POINTS_EARNED)   as points_earned,
    try_to_number(POINTS_REDEEMED) as points_redeemed,
    try_to_timestamp_ntz(AS_OF_DATE) as as_of_date,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'loyalty_points.csv' as _source_file
from {{ source('raw_loyalty_points', 'LOYALTY_POINTS') }}