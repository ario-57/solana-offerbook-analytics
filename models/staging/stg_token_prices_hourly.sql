{{  config(
    materialized='view'
)}}

select 
    mint_address,
    date_trunc('hour', price_hour) as price_hour,
    price_usd,
    price_source,
    fetched_at
from {{ source('solana_raw', 'token_prices_hourly') }}