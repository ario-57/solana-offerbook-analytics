{{ config(materialized='table') }}

select
    block_date,
    token_pair,

    count(*) as trade_count,

    count(distinct trader) as unique_traders,

    sum(amount_usd) as volume_usd

from {{ ref('stg_trades') }}

group by
    block_date,
    token_pair

order by
    block_date,
    token_pair