select trade_id,
    cast(block_date as date) as block_date,
    trader,
    token_pair,
    cast(amount_usd as double) as amount_usd

from {{ ref('trades') }}