{{ config(
    materialized='view'
) }}

select

    mint_address,

    price_date,

    reference_event_at,

    provider_price_at,

    price_usd,

    price_source,

    fetched_at

from {{
    source(
        'solana_raw',
        'token_prices_daily'
    )
}}