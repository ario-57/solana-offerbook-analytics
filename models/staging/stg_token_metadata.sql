{{ config(
    materialized='view'
) }}

select
    mint_address,
    symbol,
    name,
    logo_uri,
    fetched_at

from {{ source(
    'solana_raw',
    'token_metadata'
) }}