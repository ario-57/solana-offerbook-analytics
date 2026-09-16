{{ config(
    materialized='view'
) }}

select
    mint_address,
    token_program,
    decimals,
    supply_raw,
    is_initialized,
    fetched_at

from {{ source('solana_raw', 'token_mints') }}