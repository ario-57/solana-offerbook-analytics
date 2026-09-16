{{ config(
    materialized='view'
) }}

with mints as (

    select *
    from {{ ref('stg_token_mints') }}

),

metadata as (

    select *
    from {{ ref('stg_token_metadata') }}

),

final as (

    select
        m.mint_address,

        md.symbol,
        md.name,
        md.logo_uri,

        m.decimals,
        m.token_program,
        m.supply_raw,
        m.is_initialized,

        md.fetched_at
            as metadata_fetched_at

    from mints as m

    left join metadata as md
        on m.mint_address =
           md.mint_address

)

select *
from final