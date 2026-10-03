{{ config(
    materialized='table'
) }}

with offers as (

    select *
    from {{ ref('int_offerbook_offer_creations') }}

),

final as (

    select
        o.*,

        p.symbol as principal_symbol,
        p.name as principal_name,
        p.decimals as principal_decimals,

        c.symbol as collateral_symbol,
        c.name as collateral_name,
        c.decimals as collateral_decimals,

        o.principal_amount_raw
            / pow(
                10,
                p.decimals
            )
            as principal_amount,

        o.collateral_amount_raw
            / pow(
                10,
                c.decimals
            )
            as collateral_amount

    from offers as o

    left join {{ ref('int_tokens') }} as p
        on o.principal_mint =
           p.mint_address

    left join {{ ref('int_tokens') }} as c
        on o.collateral_mint =
           c.mint_address

)

select *
from final