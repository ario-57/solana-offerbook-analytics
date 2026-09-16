{{ config(
    materialized='view'
) }}

with offers as (

    select
        *,
        date_trunc(
            'hour',
            block_timestamp
        ) as price_hour

    from {{
        ref(
            'int_offerbook_offer_creations_enriched'
        )
    }}

),

prices as (

    select *
    from {{
        ref(
            'stg_token_prices_hourly'
        )
    }}

),

final as (

    select
        o.*,

        principal_price.price_usd
            as principal_price_usd,

        collateral_price.price_usd
            as collateral_price_usd,

        o.principal_amount
            * principal_price.price_usd
            as principal_notional_usd,

        o.collateral_amount
            * collateral_price.price_usd
            as collateral_notional_usd

    from offers as o

    left join prices
        as principal_price

        on o.principal_mint =
           principal_price.mint_address

        and o.price_hour =
            principal_price.price_hour

    left join prices
        as collateral_price

        on o.collateral_mint =
           collateral_price.mint_address

        and o.price_hour =
            collateral_price.price_hour

)

select *
from final