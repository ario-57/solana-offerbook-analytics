{{ config(
    materialized='view'
) }}

with offers as (

    select
        *,

        cast(
            block_timestamp at time zone 'UTC'
            as date
        ) as price_date

    from {{ ref('int_offerbook_offer_creations_enriched') }}

),

prices as (

    select *
    from {{ ref('stg_token_prices_daily') }}

),

final as (

    select
        o.*,

        principal_price.reference_event_at
            as principal_price_reference_event_at,

        collateral_price.reference_event_at
            as collateral_price_reference_event_at,

        principal_price.provider_price_at
            as principal_provider_price_at,

        collateral_price.provider_price_at
            as collateral_provider_price_at,

        principal_price.price_source
            as principal_price_source,

        collateral_price.price_source
            as collateral_price_source,

        principal_price.price_usd
            as principal_price_usd,

        collateral_price.price_usd
            as collateral_price_usd,

        o.principal_amount
            * principal_price.price_usd
            as principal_notional_usd,

        o.collateral_amount
            * collateral_price.price_usd
            as collateral_notional_usd,

        case
            when principal_price.price_usd is null
                then 'missing'
            when principal_price.price_source = 'fixed_usd_stablecoin'
                then 'fixed_stablecoin'
            else 'daily_reference'
        end as principal_price_status,

        case
            when collateral_price.price_usd is null
                then 'missing'
            when collateral_price.price_source = 'fixed_usd_stablecoin'
                then 'fixed_stablecoin'
            else 'daily_reference'
        end as collateral_price_status

    from offers as o

    left join prices as principal_price
        on o.principal_mint = principal_price.mint_address
        and o.price_date = principal_price.price_date

    left join prices as collateral_price
        on o.collateral_mint = collateral_price.mint_address
        and o.price_date = collateral_price.price_date

)

select *
from final
