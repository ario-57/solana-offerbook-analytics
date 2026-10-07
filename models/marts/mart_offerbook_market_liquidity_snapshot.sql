{{ config(
    materialized='view'
) }}

with offers as (

    select *
    from {{ ref('int_offerbook_offer_lifecycle') }}

    where lifecycle_status in (
        'Active',
        'PartiallyFilled'
    )
),

market as (

    select
        current_timestamp
            as snapshot_at,

        principal_asset_key,
        collateral_asset_key,
        creator_role,

        any_value(principal_symbol)
            as principal_symbol,

        any_value(collateral_symbol)
            as collateral_symbol,

        count(*)
            as open_offers,

        count(distinct creator_address)
            as active_offer_creators,

        count(
            case when is_stale_7d then 1 end
        ) as stale_7d_offers,

        count(
            case when is_stale_30d then 1 end
        ) as stale_30d_offers,

        sum(remaining_principal_usd)
            as open_principal_usd,

        sum(
            case
                when is_stale_7d
                then remaining_principal_usd
            end
        ) as stale_7d_principal_usd,

        sum(
            case
                when is_stale_30d
                then remaining_principal_usd
            end
        ) as stale_30d_principal_usd,

        median(
            offer_age_seconds
        ) as median_open_offer_age_seconds,

        quantile_cont(
            offer_age_seconds,
            0.9
        ) as p90_open_offer_age_seconds,

        avg(apy_raw)
            as avg_open_apy_raw,

        median(apy_raw)
            as median_open_apy_raw,

        quantile_cont(apy_raw, 0.25)
            as p25_open_apy_raw,

        quantile_cont(apy_raw, 0.75)
            as p75_open_apy_raw,

        avg(duration_raw)
            as avg_open_duration_raw,

        median(duration_raw)
            as median_open_duration_raw

    from offers

    where principal_asset_key is not null
      and collateral_asset_key is not null

    group by
        2,
        3,
        4
)

select
    *,

    concat(
        coalesce(principal_symbol, principal_asset_key),
        ' / ',
        coalesce(collateral_symbol, collateral_asset_key)
    ) as market_name,

    100.0
    * stale_7d_offers
    / nullif(open_offers, 0)
        as stale_7d_offer_pct,

    100.0
    * stale_30d_offers
    / nullif(open_offers, 0)
        as stale_30d_offer_pct,

    100.0
    * stale_7d_principal_usd
    / nullif(open_principal_usd, 0)
        as stale_7d_liquidity_pct,

    100.0
    * stale_30d_principal_usd
    / nullif(open_principal_usd, 0)
        as stale_30d_liquidity_pct

from market
