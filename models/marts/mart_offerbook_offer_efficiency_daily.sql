{{ config(
    materialized='view'
) }}

with offers as (

    select *
    from {{ ref('int_offerbook_offer_lifecycle') }}

),

daily as (

    select
        cast(
            offer_created_at at time zone 'UTC'
            as date
        ) as block_date,

        principal_asset_key,
        collateral_asset_key,
        creator_role,

        any_value(principal_symbol)
            as principal_symbol,

        any_value(collateral_symbol)
            as collateral_symbol,

        count(*)
            as offers_created,

        count(
            case when has_fill then 1 end
        ) as offers_with_fill,

        count(
            case when lifecycle_status = 'Fulfilled' then 1 end
        ) as fulfilled_offers,

        count(
            case when lifecycle_status = 'PartiallyFilled' then 1 end
        ) as partially_filled_offers,

        count(
            case when lifecycle_status = 'Cancelled' then 1 end
        ) as cancelled_offers,

        count(
            case when lifecycle_status = 'Expired' then 1 end
        ) as expired_offers,

        count(
            case when lifecycle_status = 'Active' then 1 end
        ) as active_offers,

        count(
            case when is_stale_7d then 1 end
        ) as stale_7d_offers,

        count(
            case when is_stale_30d then 1 end
        ) as stale_30d_offers,

        count(distinct creator_address)
            as unique_offer_creators,

        sum(loan_count)
            as loans_created_from_offers,

        sum(offered_principal_usd)
            as offered_principal_usd,

        sum(filled_principal_usd)
            as filled_principal_usd,

        sum(remaining_principal_usd)
            as remaining_principal_usd,

        avg(principal_fill_ratio)
            as avg_principal_fill_ratio,

        median(principal_fill_ratio)
            as median_principal_fill_ratio,

        avg(
            case
                when has_fill
                then seconds_to_first_fill
            end
        ) as avg_seconds_to_first_fill,

        median(
            case
                when has_fill
                then seconds_to_first_fill
            end
        ) as median_seconds_to_first_fill,

        quantile_cont(
            case
                when has_fill
                then seconds_to_first_fill
            end,
            0.9
        ) as p90_seconds_to_first_fill,

        avg(apy_raw)
            as avg_apy_raw,

        median(apy_raw)
            as median_apy_raw,

        quantile_cont(apy_raw, 0.25)
            as p25_apy_raw,

        quantile_cont(apy_raw, 0.75)
            as p75_apy_raw,

        avg(duration_raw)
            as avg_duration_raw,

        median(duration_raw)
            as median_duration_raw

    from offers

    where principal_asset_key is not null
      and collateral_asset_key is not null

    group by
        1,
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
    * offers_with_fill
    / nullif(offers_created, 0)
        as offer_fill_rate_pct,

    100.0
    * fulfilled_offers
    / nullif(offers_created, 0)
        as full_fill_rate_pct,

    100.0
    * cancelled_offers
    / nullif(offers_created, 0)
        as cancellation_rate_pct,

    100.0
    * expired_offers
    / nullif(offers_created, 0)
        as expiry_rate_pct,

    100.0
    * filled_principal_usd
    / nullif(offered_principal_usd, 0)
        as capital_utilization_pct

from daily
