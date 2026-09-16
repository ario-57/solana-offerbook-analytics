{{ config(
    materialized='view'
) }}

with repayments as (

    select *
    from {{ ref('int_offerbook_loan_repayments') }}

),

tokens as (

    select *
    from {{ ref('int_tokens') }}

),

prices as (

    select *
    from {{ ref('stg_token_prices_hourly') }}

),

final as (

    select

        --------------------------------------------------
        -- Identity
        --------------------------------------------------

        r.loan_address,
        r.offer_address,
        r.fill_index,

        r.signature,
        r.parent_instruction_index,
        r.inner_instruction_index,

        r.event_name,
        r.event_version,

        --------------------------------------------------
        -- Time
        --------------------------------------------------

        r.loan_created_at,
        r.loan_expired_at,
        r.loan_updated_at,

        r.repaid_at,

        --------------------------------------------------
        -- Participants
        --------------------------------------------------

        r.lender_address,
        r.borrower_address,
        r.creator_address,

        --------------------------------------------------
        -- State
        --------------------------------------------------

        r.loan_status,
        r.loan_type,

        r.is_extendable,
        r.extension_count,

        --------------------------------------------------
        -- Principal asset
        --------------------------------------------------

        r.principal_asset_type,
        r.principal_mint,

        {{ offerbook_asset_key(
            'r.principal_asset',
            'r.principal_asset_type'
        ) }} as principal_asset_key,

        p.symbol
            as principal_symbol,

        p.name
            as principal_name,

        p.decimals
            as principal_decimals,

        --------------------------------------------------
        -- Collateral asset
        --------------------------------------------------

        r.collateral_asset_type,
        r.collateral_mint,

        {{ offerbook_asset_key(
            'r.collateral_asset',
            'r.collateral_asset_type'
        ) }} as collateral_asset_key,

        c.symbol
            as collateral_symbol,

        c.name
            as collateral_name,

        c.decimals
            as collateral_decimals,

        --------------------------------------------------
        -- Raw amounts
        --------------------------------------------------

        r.principal_amount_raw,
        r.collateral_amount_raw,
        r.interest_raw,

        --------------------------------------------------
        -- Normalized principal
        --------------------------------------------------

        case
            when
                r.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                r.principal_amount_raw
                / pow(10, p.decimals)
        end as principal_amount,

        --------------------------------------------------
        -- Normalized collateral
        --------------------------------------------------

        case
            when
                r.collateral_asset_type = 'Token'
                and c.decimals is not null

            then
                r.collateral_amount_raw
                / pow(10, c.decimals)
        end as collateral_amount,

        --------------------------------------------------
        -- Realized interest amount
        --
        -- Interest is denominated in principal asset.
        --------------------------------------------------

        case
            when
                r.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                r.interest_raw
                / pow(10, p.decimals)
        end as realized_interest_amount,

        --------------------------------------------------
        -- Price timestamps
        --------------------------------------------------

        pp.price_hour
            as principal_price_hour,

        cp.price_hour
            as collateral_price_hour,

        --------------------------------------------------
        -- Price age
        --------------------------------------------------

        case
            when pp.price_hour is not null
            then date_diff(
                'minute',
                pp.price_hour,
                r.repaid_at
            )
        end as principal_price_age_minutes,

        case
            when cp.price_hour is not null
            then date_diff(
                'minute',
                cp.price_hour,
                r.repaid_at
            )
        end as collateral_price_age_minutes,

        --------------------------------------------------
        -- Repayment-time USD prices
        --------------------------------------------------

        pp.price_usd
            as principal_price_usd_at_repayment,

        cp.price_usd
            as collateral_price_usd_at_repayment,

        --------------------------------------------------
        -- Repaid principal USD
        --------------------------------------------------

        case
            when
                r.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    r.principal_amount_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd

        end as repaid_principal_usd,

        --------------------------------------------------
        -- REALIZED interest USD
        --------------------------------------------------

        case
            when
                r.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    r.interest_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd

        end as realized_interest_usd,

        --------------------------------------------------
        -- Total repayment value
        --
        -- Principal + realized interest
        --------------------------------------------------

        case
            when
                r.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    (
                        r.principal_amount_raw
                        + r.interest_raw
                    )
                    / pow(10, p.decimals)
                )
                * pp.price_usd

        end as total_repayment_value_usd,

        --------------------------------------------------
        -- Collateral value at repayment
        --------------------------------------------------

        case
            when
                r.collateral_asset_type = 'Token'
                and c.decimals is not null
                and cp.price_usd is not null

            then
                (
                    r.collateral_amount_raw
                    / pow(10, c.decimals)
                )
                * cp.price_usd

        end as collateral_value_at_repayment_usd,

        --------------------------------------------------
        -- Price quality
        --------------------------------------------------

        case
            when r.principal_asset_type != 'Token'
                then 'not_applicable'

            when pp.price_usd is null
                then 'missing'

            when pp.price_hour = r.repaid_at
                then 'exact'

            else 'previous_within_1h'
        end as principal_price_status,

        case
            when r.collateral_asset_type != 'Token'
                then 'not_applicable'

            when cp.price_usd is null
                then 'missing'

            when cp.price_hour = r.repaid_at
                then 'exact'

            else 'previous_within_1h'
        end as collateral_price_status,

        --------------------------------------------------
        -- Terms
        --------------------------------------------------

        r.apy_raw,
        r.duration_raw,

        --------------------------------------------------
        -- Original asset JSON
        --------------------------------------------------

        r.principal_asset,
        r.collateral_asset,

        --------------------------------------------------
        -- Lineage
        --------------------------------------------------

        r.idl_version,
        r.idl_sha256

    from repayments as r

    ------------------------------------------------------
    -- Principal metadata
    ------------------------------------------------------

    left join tokens as p
        on r.principal_mint = p.mint_address

    ------------------------------------------------------
    -- Collateral metadata
    ------------------------------------------------------

    left join tokens as c
        on r.collateral_mint = c.mint_address

    ------------------------------------------------------
    -- Closest previous principal price
    -- Maximum age = 1 hour
    ------------------------------------------------------

    left join lateral (

        select
            pr.price_hour,
            pr.price_usd,
            pr.price_source

        from prices as pr

        where pr.mint_address =
              r.principal_mint

          and pr.price_hour <=
              r.repaid_at

          and pr.price_hour >=
              r.repaid_at
              - interval '1 hour'

        order by pr.price_hour desc

        limit 1

    ) as pp
        on true

    ------------------------------------------------------
    -- Closest previous collateral price
    ------------------------------------------------------

    left join lateral (

        select
            pr.price_hour,
            pr.price_usd,
            pr.price_source

        from prices as pr

        where pr.mint_address =
              r.collateral_mint

          and pr.price_hour <=
              r.repaid_at

          and pr.price_hour >=
              r.repaid_at
              - interval '1 hour'

        order by pr.price_hour desc

        limit 1

    ) as cp
        on true

)

select *
from final