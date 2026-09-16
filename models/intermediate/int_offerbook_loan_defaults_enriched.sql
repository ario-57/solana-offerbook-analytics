{{ config(
    materialized='view'
) }}

with defaults as (

    select *
    from {{ ref('int_offerbook_loan_defaults') }}

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

        d.loan_address,
        d.offer_address,
        d.fill_index,

        d.signature,
        d.parent_instruction_index,
        d.inner_instruction_index,

        d.event_name,
        d.event_version,

        --------------------------------------------------
        -- Time
        --------------------------------------------------

        d.loan_created_at,
        d.loan_expired_at,
        d.loan_updated_at,

        d.defaulted_at,

        --------------------------------------------------
        -- Participants
        --------------------------------------------------

        d.lender_address,
        d.borrower_address,
        d.creator_address,

        --------------------------------------------------
        -- State
        --------------------------------------------------

        d.loan_status,
        d.loan_type,

        d.is_extendable,
        d.extension_count,

        --------------------------------------------------
        -- Principal metadata
        --------------------------------------------------

        d.principal_asset_type,
        d.principal_mint,

        {{ offerbook_asset_key(
            'd.principal_asset',
            'd.principal_asset_type'
        ) }} as principal_asset_key,

        p.symbol
            as principal_symbol,

        p.name
            as principal_name,

        p.decimals
            as principal_decimals,

        --------------------------------------------------
        -- Collateral metadata
        --------------------------------------------------

        d.collateral_asset_type,
        d.collateral_mint,

        {{ offerbook_asset_key(
            'd.collateral_asset',
            'd.collateral_asset_type'
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

        d.principal_amount_raw,
        d.collateral_amount_raw,
        d.interest_raw,

        --------------------------------------------------
        -- Normalized amounts
        --------------------------------------------------

        case
            when
                d.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                d.principal_amount_raw
                / pow(10, p.decimals)

        end as principal_amount,

        case
            when
                d.collateral_asset_type = 'Token'
                and c.decimals is not null

            then
                d.collateral_amount_raw
                / pow(10, c.decimals)

        end as collateral_amount,

        case
            when
                d.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                d.interest_raw
                / pow(10, p.decimals)

        end as interest_amount,

        --------------------------------------------------
        -- Point-in-time price timestamps
        --------------------------------------------------

        pp.price_hour
            as principal_price_hour,

        cp.price_hour
            as collateral_price_hour,

        --------------------------------------------------
        -- Price ages
        --------------------------------------------------

        case
            when pp.price_hour is not null

            then date_diff(
                'minute',
                pp.price_hour,
                d.defaulted_at
            )

        end as principal_price_age_minutes,

        case
            when cp.price_hour is not null

            then date_diff(
                'minute',
                cp.price_hour,
                d.defaulted_at
            )

        end as collateral_price_age_minutes,

        --------------------------------------------------
        -- Prices at default
        --------------------------------------------------

        pp.price_usd
            as principal_price_usd_at_default,

        cp.price_usd
            as collateral_price_usd_at_default,

        --------------------------------------------------
        -- Defaulted principal USD
        --------------------------------------------------

        case
            when
                d.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    d.principal_amount_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd

        end as defaulted_principal_usd,

        --------------------------------------------------
        -- Interest amount at default value
        --
        -- Be careful with terminology:
        -- this is NOT realized lender interest.
        --------------------------------------------------

        case
            when
                d.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    d.interest_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd

        end as contractual_interest_at_default_usd,

        --------------------------------------------------
        -- Collateral value at default
        --------------------------------------------------

        case
            when
                d.collateral_asset_type = 'Token'
                and c.decimals is not null
                and cp.price_usd is not null

            then
                (
                    d.collateral_amount_raw
                    / pow(10, c.decimals)
                )
                * cp.price_usd

        end as collateral_value_at_default_usd,

        --------------------------------------------------
        -- Collateral / principal ratio at default
        --------------------------------------------------

        case
            when
                (
                    d.principal_asset_type = 'Token'
                    and p.decimals is not null
                    and pp.price_usd is not null
                )

                and
                (
                    d.collateral_asset_type = 'Token'
                    and c.decimals is not null
                    and cp.price_usd is not null
                )

                and
                (
                    (
                        d.principal_amount_raw
                        / pow(10, p.decimals)
                    )
                    * pp.price_usd
                ) != 0

            then

                (
                    (
                        d.collateral_amount_raw
                        / pow(10, c.decimals)
                    )
                    * cp.price_usd
                )

                /

                (
                    (
                        d.principal_amount_raw
                        / pow(10, p.decimals)
                    )
                    * pp.price_usd
                )

        end as collateral_to_principal_ratio_at_default,

        --------------------------------------------------
        -- Price quality
        --------------------------------------------------

        case
            when d.principal_asset_type != 'Token'
                then 'not_applicable'

            when pp.price_usd is null
                then 'missing'

            when pp.price_hour = d.defaulted_at
                then 'exact'

            else 'previous_within_1h'

        end as principal_price_status,

        case
            when d.collateral_asset_type != 'Token'
                then 'not_applicable'

            when cp.price_usd is null
                then 'missing'

            when cp.price_hour = d.defaulted_at
                then 'exact'

            else 'previous_within_1h'

        end as collateral_price_status,

        --------------------------------------------------
        -- Terms
        --------------------------------------------------

        d.apy_raw,
        d.duration_raw,

        --------------------------------------------------
        -- Original asset JSON
        --------------------------------------------------

        d.principal_asset,
        d.collateral_asset,

        --------------------------------------------------
        -- Lineage
        --------------------------------------------------

        d.idl_version,
        d.idl_sha256

    from defaults as d

    ------------------------------------------------------
    -- Principal token metadata
    ------------------------------------------------------

    left join tokens as p
        on d.principal_mint = p.mint_address

    ------------------------------------------------------
    -- Collateral token metadata
    ------------------------------------------------------

    left join tokens as c
        on d.collateral_mint = c.mint_address

    ------------------------------------------------------
    -- Principal price:
    -- closest previous price within one hour
    ------------------------------------------------------

    left join lateral (

        select
            pr.price_hour,
            pr.price_usd,
            pr.price_source

        from prices as pr

        where pr.mint_address =
              d.principal_mint

          and pr.price_hour <=
              d.defaulted_at

          and pr.price_hour >=
              d.defaulted_at
              - interval '1 hour'

        order by
            pr.price_hour desc

        limit 1

    ) as pp
        on true

    ------------------------------------------------------
    -- Collateral price:
    -- closest previous price within one hour
    ------------------------------------------------------

    left join lateral (

        select
            pr.price_hour,
            pr.price_usd,
            pr.price_source

        from prices as pr

        where pr.mint_address =
              d.collateral_mint

          and pr.price_hour <=
              d.defaulted_at

          and pr.price_hour >=
              d.defaulted_at
              - interval '1 hour'

        order by
            pr.price_hour desc

        limit 1

    ) as cp
        on true

)

select *
from final