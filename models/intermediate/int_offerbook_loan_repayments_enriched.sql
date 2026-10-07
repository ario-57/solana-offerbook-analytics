{{ config(
    materialized='view'
) }}

with repayments as (

    select
        *,

        cast(
            repaid_at at time zone 'UTC'
            as date
        ) as price_date

    from {{ ref('int_offerbook_loan_repayments') }}

),

tokens as (

    select *
    from {{ ref('int_tokens') }}

),

prices as (

    select *
    from {{ ref('stg_token_prices_daily') }}

),

final as (

    select

        r.loan_address,
        r.offer_address,
        r.fill_index,

        r.signature,
        r.parent_instruction_index,
        r.inner_instruction_index,

        r.event_name,
        r.event_version,

        r.loan_created_at,
        r.loan_expired_at,
        r.loan_updated_at,
        r.repaid_at,
        r.price_date,

        r.lender_address,
        r.borrower_address,
        r.creator_address,

        r.loan_status,
        r.loan_type,

        r.is_extendable,
        r.extension_count,

        r.principal_asset_type,
        r.principal_mint,

        {{ offerbook_asset_key(
            'r.principal_asset',
            'r.principal_asset_type'
        ) }} as principal_asset_key,

        p.symbol as principal_symbol,
        p.name as principal_name,
        p.decimals as principal_decimals,

        r.collateral_asset_type,
        r.collateral_mint,

        {{ offerbook_asset_key(
            'r.collateral_asset',
            'r.collateral_asset_type'
        ) }} as collateral_asset_key,

        c.symbol as collateral_symbol,
        c.name as collateral_name,
        c.decimals as collateral_decimals,

        r.principal_amount_raw,
        r.collateral_amount_raw,
        r.interest_raw,

        case
            when r.principal_asset_type = 'Token'
             and p.decimals is not null
            then r.principal_amount_raw / pow(10, p.decimals)
        end as principal_amount,

        case
            when r.collateral_asset_type = 'Token'
             and c.decimals is not null
            then r.collateral_amount_raw / pow(10, c.decimals)
        end as collateral_amount,

        case
            when r.principal_asset_type = 'Token'
             and p.decimals is not null
            then r.interest_raw / pow(10, p.decimals)
        end as realized_interest_amount,

        pp.price_date as principal_price_date,
        cp.price_date as collateral_price_date,

        pp.reference_event_at as principal_price_reference_event_at,
        cp.reference_event_at as collateral_price_reference_event_at,

        pp.provider_price_at as principal_provider_price_at,
        cp.provider_price_at as collateral_provider_price_at,

        pp.price_source as principal_price_source,
        cp.price_source as collateral_price_source,

        case
            when pp.reference_event_at is not null
            then date_diff(
                'minute',
                pp.reference_event_at,
                r.repaid_at
            )
        end as principal_price_age_minutes,

        case
            when cp.reference_event_at is not null
            then date_diff(
                'minute',
                cp.reference_event_at,
                r.repaid_at
            )
        end as collateral_price_age_minutes,

        pp.price_usd as principal_price_usd_at_repayment,
        cp.price_usd as collateral_price_usd_at_repayment,

        case
            when r.principal_asset_type = 'Token'
             and p.decimals is not null
             and pp.price_usd is not null
            then (
                r.principal_amount_raw
                / pow(10, p.decimals)
            ) * pp.price_usd
        end as repaid_principal_usd,

        case
            when r.principal_asset_type = 'Token'
             and p.decimals is not null
             and pp.price_usd is not null
            then (
                r.interest_raw
                / pow(10, p.decimals)
            ) * pp.price_usd
        end as realized_interest_usd,

        case
            when r.principal_asset_type = 'Token'
             and p.decimals is not null
             and pp.price_usd is not null
            then (
                (
                    r.principal_amount_raw
                    + r.interest_raw
                )
                / pow(10, p.decimals)
            ) * pp.price_usd
        end as total_repayment_value_usd,

        case
            when r.collateral_asset_type = 'Token'
             and c.decimals is not null
             and cp.price_usd is not null
            then (
                r.collateral_amount_raw
                / pow(10, c.decimals)
            ) * cp.price_usd
        end as collateral_value_at_repayment_usd,

        case
            when r.principal_asset_type != 'Token'
                then 'not_applicable'
            when pp.price_usd is null
                then 'missing'
            when pp.price_source = 'fixed_usd_stablecoin'
                then 'fixed_stablecoin'
            else 'daily_reference'
        end as principal_price_status,

        case
            when r.collateral_asset_type != 'Token'
                then 'not_applicable'
            when cp.price_usd is null
                then 'missing'
            when cp.price_source = 'fixed_usd_stablecoin'
                then 'fixed_stablecoin'
            else 'daily_reference'
        end as collateral_price_status,

        r.apy_raw,
        r.duration_raw,

        r.principal_asset,
        r.collateral_asset,

        r.idl_version,
        r.idl_sha256

    from repayments as r

    left join tokens as p
        on r.principal_mint = p.mint_address

    left join tokens as c
        on r.collateral_mint = c.mint_address

    left join prices as pp
        on r.principal_mint = pp.mint_address
        and r.price_date = pp.price_date

    left join prices as cp
        on r.collateral_mint = cp.mint_address
        and r.price_date = cp.price_date

)

select *
from final
