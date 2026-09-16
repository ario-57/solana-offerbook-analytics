{{ config(
    materialized='view'
) }}

with loans as (

    select
        *,

        date_trunc(
            'hour',
            loan_created_at
        ) as price_hour

    from {{ ref('int_offerbook_loans') }}

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
        -- Loan identity
        --------------------------------------------------

        l.loan_address,
        l.offer_address,

        l.signature,
        l.parent_instruction_index,
        l.inner_instruction_index,

        l.event_name,
        l.event_version,

        --------------------------------------------------
        -- Time
        --------------------------------------------------

        l.block_timestamp,
        l.loan_created_at,
        l.loan_expired_at,
        l.loan_updated_at,

        l.price_hour,

        --------------------------------------------------
        -- Participants
        --------------------------------------------------

        l.lender_address,
        l.borrower_address,
        l.creator_address,

        --------------------------------------------------
        -- Loan state
        --------------------------------------------------

        l.loan_status,
        l.loan_type,
        l.fill_index,

        l.is_extendable,
        l.extension_count,

        --------------------------------------------------
        -- Principal asset
        --------------------------------------------------

        l.principal_asset_type,
        l.principal_mint,
    
        {{ offerbook_asset_key(
            'l.principal_asset',
            'l.principal_asset_type'
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

        l.collateral_asset_type,
        l.collateral_mint,

        {{ offerbook_asset_key(
            'l.collateral_asset',
            'l.collateral_asset_type'
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

        l.principal_amount_raw,
        l.collateral_amount_raw,
        l.interest_raw,

        --------------------------------------------------
        -- Normalized principal
        --------------------------------------------------

        case
            when
                l.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                l.principal_amount_raw
                / pow(10, p.decimals)
        end as principal_amount,

        --------------------------------------------------
        -- Normalized collateral
        --------------------------------------------------

        case
            when
                l.collateral_asset_type = 'Token'
                and c.decimals is not null

            then
                l.collateral_amount_raw
                / pow(10, c.decimals)
        end as collateral_amount,

        --------------------------------------------------
        -- Interest
        --
        -- Interest is denominated in the principal asset,
        -- therefore use principal decimals.
        --------------------------------------------------

        case
            when
                l.principal_asset_type = 'Token'
                and p.decimals is not null

            then
                l.interest_raw
                / pow(10, p.decimals)
        end as interest_amount,

        --------------------------------------------------
        -- Hourly prices
        --------------------------------------------------

        pp.price_usd
            as principal_price_usd,

        cp.price_usd
            as collateral_price_usd,

        --------------------------------------------------
        -- USD amounts
        --------------------------------------------------

        case
            when
                l.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    l.principal_amount_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd
        end as principal_amount_usd,

        case
            when
                l.collateral_asset_type = 'Token'
                and c.decimals is not null
                and cp.price_usd is not null

            then
                (
                    l.collateral_amount_raw
                    / pow(10, c.decimals)
                )
                * cp.price_usd
        end as collateral_amount_usd,

        case
            when
                l.principal_asset_type = 'Token'
                and p.decimals is not null
                and pp.price_usd is not null

            then
                (
                    l.interest_raw
                    / pow(10, p.decimals)
                )
                * pp.price_usd
        end as interest_usd,

        --------------------------------------------------
        -- Price quality
        --------------------------------------------------

        case
            when l.principal_asset_type != 'Token'
                then 'not_applicable'

            when pp.price_usd is null
                then 'missing'

            else 'exact_hour'
        end as principal_price_status,

        case
            when l.collateral_asset_type != 'Token'
                then 'not_applicable'

            when cp.price_usd is null
                then 'missing'

            else 'exact_hour'
        end as collateral_price_status,

        --------------------------------------------------
        -- Original protocol fields
        --------------------------------------------------

        l.apy_raw,
        l.duration_raw,

        l.principal_asset,
        l.collateral_asset,

        --------------------------------------------------
        -- Decoder lineage
        --------------------------------------------------

        l.idl_version,
        l.idl_sha256

    from loans as l

    ------------------------------------------------------
    -- Principal token metadata
    ------------------------------------------------------

    left join tokens as p
        on l.principal_mint =
           p.mint_address

    ------------------------------------------------------
    -- Collateral token metadata
    ------------------------------------------------------

    left join tokens as c
        on l.collateral_mint =
           c.mint_address

    ------------------------------------------------------
    -- Principal hourly price
    ------------------------------------------------------

    left join prices as pp
        on l.principal_mint =
           pp.mint_address

        and l.price_hour =
            pp.price_hour

    ------------------------------------------------------
    -- Collateral hourly price
    ------------------------------------------------------

    left join prices as cp
        on l.collateral_mint =
           cp.mint_address

        and l.price_hour =
            cp.price_hour

)

select *
from final