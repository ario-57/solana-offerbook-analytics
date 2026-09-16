{{ config(
    materialized='view'
) }}

with

------------------------------------------------------------
-- 1. ORIGINATIONS
--
-- Grain:
-- block_date
-- + principal_asset_key
-- + collateral_asset_key
------------------------------------------------------------

originations as (

    select

        cast(
            loan_created_at as date
        ) as block_date,

        principal_asset_key,
        collateral_asset_key,

        --------------------------------------------------
        -- Descriptive attributes
        --
        -- These are NOT part of the grain.
        --------------------------------------------------

        any_value(
            principal_asset_type
        ) as principal_asset_type,

        any_value(
            principal_mint
        ) as principal_mint,

        any_value(
            principal_symbol
        ) as principal_symbol,

        any_value(
            collateral_asset_type
        ) as collateral_asset_type,

        any_value(
            collateral_mint
        ) as collateral_mint,

        any_value(
            collateral_symbol
        ) as collateral_symbol,

        --------------------------------------------------
        -- Activity
        --------------------------------------------------

        count(*) as originated_loans,

        count(
            distinct lender_address
        ) as origination_lenders,

        count(
            distinct borrower_address
        ) as origination_borrowers,

        --------------------------------------------------
        -- USD values
        --------------------------------------------------

        sum(
            origination_volume_usd
        ) as origination_volume_usd,

        sum(
            origination_collateral_value_usd
        ) as origination_collateral_value_usd,

        avg(
            origination_volume_usd
        ) as avg_origination_size_usd,

        median(
            origination_volume_usd
        ) as median_origination_size_usd,

        --------------------------------------------------
        -- Terms
        --------------------------------------------------

        avg(
            apy_raw
        ) as avg_apy_raw,

        avg(
            original_duration_raw
        ) as avg_duration_raw,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        count(
            case
                when origination_volume_usd is not null
                then 1
            end
        ) as priced_originations,

        count(
            case
                when origination_volume_usd is null
                then 1
            end
        ) as unpriced_originations

    from {{ ref('int_offerbook_loan_lifecycle') }}

    where principal_asset_key is not null
      and collateral_asset_key is not null

    group by
        1,
        2,
        3
),


------------------------------------------------------------
-- 2. REPAYMENTS
--
-- Same grain:
-- block_date
-- + principal_asset_key
-- + collateral_asset_key
------------------------------------------------------------

repayments as (

    select

        cast(
            repaid_at as date
        ) as block_date,

        principal_asset_key,
        collateral_asset_key,

        --------------------------------------------------
        -- Descriptive attributes
        --------------------------------------------------

        any_value(
            principal_asset_type
        ) as principal_asset_type,

        any_value(
            principal_mint
        ) as principal_mint,

        any_value(
            principal_symbol
        ) as principal_symbol,

        any_value(
            collateral_asset_type
        ) as collateral_asset_type,

        any_value(
            collateral_mint
        ) as collateral_mint,

        any_value(
            collateral_symbol
        ) as collateral_symbol,

        --------------------------------------------------
        -- Activity
        --------------------------------------------------

        count(*) as repaid_loans,

        count(
            distinct lender_address
        ) as repayment_lenders,

        count(
            distinct borrower_address
        ) as repayment_borrowers,

        --------------------------------------------------
        -- USD values at repayment
        --------------------------------------------------

        sum(
            repaid_principal_usd
        ) as repaid_principal_usd,

        sum(
            realized_interest_usd
        ) as realized_interest_usd,

        sum(
            total_repayment_value_usd
        ) as total_repayment_value_usd,

        avg(
            realized_interest_usd
        ) as avg_realized_interest_usd,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        count(
            case
                when principal_price_usd_at_repayment
                     is not null
                then 1
            end
        ) as priced_repayments,

        count(
            case
                when principal_price_usd_at_repayment
                     is null
                then 1
            end
        ) as unpriced_repayments

    from {{ ref('int_offerbook_loan_repayments_enriched') }}

    where principal_asset_key is not null
      and collateral_asset_key is not null

    group by
        1,
        2,
        3
),


------------------------------------------------------------
-- 3. DEFAULTS
--
-- Same grain:
-- block_date
-- + principal_asset_key
-- + collateral_asset_key
------------------------------------------------------------

defaults as (

    select

        cast(
            defaulted_at as date
        ) as block_date,

        principal_asset_key,
        collateral_asset_key,

        --------------------------------------------------
        -- Descriptive attributes
        --------------------------------------------------

        any_value(
            principal_asset_type
        ) as principal_asset_type,

        any_value(
            principal_mint
        ) as principal_mint,

        any_value(
            principal_symbol
        ) as principal_symbol,

        any_value(
            collateral_asset_type
        ) as collateral_asset_type,

        any_value(
            collateral_mint
        ) as collateral_mint,

        any_value(
            collateral_symbol
        ) as collateral_symbol,

        --------------------------------------------------
        -- Activity
        --------------------------------------------------

        count(*) as defaulted_loans,

        count(
            distinct lender_address
        ) as default_lenders,

        count(
            distinct borrower_address
        ) as default_borrowers,

        --------------------------------------------------
        -- USD values at default
        --------------------------------------------------

        sum(
            defaulted_principal_usd
        ) as defaulted_principal_usd,

        sum(
            collateral_value_at_default_usd
        ) as collateral_value_at_default_usd,

        sum(
            contractual_interest_at_default_usd
        ) as contractual_interest_at_default_usd,

        avg(
            collateral_to_principal_ratio_at_default
        ) as avg_collateral_to_principal_ratio_at_default,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        count(
            case
                when principal_price_usd_at_default
                     is not null
                then 1
            end
        ) as principal_priced_defaults,

        count(
            case
                when principal_price_usd_at_default
                     is null
                then 1
            end
        ) as principal_unpriced_defaults,

        count(
            case
                when collateral_price_usd_at_default
                     is not null
                then 1
            end
        ) as collateral_priced_defaults,

        count(
            case
                when collateral_price_usd_at_default
                     is null
                then 1
            end
        ) as collateral_unpriced_defaults

    from {{ ref('int_offerbook_loan_defaults_enriched') }}

    where principal_asset_key is not null
      and collateral_asset_key is not null

    group by
        1,
        2,
        3
),


------------------------------------------------------------
-- 4. ALL MARKET KEYS
--
-- First use UNION ALL so we don't depend on descriptive
-- fields for deduplication.
------------------------------------------------------------

market_keys_raw as (

    select
        block_date,
        principal_asset_key,
        collateral_asset_key

    from originations


    union all


    select
        block_date,
        principal_asset_key,
        collateral_asset_key

    from repayments


    union all


    select
        block_date,
        principal_asset_key,
        collateral_asset_key

    from defaults

),


------------------------------------------------------------
-- Explicitly guarantee ONE row per market key.
------------------------------------------------------------

market_keys as (

    select
        block_date,
        principal_asset_key,
        collateral_asset_key

    from market_keys_raw

    group by
        1,
        2,
        3

),


------------------------------------------------------------
-- 5. JOIN ALL ACTIVITY
------------------------------------------------------------

combined as (

    select

        --------------------------------------------------
        -- TRUE PRIMARY KEY
        --------------------------------------------------

        k.block_date,

        k.principal_asset_key,
        k.collateral_asset_key,

        --------------------------------------------------
        -- Principal metadata
        --------------------------------------------------

        coalesce(
            o.principal_asset_type,
            r.principal_asset_type,
            d.principal_asset_type
        ) as principal_asset_type,

        coalesce(
            o.principal_mint,
            r.principal_mint,
            d.principal_mint
        ) as principal_mint,

        coalesce(
            o.principal_symbol,
            r.principal_symbol,
            d.principal_symbol
        ) as principal_symbol,

        --------------------------------------------------
        -- Collateral metadata
        --------------------------------------------------

        coalesce(
            o.collateral_asset_type,
            r.collateral_asset_type,
            d.collateral_asset_type
        ) as collateral_asset_type,

        coalesce(
            o.collateral_mint,
            r.collateral_mint,
            d.collateral_mint
        ) as collateral_mint,

        coalesce(
            o.collateral_symbol,
            r.collateral_symbol,
            d.collateral_symbol
        ) as collateral_symbol,

        --------------------------------------------------
        -- Originations
        --------------------------------------------------

        coalesce(
            o.originated_loans,
            0
        ) as originated_loans,

        coalesce(
            o.origination_lenders,
            0
        ) as origination_lenders,

        coalesce(
            o.origination_borrowers,
            0
        ) as origination_borrowers,

        o.origination_volume_usd,

        o.origination_collateral_value_usd,

        o.avg_origination_size_usd,

        o.median_origination_size_usd,

        o.avg_apy_raw,

        o.avg_duration_raw,

        coalesce(
            o.priced_originations,
            0
        ) as priced_originations,

        coalesce(
            o.unpriced_originations,
            0
        ) as unpriced_originations,

        --------------------------------------------------
        -- Repayments
        --------------------------------------------------

        coalesce(
            r.repaid_loans,
            0
        ) as repaid_loans,

        coalesce(
            r.repayment_lenders,
            0
        ) as repayment_lenders,

        coalesce(
            r.repayment_borrowers,
            0
        ) as repayment_borrowers,

        r.repaid_principal_usd,

        r.realized_interest_usd,

        r.total_repayment_value_usd,

        r.avg_realized_interest_usd,

        coalesce(
            r.priced_repayments,
            0
        ) as priced_repayments,

        coalesce(
            r.unpriced_repayments,
            0
        ) as unpriced_repayments,

        --------------------------------------------------
        -- Defaults
        --------------------------------------------------

        coalesce(
            d.defaulted_loans,
            0
        ) as defaulted_loans,

        coalesce(
            d.default_lenders,
            0
        ) as default_lenders,

        coalesce(
            d.default_borrowers,
            0
        ) as default_borrowers,

        d.defaulted_principal_usd,

        d.collateral_value_at_default_usd,

        d.contractual_interest_at_default_usd,

        d.avg_collateral_to_principal_ratio_at_default,

        coalesce(
            d.principal_priced_defaults,
            0
        ) as principal_priced_defaults,

        coalesce(
            d.principal_unpriced_defaults,
            0
        ) as principal_unpriced_defaults,

        coalesce(
            d.collateral_priced_defaults,
            0
        ) as collateral_priced_defaults,

        coalesce(
            d.collateral_unpriced_defaults,
            0
        ) as collateral_unpriced_defaults

    from market_keys as k

    ------------------------------------------------------
    -- Originations
    ------------------------------------------------------

    left join originations as o

        on k.block_date =
           o.block_date

        and k.principal_asset_key =
            o.principal_asset_key

        and k.collateral_asset_key =
            o.collateral_asset_key

    ------------------------------------------------------
    -- Repayments
    ------------------------------------------------------

    left join repayments as r

        on k.block_date =
           r.block_date

        and k.principal_asset_key =
            r.principal_asset_key

        and k.collateral_asset_key =
            r.collateral_asset_key

    ------------------------------------------------------
    -- Defaults
    ------------------------------------------------------

    left join defaults as d

        on k.block_date =
           d.block_date

        and k.principal_asset_key =
            d.principal_asset_key

        and k.collateral_asset_key =
            d.collateral_asset_key

),


------------------------------------------------------------
-- 6. Display fields
------------------------------------------------------------

final as (

    select

        *,

        concat(

            coalesce(
                principal_symbol,
                principal_asset_key
            ),

            ' / ',

            coalesce(
                collateral_symbol,
                collateral_asset_key
            )

        ) as market_name

    from combined

)


select *
from final

order by
    block_date,
    origination_volume_usd desc nulls last,
    principal_asset_key,
    collateral_asset_key