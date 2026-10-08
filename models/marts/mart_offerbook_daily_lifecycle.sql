{{ config(
    materialized='table'
) }}

with

------------------------------------------------------------
-- 1. Originations
------------------------------------------------------------

originations as (

    select
        cast(
            loan_created_at as date
        ) as block_date,

        count(*) as originated_loans,

        count(
            distinct lender_address
        ) as origination_lenders,

        count(
            distinct borrower_address
        ) as origination_borrowers,

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

        count(
            case
                when origination_volume_usd
                     is not null
                then 1
            end
        ) as priced_originations,

        count(
            case
                when origination_volume_usd
                     is null
                then 1
            end
        ) as unpriced_originations

    from {{ ref('int_offerbook_loan_lifecycle') }}

    group by 1
),

------------------------------------------------------------
-- 2. Repayments
------------------------------------------------------------

repayments as (

    select
        cast(
            repaid_at as date
        ) as block_date,

        count(*) as repaid_loans,

        count(
            distinct lender_address
        ) as repayment_lenders,

        count(
            distinct borrower_address
        ) as repayment_borrowers,

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

    group by 1
),

------------------------------------------------------------
-- 3. Defaults
------------------------------------------------------------

defaults as (

    select
        cast(
            defaulted_at as date
        ) as block_date,

        count(*) as defaulted_loans,

        count(
            distinct lender_address
        ) as default_lenders,

        count(
            distinct borrower_address
        ) as default_borrowers,

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

        count(
            case
                when principal_price_usd_at_default
                     is not null
                then 1
            end
        ) as principal_priced_defaults,

        count(
            case
                when collateral_price_usd_at_default
                     is not null
                then 1
            end
        ) as collateral_priced_defaults

    from {{ ref('int_offerbook_loan_defaults_enriched') }}

    group by 1
),

------------------------------------------------------------
-- 4. Complete lifecycle date set
------------------------------------------------------------

dates as (

    select block_date
    from originations

    union

    select block_date
    from repayments

    union

    select block_date
    from defaults

),

------------------------------------------------------------
-- 5. Daily users across ALL lifecycle activity
------------------------------------------------------------

daily_users_base as (

    --------------------------------------------------------
    -- Origination lenders
    --------------------------------------------------------

    select
        cast(
            loan_created_at as date
        ) as block_date,

        lender_address
            as user_address

    from {{ ref('int_offerbook_loan_lifecycle') }}

    where lender_address is not null


    union


    --------------------------------------------------------
    -- Origination borrowers
    --------------------------------------------------------

    select
        cast(
            loan_created_at as date
        ) as block_date,

        borrower_address
            as user_address

    from {{ ref('int_offerbook_loan_lifecycle') }}

    where borrower_address is not null


    union


    --------------------------------------------------------
    -- Repayment lenders
    --------------------------------------------------------

    select
        cast(
            repaid_at as date
        ) as block_date,

        lender_address
            as user_address

    from {{ ref('int_offerbook_loan_repayments_enriched') }}

    where lender_address is not null


    union


    --------------------------------------------------------
    -- Repayment borrowers
    --------------------------------------------------------

    select
        cast(
            repaid_at as date
        ) as block_date,

        borrower_address
            as user_address

    from {{ ref('int_offerbook_loan_repayments_enriched') }}

    where borrower_address is not null


    union


    --------------------------------------------------------
    -- Default lenders
    --------------------------------------------------------

    select
        cast(
            defaulted_at as date
        ) as block_date,

        lender_address
            as user_address

    from {{ ref('int_offerbook_loan_defaults_enriched') }}

    where lender_address is not null


    union


    --------------------------------------------------------
    -- Default borrowers
    --------------------------------------------------------

    select
        cast(
            defaulted_at as date
        ) as block_date,

        borrower_address
            as user_address

    from {{ ref('int_offerbook_loan_defaults_enriched') }}

    where borrower_address is not null

),

daily_users as (

    select
        block_date,

        count(
            distinct user_address
        ) as unique_active_users

    from daily_users_base

    group by 1
),

------------------------------------------------------------
-- 6. Final daily lifecycle table
------------------------------------------------------------

final as (

    select

        d.block_date,

        ----------------------------------------------------
        -- Originations
        ----------------------------------------------------

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

        coalesce(
            o.priced_originations,
            0
        ) as priced_originations,

        coalesce(
            o.unpriced_originations,
            0
        ) as unpriced_originations,

        ----------------------------------------------------
        -- Repayments
        ----------------------------------------------------

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

        ----------------------------------------------------
        -- Defaults
        ----------------------------------------------------

        coalesce(
            df.defaulted_loans,
            0
        ) as defaulted_loans,

        coalesce(
            df.default_lenders,
            0
        ) as default_lenders,

        coalesce(
            df.default_borrowers,
            0
        ) as default_borrowers,

        df.defaulted_principal_usd,

        df.collateral_value_at_default_usd,

        df.contractual_interest_at_default_usd,

        df.avg_collateral_to_principal_ratio_at_default,

        coalesce(
            df.principal_priced_defaults,
            0
        ) as principal_priced_defaults,

        coalesce(
            df.collateral_priced_defaults,
            0
        ) as collateral_priced_defaults,

        ----------------------------------------------------
        -- Overall daily users
        ----------------------------------------------------

        coalesce(
            u.unique_active_users,
            0
        ) as unique_active_users

    from dates as d

    left join originations as o
        on d.block_date = o.block_date

    left join repayments as r
        on d.block_date = r.block_date

    left join defaults as df
        on d.block_date = df.block_date

    left join daily_users as u
        on d.block_date = u.block_date

)

select *
from final
order by block_date