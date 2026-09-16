{{ config(
    materialized='view'
) }}

with loans as (

    select
        *,

        cast(
            loan_created_at as date
        ) as cohort_date

    from {{ ref('int_offerbook_loan_lifecycle') }}

),

cohorts as (

    select

        --------------------------------------------------
        -- Cohort identity
        --------------------------------------------------

        cohort_date,

        principal_asset_key,
        principal_asset_type,
        principal_mint,
        principal_symbol,

        collateral_asset_key,
        collateral_asset_type,
        collateral_mint,
        collateral_symbol,

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
        ) as market_name,

        --------------------------------------------------
        -- Cohort age
        --------------------------------------------------

        date_diff(
            'day',
            cohort_date,
            current_date
        ) as cohort_age_days,

        --------------------------------------------------
        -- Originated loans
        --------------------------------------------------

        count(*) as originated_loans,

        count(
            distinct lender_address
        ) as unique_lenders,

        count(
            distinct borrower_address
        ) as unique_borrowers,

        --------------------------------------------------
        -- Current lifecycle outcomes
        --------------------------------------------------

        count(
            case
                when lifecycle_status = 'Active'
                then 1
            end
        ) as active_loans,

        count(
            case
                when lifecycle_status = 'Repaid'
                then 1
            end
        ) as repaid_loans,

        count(
            case
                when lifecycle_status = 'Defaulted'
                then 1
            end
        ) as defaulted_loans,

        count(
            case
                when is_closed = true
                then 1
            end
        ) as closed_loans,

        count(
            case
                when is_open = true
                 and is_past_due = true
                then 1
            end
        ) as past_due_open_loans,

        --------------------------------------------------
        -- Extensions
        --------------------------------------------------

        count(
            case
                when observed_extension_events > 0
                then 1
            end
        ) as extended_loans,

        sum(
            observed_extension_events
        ) as extension_events,

        --------------------------------------------------
        -- Origination-value USD
        --------------------------------------------------

        sum(
            origination_volume_usd
        ) as cohort_origination_volume_usd,

        sum(
            case
                when lifecycle_status = 'Active'
                then origination_volume_usd
            end
        ) as active_origination_volume_usd,

        sum(
            case
                when lifecycle_status = 'Repaid'
                then origination_volume_usd
            end
        ) as repaid_origination_volume_usd,

        sum(
            case
                when lifecycle_status = 'Defaulted'
                then origination_volume_usd
            end
        ) as defaulted_origination_volume_usd,

        sum(
            case
                when is_open = true
                 and is_past_due = true
                then origination_volume_usd
            end
        ) as past_due_origination_volume_usd,

        --------------------------------------------------
        -- Loan size
        --------------------------------------------------

        avg(
            origination_volume_usd
        ) as avg_loan_size_usd,

        median(
            origination_volume_usd
        ) as median_loan_size_usd,

        --------------------------------------------------
        -- Lifecycle timing
        --------------------------------------------------

        avg(
            case
                when lifecycle_status = 'Repaid'
                then days_to_close
            end
        ) as avg_days_to_repay,

        median(
            case
                when lifecycle_status = 'Repaid'
                then days_to_close
            end
        ) as median_days_to_repay,

        avg(
            case
                when lifecycle_status = 'Defaulted'
                then days_to_close
            end
        ) as avg_days_to_default,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        count(
            case
                when origination_volume_usd is not null
                then 1
            end
        ) as priced_loans,

        count(
            case
                when origination_volume_usd is null
                then 1
            end
        ) as unpriced_loans

    from loans

group by
    1, 2, 3, 4, 5,
    6, 7, 8, 9

),

final as (

    select
        *,

        --------------------------------------------------
        -- Loan-count outcome rates
        --------------------------------------------------

        100.0
        * active_loans
        / nullif(
            originated_loans,
            0
        ) as active_rate_pct,

        100.0
        * repaid_loans
        / nullif(
            originated_loans,
            0
        ) as repayment_rate_pct,

        100.0
        * defaulted_loans
        / nullif(
            originated_loans,
            0
        ) as default_rate_pct,

        100.0
        * closed_loans
        / nullif(
            originated_loans,
            0
        ) as resolved_rate_pct,

        --------------------------------------------------
        -- Outcomes among CLOSED loans only
        --------------------------------------------------

        100.0
        * repaid_loans
        / nullif(
            closed_loans,
            0
        ) as repayment_share_of_closed_pct,

        100.0
        * defaulted_loans
        / nullif(
            closed_loans,
            0
        ) as default_share_of_closed_pct,

        --------------------------------------------------
        -- Past due
        --------------------------------------------------

        100.0
        * past_due_open_loans
        / nullif(
            active_loans,
            0
        ) as past_due_share_of_active_pct,

        --------------------------------------------------
        -- Volume-weighted outcomes
        --------------------------------------------------

        100.0
        * repaid_origination_volume_usd
        / nullif(
            cohort_origination_volume_usd,
            0
        ) as repaid_volume_share_pct,

        100.0
        * defaulted_origination_volume_usd
        / nullif(
            cohort_origination_volume_usd,
            0
        ) as defaulted_volume_share_pct,

        --------------------------------------------------
        -- Extension rate
        --------------------------------------------------

        100.0
        * extended_loans
        / nullif(
            originated_loans,
            0
        ) as extension_rate_pct,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        100.0
        * priced_loans
        / nullif(
            originated_loans,
            0
        ) as price_coverage_pct

    from cohorts

)

select *
from final

order by
    cohort_date,
    cohort_origination_volume_usd desc nulls last