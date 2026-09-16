{{ config(
    materialized='view'
) }}

with loans as (

    select *
    from {{ ref('int_offerbook_loan_lifecycle') }}

),

------------------------------------------------------------
-- Unique protocol users
------------------------------------------------------------

users as (

    select lender_address as user_address
    from loans
    where lender_address is not null

    union

    select borrower_address as user_address
    from loans
    where borrower_address is not null

),

user_metrics as (

    select
        count(*) as unique_users
    from users

),

------------------------------------------------------------
-- Main lifecycle metrics
------------------------------------------------------------

metrics as (

    select

        ----------------------------------------------------
        -- Loan counts
        ----------------------------------------------------

        count(*) as total_loans,

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

        ----------------------------------------------------
        -- Open-loan health
        ----------------------------------------------------

        count(
            case
                when is_open = true
                 and is_past_due = false
                then 1
            end
        ) as open_not_past_due_loans,

        count(
            case
                when is_open = true
                 and is_past_due = true
                then 1
            end
        ) as past_due_open_loans,

        ----------------------------------------------------
        -- Extensions
        ----------------------------------------------------

        count(
            case
                when observed_extension_events > 0
                then 1
            end
        ) as extended_loans,

        sum(
            observed_extension_events
        ) as total_extension_events,

        count(
            case
                when is_open = true
                 and is_extendable = true
                then 1
            end
        ) as extendable_open_loans,

        ----------------------------------------------------
        -- Users
        ----------------------------------------------------

        count(
            distinct lender_address
        ) as unique_lenders,

        count(
            distinct borrower_address
        ) as unique_borrowers,

        ----------------------------------------------------
        -- Total origination volume
        ----------------------------------------------------

        sum(
            origination_volume_usd
        ) as total_origination_volume_usd,

        ----------------------------------------------------
        -- Principal grouped by CURRENT lifecycle state
        --
        -- IMPORTANT:
        -- these values use the USD price at origination.
        ----------------------------------------------------

        sum(
            case
                when is_open = true
                then origination_volume_usd
            end
        ) as open_principal_at_origination_usd,

        sum(
            case
                when lifecycle_status = 'Repaid'
                then origination_volume_usd
            end
        ) as repaid_principal_at_origination_usd,

        sum(
            case
                when lifecycle_status = 'Defaulted'
                then origination_volume_usd
            end
        ) as defaulted_principal_at_origination_usd,

        sum(
            case
                when is_closed = true
                then origination_volume_usd
            end
        ) as closed_principal_at_origination_usd,

        sum(
            case
                when is_open = true
                 and is_past_due = true
                then origination_volume_usd
            end
        ) as past_due_principal_at_origination_usd,

        ----------------------------------------------------
        -- Loan size
        ----------------------------------------------------

        avg(
            origination_volume_usd
        ) as avg_loan_size_usd,

        median(
            origination_volume_usd
        ) as median_loan_size_usd,

        ----------------------------------------------------
        -- Time to closure
        ----------------------------------------------------

        avg(
            case
                when is_closed = true
                then days_to_close
            end
        ) as avg_days_to_close,

        avg(
            case
                when lifecycle_status = 'Repaid'
                then days_to_close
            end
        ) as avg_days_to_repay,

        avg(
            case
                when lifecycle_status = 'Defaulted'
                then days_to_close
            end
        ) as avg_days_to_default,

        ----------------------------------------------------
        -- Price coverage
        ----------------------------------------------------

        count(
            case
                when origination_volume_usd
                     is not null
                then 1
            end
        ) as priced_loan_count,

        count(
            case
                when origination_volume_usd
                     is null
                then 1
            end
        ) as unpriced_loan_count

    from loans

),

final as (

    select

        current_timestamp
            as snapshot_at,

        ----------------------------------------------------
        -- Counts
        ----------------------------------------------------

        m.total_loans,
        m.active_loans,
        m.repaid_loans,
        m.defaulted_loans,
        m.closed_loans,

        m.open_not_past_due_loans,
        m.past_due_open_loans,

        ----------------------------------------------------
        -- Extensions
        ----------------------------------------------------

        m.extended_loans,
        m.total_extension_events,
        m.extendable_open_loans,

        ----------------------------------------------------
        -- Users
        ----------------------------------------------------

        m.unique_lenders,
        m.unique_borrowers,
        u.unique_users,

        ----------------------------------------------------
        -- USD metrics
        ----------------------------------------------------

        m.total_origination_volume_usd,

        m.open_principal_at_origination_usd,
        m.repaid_principal_at_origination_usd,
        m.defaulted_principal_at_origination_usd,
        m.closed_principal_at_origination_usd,

        m.past_due_principal_at_origination_usd,

        ----------------------------------------------------
        -- Loan size
        ----------------------------------------------------

        m.avg_loan_size_usd,
        m.median_loan_size_usd,

        ----------------------------------------------------
        -- Lifecycle duration
        ----------------------------------------------------

        m.avg_days_to_close,
        m.avg_days_to_repay,
        m.avg_days_to_default,

        ----------------------------------------------------
        -- Rates: denominator = ALL originated loans
        ----------------------------------------------------

        100.0
        * m.active_loans
        / nullif(
            m.total_loans,
            0
        ) as active_loan_pct,

        100.0
        * m.repaid_loans
        / nullif(
            m.total_loans,
            0
        ) as repayment_rate_all_loans_pct,

        100.0
        * m.defaulted_loans
        / nullif(
            m.total_loans,
            0
        ) as default_rate_all_loans_pct,

        100.0
        * m.closed_loans
        / nullif(
            m.total_loans,
            0
        ) as closed_loan_pct,

        ----------------------------------------------------
        -- Resolution rates:
        -- denominator = CLOSED loans only
        ----------------------------------------------------

        100.0
        * m.repaid_loans
        / nullif(
            m.closed_loans,
            0
        ) as repayment_share_of_closed_pct,

        100.0
        * m.defaulted_loans
        / nullif(
            m.closed_loans,
            0
        ) as default_share_of_closed_pct,

        ----------------------------------------------------
        -- Past due
        ----------------------------------------------------

        100.0
        * m.past_due_open_loans
        / nullif(
            m.active_loans,
            0
        ) as past_due_share_of_active_pct,

        ----------------------------------------------------
        -- Pricing coverage
        ----------------------------------------------------

        m.priced_loan_count,
        m.unpriced_loan_count,

        100.0
        * m.priced_loan_count
        / nullif(
            m.total_loans,
            0
        ) as price_coverage_pct

    from metrics as m

    cross join user_metrics as u

)

select *
from final