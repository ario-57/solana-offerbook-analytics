{{ config(materialized='table') }}

with loan_stats as (

    select
        borrower_address as wallet_address,

        count(*) as originated_loans,

        count(case when lifecycle_status = 'Active' then 1 end)
            as active_loans,

        count(case when lifecycle_status = 'Repaid' then 1 end)
            as repaid_loans,

        count(case when lifecycle_status = 'Defaulted' then 1 end)
            as defaulted_loans,

        count(distinct lender_address)
            as unique_lenders,

        sum(origination_volume_usd)
            as borrowed_volume_usd,

        count(case when origination_volume_usd is not null then 1 end)
            as priced_originated_loans,

        avg(apy_raw) / 100.0
            as avg_apy_pct,

        median(apy_raw) / 100.0
            as median_apy_pct,

        avg(effective_duration_raw) / 86400.0
            as avg_duration_days,

        sum(extension_count)
            as total_extensions,

        min(loan_created_at)
            as first_loan_at,

        max(loan_created_at)
            as last_loan_at

    from {{ ref('int_offerbook_loan_lifecycle') }}

    where borrower_address is not null

    group by 1
),

repayment_stats as (

    select
        borrower_address as wallet_address,

        count(*) as repayment_events,

        count(case when realized_interest_usd is not null then 1 end)
            as priced_repayments,

        sum(repaid_principal_usd)
            as repaid_principal_usd,

        sum(realized_interest_usd)
            as realized_interest_paid_usd

    from {{ ref('int_offerbook_loan_repayments_enriched') }}

    where borrower_address is not null

    group by 1
),

default_stats as (

    select
        borrower_address as wallet_address,

        count(*) as default_events,

        count(case when defaulted_principal_usd is not null then 1 end)
            as priced_defaults,

        sum(defaulted_principal_usd)
            as defaulted_principal_usd

    from {{ ref('int_offerbook_loan_defaults_enriched') }}

    where borrower_address is not null

    group by 1
)

select
    b.*,

    coalesce(r.repayment_events, 0)
        as repayment_events,

    coalesce(r.priced_repayments, 0)
        as priced_repayments,

    r.repaid_principal_usd,
    r.realized_interest_paid_usd,

    coalesce(d.default_events, 0)
        as default_events,

    coalesce(d.priced_defaults, 0)
        as priced_defaults,

    d.defaulted_principal_usd,

    100.0 * b.defaulted_loans
        / nullif(b.originated_loans, 0)
        as default_rate_pct,

    100.0 * r.realized_interest_paid_usd
        / nullif(r.repaid_principal_usd, 0)
        as realized_borrowing_cost_pct,

    100.0 * b.priced_originated_loans
        / nullif(b.originated_loans, 0)
        as origination_price_coverage_pct

from loan_stats as b

left join repayment_stats as r
    using (wallet_address)

left join default_stats as d
    using (wallet_address)
