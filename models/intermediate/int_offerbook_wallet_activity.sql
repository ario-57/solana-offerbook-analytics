{{ config(materialized='view') }}

with activity as (

    select
        cast(loan_created_at at time zone 'UTC' as date) as block_date,
        lender_address as wallet_address,
        'lender' as wallet_role,
        'origination' as activity_type
    from {{ ref('int_offerbook_loan_lifecycle') }}
    where lender_address is not null

    union all

    select
        cast(loan_created_at at time zone 'UTC' as date),
        borrower_address,
        'borrower',
        'origination'
    from {{ ref('int_offerbook_loan_lifecycle') }}
    where borrower_address is not null

    union all

    select
        cast(repaid_at at time zone 'UTC' as date),
        lender_address,
        'lender',
        'repayment'
    from {{ ref('int_offerbook_loan_repayments_enriched') }}
    where lender_address is not null

    union all

    select
        cast(repaid_at at time zone 'UTC' as date),
        borrower_address,
        'borrower',
        'repayment'
    from {{ ref('int_offerbook_loan_repayments_enriched') }}
    where borrower_address is not null

    union all

    select
        cast(defaulted_at at time zone 'UTC' as date),
        lender_address,
        'lender',
        'default'
    from {{ ref('int_offerbook_loan_defaults_enriched') }}
    where lender_address is not null

    union all

    select
        cast(defaulted_at at time zone 'UTC' as date),
        borrower_address,
        'borrower',
        'default'
    from {{ ref('int_offerbook_loan_defaults_enriched') }}
    where borrower_address is not null
),

daily_wallet_role as (

    select
        block_date,
        wallet_address,
        wallet_role,

        count(distinct activity_type) as activity_type_count,

        max(case when activity_type = 'origination' then 1 else 0 end)
            as had_origination,

        max(case when activity_type = 'repayment' then 1 else 0 end)
            as had_repayment,

        max(case when activity_type = 'default' then 1 else 0 end)
            as had_default

    from activity

    group by 1, 2, 3
),

final as (

    select
        *,

        min(block_date) over (
            partition by wallet_address, wallet_role
        ) as first_role_activity_date

    from daily_wallet_role
)

select *
from final
