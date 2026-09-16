{{ config(
    materialized='view'
) }}

with loans as (

    select *
    from {{ ref('int_offerbook_loans_enriched') }}

),

daily_metrics as (

    select

        cast(
            loan_created_at as date
        ) as block_date,

        --------------------------------------------------
        -- Loan activity
        --------------------------------------------------

        count(*) as loan_count,

        count(
            distinct offer_address
        ) as filled_offer_count,

        --------------------------------------------------
        -- Participants
        --------------------------------------------------

        count(
            distinct lender_address
        ) as unique_lenders,

        count(
            distinct borrower_address
        ) as unique_borrowers,

        --------------------------------------------------
        -- USD values
        --------------------------------------------------

        sum(
            principal_amount_usd
        ) as origination_volume_usd,

        sum(
            collateral_amount_usd
        ) as collateral_value_usd,

        sum(
            interest_usd
        ) as interest_usd,

        --------------------------------------------------
        -- Loan size
        --------------------------------------------------

        avg(
            principal_amount_usd
        ) as avg_loan_size_usd,

        median(
            principal_amount_usd
        ) as median_loan_size_usd,

        --------------------------------------------------
        -- Loan terms
        --------------------------------------------------

        avg(
            apy_raw
        ) as avg_apy_raw,

        avg(
            duration_raw
        ) as avg_duration_raw,

        --------------------------------------------------
        -- Markets
        --------------------------------------------------

        count(
            distinct concat(
                principal_mint,
                ':',
                collateral_mint
            )
        ) as token_pair_count,

        --------------------------------------------------
        -- Price coverage
        --------------------------------------------------

        count(
            case
                when principal_price_usd
                     is not null
                then 1
            end
        ) as priced_loan_count,

        count(
            case
                when principal_price_usd
                     is null
                then 1
            end
        ) as unpriced_loan_count

    from loans

    group by 1

),

users as (

    select
        cast(
            loan_created_at as date
        ) as block_date,

        lender_address as user_address

    from loans

    where lender_address is not null


    union


    select
        cast(
            loan_created_at as date
        ) as block_date,

        borrower_address as user_address

    from loans

    where borrower_address is not null

),

daily_users as (

    select
        block_date,

        count(
            distinct user_address
        ) as unique_users

    from users

    group by 1

)

select

    d.block_date,

    d.loan_count,
    d.filled_offer_count,

    d.unique_lenders,
    d.unique_borrowers,

    u.unique_users,

    d.origination_volume_usd,
    d.collateral_value_usd,
    d.interest_usd,

    d.avg_loan_size_usd,
    d.median_loan_size_usd,

    d.avg_apy_raw,
    d.avg_duration_raw,

    d.token_pair_count,

    d.priced_loan_count,
    d.unpriced_loan_count

from daily_metrics as d

left join daily_users as u
    on d.block_date = u.block_date