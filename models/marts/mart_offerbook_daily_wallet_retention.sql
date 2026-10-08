{{ config(materialized='table') }}

with role_activity as (

    select
        block_date,
        wallet_role,
        wallet_address,
        first_role_activity_date
    from {{ ref('int_offerbook_wallet_activity') }}
),

role_daily as (

    select
        block_date,
        wallet_role,

        count(*) as active_wallets,

        count(
            case when block_date = first_role_activity_date then 1 end
        ) as new_wallets,

        count(
            case when block_date > first_role_activity_date then 1 end
        ) as returning_wallets

    from role_activity

    group by 1, 2
),

overall_activity as (

    select
        block_date,
        wallet_address
    from role_activity
    group by 1, 2
),

overall_with_first as (

    select
        *,

        min(block_date) over (
            partition by wallet_address
        ) as first_activity_date

    from overall_activity
),

overall_daily as (

    select
        block_date,
        'all' as wallet_role,

        count(*) as active_wallets,

        count(
            case when block_date = first_activity_date then 1 end
        ) as new_wallets,

        count(
            case when block_date > first_activity_date then 1 end
        ) as returning_wallets

    from overall_with_first

    group by 1
),

combined as (

    select * from role_daily

    union all

    select * from overall_daily
)

select
    *,

    100.0 * new_wallets
        / nullif(active_wallets, 0)
        as new_wallet_share_pct,

    100.0 * returning_wallets
        / nullif(active_wallets, 0)
        as returning_wallet_share_pct

from combined

order by block_date, wallet_role
