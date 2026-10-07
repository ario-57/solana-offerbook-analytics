{{ config(
    materialized='table'
) }}

with events as (

    select *
    from {{ ref('int_offerbook_offer_events') }}

),

created_ranked as (

    select
        *,
        row_number() over (
            partition by offer_address
            order by
                offer_created_at asc nulls last,
                block_timestamp asc,
                signature asc,
                inner_instruction_index asc
        ) as rn

    from events

    where event_type = 'created'
),

created as (

    select *
    from created_ranked
    where rn = 1

),

latest_ranked as (

    select
        *,
        row_number() over (
            partition by offer_address
            order by
                block_timestamp desc,
                signature desc,
                inner_instruction_index desc
        ) as rn

    from events
),

latest as (

    select *
    from latest_ranked
    where rn = 1

),

event_stats as (

    select
        offer_address,

        count(*) as observed_offer_events,

        count(
            case when event_type = 'filled' then 1 end
        ) as observed_fill_events,

        count(
            case when event_type = 'cancelled' then 1 end
        ) as observed_cancel_events,

        min(
            case when event_type = 'filled' then block_timestamp end
        ) as first_fill_event_at,

        max(
            case when event_type = 'filled' then block_timestamp end
        ) as last_fill_event_at,

        max(
            case when event_type = 'cancelled' then block_timestamp end
        ) as cancelled_at

    from events

    group by 1
),

loans as (

    select
        offer_address,

        count(*) as loan_count,

        min(loan_created_at)
            as first_loan_created_at,

        max(loan_created_at)
            as last_loan_created_at,

        sum(principal_amount_raw)
            as filled_principal_raw,

        sum(collateral_amount_raw)
            as filled_collateral_raw,

        sum(principal_amount)
            as filled_principal_amount,

        sum(collateral_amount)
            as filled_collateral_amount,

        count(distinct lender_address)
            as unique_lenders,

        count(distinct borrower_address)
            as unique_borrowers

    from {{ ref('int_offerbook_loans_enriched') }}

    group by 1
),

creation_metadata as (

    select
        offer_address,

        any_value(principal_symbol)
            as principal_symbol,

        any_value(collateral_symbol)
            as collateral_symbol,

        any_value(principal_amount)
            as offered_principal_amount,

        any_value(collateral_amount)
            as offered_collateral_amount

    from {{ ref('int_offerbook_offer_creations_enriched') }}

    group by 1
),

combined as (

    select
        c.offer_address,

        c.signature
            as creation_signature,

        c.event_version
            as creation_event_version,

        c.creator_address,

        c.offer_side,

        case
            when lower(c.offer_side) = 'principal'
                then 'lender'
            when lower(c.offer_side) = 'collateral'
                then 'borrower'
            else 'unknown'
        end as creator_role,

        c.principal_asset_type,
        c.principal_asset_key,

        c.collateral_asset_type,
        c.collateral_asset_key,

        c.principal_amount_raw,
        c.collateral_amount_raw,

        c.apy_raw,
        c.duration_raw,

        c.offer_created_at,
        c.offer_expired_at,

        c.min_fill_amount_raw,
        c.allow_partial_fill,
        c.allow_extend,
        c.countered_offer_address,

        l.event_name
            as latest_event_name,

        l.event_type
            as latest_event_type,

        l.offer_status
            as latest_protocol_status,

        l.remaining_principal_raw,
        l.remaining_collateral_raw,
        l.fill_counter,

        s.observed_offer_events,
        s.observed_fill_events,
        s.observed_cancel_events,

        coalesce(
            n.first_loan_created_at,
            s.first_fill_event_at
        ) as first_fill_at,

        coalesce(
            n.last_loan_created_at,
            s.last_fill_event_at
        ) as last_fill_at,

        s.cancelled_at,

        coalesce(n.loan_count, 0)
            as loan_count,

        coalesce(n.filled_principal_raw, 0)
            as filled_principal_raw,

        coalesce(n.filled_collateral_raw, 0)
            as filled_collateral_raw,

        n.filled_principal_amount,
        n.filled_collateral_amount,

        coalesce(n.unique_lenders, 0)
            as unique_lenders,

        coalesce(n.unique_borrowers, 0)
            as unique_borrowers,

        m.principal_symbol,
        m.collateral_symbol,
        m.offered_principal_amount,
        m.offered_collateral_amount

    from created as c

    left join latest as l
        on c.offer_address = l.offer_address

    left join event_stats as s
        on c.offer_address = s.offer_address

    left join loans as n
        on c.offer_address = n.offer_address

    left join creation_metadata as m
        on c.offer_address = m.offer_address
),

final as (

    select
        *,

        case
            when latest_protocol_status = 'Cancelled'
              or latest_event_type = 'cancelled'
                then 'Cancelled'

            when latest_protocol_status = 'Fulfilled'
                then 'Fulfilled'

            when offer_expired_at is not null
             and offer_expired_at < current_timestamp
                then 'Expired'

            when latest_protocol_status = 'PartiallyFilled'
              or coalesce(loan_count, 0) > 0
                then 'PartiallyFilled'

            else 'Active'
        end as lifecycle_status,

        coalesce(loan_count, 0) > 0
            as has_fill,

        date_diff(
            'second',
            offer_created_at,
            first_fill_at
        ) as seconds_to_first_fill,

        date_diff(
            'second',
            offer_created_at,
            coalesce(cancelled_at, offer_expired_at)
        ) as seconds_to_unfilled_close,

        date_diff(
            'second',
            offer_created_at,
            current_timestamp
        ) as offer_age_seconds,

        case
            when principal_amount_raw is not null
             and principal_amount_raw != 0
            then
                least(
                    1.0,
                    greatest(
                        0.0,
                        filled_principal_raw::double
                        / principal_amount_raw::double
                    )
                )
        end as principal_fill_ratio,

        case
            when offered_principal_amount is not null
            then greatest(
                0,
                offered_principal_amount
                - coalesce(filled_principal_amount, 0)
            )
        end as remaining_principal_amount,

        case
            when lifecycle_status in ('Active', 'PartiallyFilled')
             and date_diff(
                 'day',
                 offer_created_at,
                 current_timestamp
             ) >= 7
                then true
            else false
        end as is_stale_7d,

        case
            when lifecycle_status in ('Active', 'PartiallyFilled')
             and date_diff(
                 'day',
                 offer_created_at,
                 current_timestamp
             ) >= 30
                then true
            else false
        end as is_stale_30d

    from combined
),

status_flags as (

    select
        *,

        lifecycle_status in (
            'Active',
            'PartiallyFilled'
        ) as is_open,

        lifecycle_status in (
            'Fulfilled',
            'Cancelled',
            'Expired'
        ) as is_closed

    from final
)

select *
from status_flags
