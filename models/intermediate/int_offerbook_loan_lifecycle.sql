{{ config(
    materialized='view'
) }}

with loans as (

    select *
    from {{ ref('int_offerbook_loans_enriched') }}

),

------------------------------------------------------------
-- Rank extensions so we can get the latest state
------------------------------------------------------------

extensions_ranked as (

    select
        *,

        row_number() over (
            partition by loan_address
            order by
                extended_at desc,
                block_timestamp desc,
                signature desc
        ) as rn

    from {{ ref('int_offerbook_loan_extensions') }}

),

latest_extension as (

    select *
    from extensions_ranked
    where rn = 1

),

------------------------------------------------------------
-- Rank extendability updates
------------------------------------------------------------

extendability_ranked as (

    select
        *,

        row_number() over (
            partition by loan_address
            order by
                extendability_updated_at desc,
                block_timestamp desc,
                signature desc
        ) as rn

    from {{ ref('int_offerbook_loan_extendability_updates') }}

),

latest_extendability as (

    select *
    from extendability_ranked
    where rn = 1

),

------------------------------------------------------------
-- Combine terminal events
------------------------------------------------------------

terminal_events as (

    select
        loan_address,

        'Repaid' as terminal_event_type,

        repaid_at as terminal_at,

        signature as terminal_signature,

        loan_status as terminal_loan_status,

        interest_raw as terminal_interest_raw,

        loan_expired_at
            as terminal_loan_expired_at,

        loan_updated_at
            as terminal_loan_updated_at,

        is_extendable
            as terminal_is_extendable,

        extension_count
            as terminal_extension_count

    from {{ ref('int_offerbook_loan_repayments') }}


    union all


    select
        loan_address,

        'Defaulted'
            as terminal_event_type,

        defaulted_at
            as terminal_at,

        signature
            as terminal_signature,

        loan_status
            as terminal_loan_status,

        interest_raw
            as terminal_interest_raw,

        loan_expired_at
            as terminal_loan_expired_at,

        loan_updated_at
            as terminal_loan_updated_at,

        is_extendable
            as terminal_is_extendable,

        extension_count
            as terminal_extension_count

    from {{ ref('int_offerbook_loan_defaults') }}

),

------------------------------------------------------------
-- Normally there should be only one terminal event per loan.
-- Still rank them defensively.
------------------------------------------------------------

terminal_events_ranked as (

    select
        *,

        row_number() over (
            partition by loan_address
            order by
                terminal_at desc,
                terminal_signature desc
        ) as rn

    from terminal_events

),

latest_terminal_event as (

    select *
    from terminal_events_ranked
    where rn = 1

),

------------------------------------------------------------
-- Data-quality information about terminal events
------------------------------------------------------------

terminal_event_counts as (

    select
        loan_address,

        count(*) as terminal_event_count,

        count(
            distinct terminal_event_type
        ) as terminal_event_type_count

    from terminal_events

    group by 1

),

------------------------------------------------------------
-- Count extension events
------------------------------------------------------------

extension_counts as (

    select
        loan_address,

        count(*) as observed_extension_events,

        max(extension_count)
            as max_observed_extension_count

    from {{ ref('int_offerbook_loan_extensions') }}

    group by 1

),

------------------------------------------------------------
-- Assemble current lifecycle state
------------------------------------------------------------

assembled as (

    select

        ----------------------------------------------------
        -- Identity
        ----------------------------------------------------

        l.loan_address,
        l.offer_address,
        l.fill_index,

        l.signature
            as creation_signature,

        l.event_version
            as creation_event_version,

        ----------------------------------------------------
        -- Participants
        ----------------------------------------------------

        l.lender_address,
        l.borrower_address,
        l.creator_address,

        ----------------------------------------------------
        -- Assets
        ----------------------------------------------------

        l.principal_asset_type,
        l.principal_asset_key,
        l.principal_mint,
        l.principal_symbol,
        l.principal_name,
        l.principal_decimals,

        l.collateral_asset_type,
        l.collateral_asset_key,
        l.collateral_mint,
        l.collateral_symbol,
        l.collateral_name,
        l.collateral_decimals,

        ----------------------------------------------------
        -- Original loan values
        ----------------------------------------------------

        l.principal_amount_raw,
        l.principal_amount,

        l.collateral_amount_raw,
        l.collateral_amount,

        l.interest_raw
            as original_interest_raw,

        l.interest_amount
            as original_interest_amount,

        l.apy_raw,

        l.duration_raw
            as original_duration_raw,

        ----------------------------------------------------
        -- Origination USD values
        ----------------------------------------------------

        l.principal_price_usd
            as origination_principal_price_usd,

        l.principal_amount_usd
            as origination_volume_usd,

        l.collateral_price_usd
            as origination_collateral_price_usd,

        l.collateral_amount_usd
            as origination_collateral_value_usd,

        l.principal_price_status,

        l.collateral_price_status,

        ----------------------------------------------------
        -- Original timestamps
        ----------------------------------------------------

        l.loan_created_at,

        l.loan_expired_at
            as original_expired_at,

        l.loan_updated_at
            as original_updated_at,

        ----------------------------------------------------
        -- Extension information
        ----------------------------------------------------

        e.extended_at
            as latest_extended_at,

        coalesce(
            ec.observed_extension_events,
            0
        ) as observed_extension_events,

        coalesce(
            e.extension_count,
            l.extension_count,
            0
        ) as extension_count,

        ec.max_observed_extension_count,

        ----------------------------------------------------
        -- Effective loan terms
        --
        -- Latest extension overrides original state.
        ----------------------------------------------------

        coalesce(
            t.terminal_interest_raw,
            e.interest_raw,
            l.interest_raw
        ) as effective_interest_raw,

        coalesce(
            e.duration_raw,
            l.duration_raw
        ) as effective_duration_raw,

        coalesce(
            t.terminal_loan_expired_at,
            e.loan_expired_at,
            l.loan_expired_at
        ) as effective_expired_at,

        coalesce(
            t.terminal_loan_updated_at,
            e.loan_updated_at,
            l.loan_updated_at
        ) as effective_updated_at,

        ----------------------------------------------------
        -- Extendability
        --
        -- Explicit update has highest priority.
        ----------------------------------------------------

        coalesce(
            eu.is_extendable,
            t.terminal_is_extendable,
            e.is_extendable,
            l.is_extendable
        ) as is_extendable,

        eu.extendability_updated_at
            as latest_extendability_updated_at,

        ----------------------------------------------------
        -- Terminal state
        ----------------------------------------------------

        t.terminal_event_type,

        t.terminal_at,

        t.terminal_signature,

        t.terminal_loan_status,

        coalesce(
            tc.terminal_event_count,
            0
        ) as terminal_event_count,

        coalesce(
            tc.terminal_event_type_count,
            0
        ) as terminal_event_type_count,

        ----------------------------------------------------
        -- Decoder lineage
        ----------------------------------------------------

        l.idl_version,
        l.idl_sha256

    from loans as l

    left join latest_extension as e
        on l.loan_address = e.loan_address

    left join extension_counts as ec
        on l.loan_address = ec.loan_address

    left join latest_extendability as eu
        on l.loan_address = eu.loan_address

    left join latest_terminal_event as t
        on l.loan_address = t.loan_address

    left join terminal_event_counts as tc
        on l.loan_address = tc.loan_address

),

final as (

    select
        *,

        ----------------------------------------------------
        -- Canonical lifecycle state
        ----------------------------------------------------

        case

            when terminal_event_type = 'Repaid'
                then 'Repaid'

            when terminal_event_type = 'Defaulted'
                then 'Defaulted'

            else 'Active'

        end as lifecycle_status,

        ----------------------------------------------------
        -- Open / closed flags
        ----------------------------------------------------

        terminal_event_type is null
            as is_open,

        terminal_event_type is not null
            as is_closed,

        ----------------------------------------------------
        -- Past due
        --
        -- IMPORTANT:
        -- past due does NOT automatically mean defaulted.
        -- We only call a loan Defaulted when we see the
        -- actual LoanDefaulted event.
        ----------------------------------------------------

        (
            terminal_event_type is null
            and effective_expired_at
                < current_timestamp
        ) as is_past_due,

        ----------------------------------------------------
        -- Terminal-event consistency
        ----------------------------------------------------

        terminal_event_type_count > 1
            as has_conflicting_terminal_events,

        ----------------------------------------------------
        -- Time to terminal event
        ----------------------------------------------------

        case
            when terminal_at is not null
            then date_diff(
                'second',
                loan_created_at,
                terminal_at
            )
        end as seconds_to_close,

        case
            when terminal_at is not null
            then
                date_diff(
                    'second',
                    loan_created_at,
                    terminal_at
                ) / 86400.0
        end as days_to_close,

        ----------------------------------------------------
        -- Effective interest normalized by principal
        -- decimals.
        ----------------------------------------------------

        case
            when principal_decimals is not null
            then
                effective_interest_raw
                / pow(
                    10,
                    principal_decimals
                )
        end as effective_interest_amount

    from assembled

)

select *
from final