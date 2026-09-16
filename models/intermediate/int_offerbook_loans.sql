{{ config(
    materialized='view'
) }}

with loan_events as (

    select
        signature,
        parent_instruction_index,
        inner_instruction_index,

        block_timestamp,
        is_success,

        event_name,
        decoded_event,

        decode_status,
        idl_version,
        idl_sha256

    from {{ ref('stg_offerbook_events') }}

    where event_name in (
        'LoanCreated',
        'LoanCreatedV1'
    )

      -- Do not count events from reverted transactions.
      and is_success = true

      -- For the canonical loan table we only accept
      -- completely decoded events.
      and decode_status = 'ok'

),

parsed as (

    select
        signature,
        parent_instruction_index,
        inner_instruction_index,

        block_timestamp,

        event_name,

        case
            when event_name = 'LoanCreated'
                then 'v0'

            when event_name = 'LoanCreatedV1'
                then 'v1'
        end as event_version,

        --------------------------------------------------
        -- Loan identity
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.pubkey'
        ) as loan_address,

        json_extract_string(
            decoded_event,
            '$.loan.offer'
        ) as offer_address,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.fill_index'
            )
            as ubigint
        ) as fill_index,

        --------------------------------------------------
        -- Participants
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.loan.lender'
        ) as lender_address,

        json_extract_string(
            decoded_event,
            '$.loan.borrower'
        ) as borrower_address,

        json_extract_string(
            decoded_event,
            '$.loan.creator'
        ) as creator_address,

        --------------------------------------------------
        -- Loan state
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.loan.status.variant'
        ) as loan_status,

        json_extract_string(
            decoded_event,
            '$.loan.loan_type.variant'
        ) as loan_type,

        --------------------------------------------------
        -- Asset types
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.loan.principal.variant'
        ) as principal_asset_type,

        json_extract_string(
            decoded_event,
            '$.loan.collateral.variant'
        ) as collateral_asset_type,

        --------------------------------------------------
        -- Preserve full asset JSON too
        --------------------------------------------------

        json_extract(
            decoded_event,
            '$.loan.principal'
        ) as principal_asset,

        json_extract(
            decoded_event,
            '$.loan.collateral'
        ) as collateral_asset,

        --------------------------------------------------
        -- Token / NFT mint
        --------------------------------------------------

        case

            when json_extract_string(
                decoded_event,
                '$.loan.principal.variant'
            ) in (
                'Token',
                'ClassicNft',
                'ProgrammableNft'
            )

            then json_extract_string(
                decoded_event,
                '$.loan.principal.fields[0].mint'
            )

        end as principal_mint,

        case

            when json_extract_string(
                decoded_event,
                '$.loan.collateral.variant'
            ) in (
                'Token',
                'ClassicNft',
                'ProgrammableNft'
            )

            then json_extract_string(
                decoded_event,
                '$.loan.collateral.fields[0].mint'
            )

        end as collateral_mint,

        --------------------------------------------------
        -- Token programs
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.loan.principal.fields[0].token_program'
        ) as principal_token_program,

        json_extract_string(
            decoded_event,
            '$.loan.collateral.fields[0].token_program'
        ) as collateral_token_program,

        --------------------------------------------------
        -- Amounts
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.principal_amount'
            )
            as ubigint
        ) as principal_amount_raw,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.collateral_amount'
            )
            as ubigint
        ) as collateral_amount_raw,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.interest'
            )
            as ubigint
        ) as interest_raw,

        --------------------------------------------------
        -- Loan terms
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.apy'
            )
            as bigint
        ) as apy_raw,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.duration'
            )
            as bigint
        ) as duration_raw,

        --------------------------------------------------
        -- Protocol timestamps
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.created_at'
            )
            as bigint
        ) as created_at_raw,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.expired_at'
            )
            as bigint
        ) as expired_at_raw,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.updated_at'
            )
            as bigint
        ) as updated_at_raw,

        --------------------------------------------------
        -- V1 extension fields
        --------------------------------------------------

        case
            when event_name = 'LoanCreatedV1'
            then
                try_cast(
                    json_extract_string(
                        decoded_event,
                        '$.loan.extendable'
                    )
                    as integer
                ) = 1
        end as is_extendable,

        case
            when event_name = 'LoanCreatedV1'
            then
                try_cast(
                    json_extract_string(
                        decoded_event,
                        '$.loan.extension_count'
                    )
                    as integer
                )
        end as extension_count,

        --------------------------------------------------
        -- Decode provenance
        --------------------------------------------------

        idl_version,
        idl_sha256

    from loan_events

),

final as (

    select
        *,

        to_timestamp(
            created_at_raw
        ) as loan_created_at,

        to_timestamp(
            expired_at_raw
        ) as loan_expired_at,

        to_timestamp(
            updated_at_raw
        ) as loan_updated_at

    from parsed

)

select *
from final