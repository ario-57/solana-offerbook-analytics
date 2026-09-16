{{ config(
    materialized='view'
) }}

with extension_events as (

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

    where event_name = 'LoanExtended'
      and is_success = true
      and decode_status = 'ok'

),

parsed as (

    select
        signature,
        parent_instruction_index,
        inner_instruction_index,

        block_timestamp,

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
        -- Current loan state after extension
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
        -- Assets
        --------------------------------------------------

        json_extract_string(
            decoded_event,
            '$.loan.principal.variant'
        ) as principal_asset_type,

        json_extract_string(
            decoded_event,
            '$.loan.collateral.variant'
        ) as collateral_asset_type,

        json_extract(
            decoded_event,
            '$.loan.principal'
        ) as principal_asset,

        json_extract(
            decoded_event,
            '$.loan.collateral'
        ) as collateral_asset,

        --------------------------------------------------
        -- Token mints
        --------------------------------------------------

        case
            when json_extract_string(
                decoded_event,
                '$.loan.principal.variant'
            ) = 'Token'

            then json_extract_string(
                decoded_event,
                '$.loan.principal.fields[0].mint'
            )
        end as principal_mint,

        case
            when json_extract_string(
                decoded_event,
                '$.loan.collateral.variant'
            ) = 'Token'

            then json_extract_string(
                decoded_event,
                '$.loan.collateral.fields[0].mint'
            )
        end as collateral_mint,

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
        -- Terms after extension
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
        -- Extension fields
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.extendable'
            )
            as integer
        ) = 1 as is_extendable,

        try_cast(
            json_extract_string(
                decoded_event,
                '$.loan.extension_count'
            )
            as integer
        ) as extension_count,

        --------------------------------------------------
        -- Decoder lineage
        --------------------------------------------------

        idl_version,
        idl_sha256

    from extension_events

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
        ) as loan_updated_at,

        block_timestamp
            as extended_at

    from parsed

)

select *
from final