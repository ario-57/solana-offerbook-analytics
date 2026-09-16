{{ config(
    materialized='view'
) }}

with update_events as (

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

    where event_name = 'LoanExtendabilityUpdated'
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
            '$.loan'
        ) as loan_address,

        --------------------------------------------------
        -- New extendability value
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.extendable'
            )
            as integer
        ) = 1 as is_extendable,

        --------------------------------------------------
        -- Protocol timestamp
        --------------------------------------------------

        try_cast(
            json_extract_string(
                decoded_event,
                '$.timestamp'
            )
            as bigint
        ) as updated_at_raw,

        --------------------------------------------------
        -- Decoder lineage
        --------------------------------------------------

        idl_version,
        idl_sha256

    from update_events

),

final as (

    select
        *,

        to_timestamp(
            updated_at_raw
        ) as protocol_updated_at,

        block_timestamp
            as extendability_updated_at

    from parsed

)

select *
from final