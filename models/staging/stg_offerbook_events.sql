{{ config(
    materialized='view'
) }}

select
    signature,

    parent_instruction_index,
    inner_instruction_index,

    block_timestamp,
    is_success,

    event_name,
    event_discriminator,

    decoded_event,

    remaining_bytes_hex,

    decode_status,
    decode_error,

    idl_version,
    idl_sha256,

    decoded_at

from {{
    source(
        'offerbook_decoded',
        'offerbook_events'
    )
}}