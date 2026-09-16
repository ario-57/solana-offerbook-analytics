select
    signature,

    instruction_index,

    discriminator,
    instruction_name,

    decoded_args,
    decoded_accounts,

    instruction_data,
    instruction_data_hex,

    remaining_bytes_hex,
    decoded_length,

    decode_status,
    decode_error,

    idl_version,
    decoded_at

from {{ source('offerbook_decoded', 'offerbook_instructions') }}