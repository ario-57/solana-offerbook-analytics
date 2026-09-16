select
    signature,

    program_id,

    slot,

    to_timestamp(block_time) as block_timestamp,

    case
        when json_extract(transaction_json, '$.meta.err') = 'null'
            then true
        else false
    end as is_success,

    transaction_json->'meta'->'err' as error,

    cast(
        transaction_json->'meta'->>'fee'
        as bigint
    ) as fee_lamports,

    transaction_json,

    ingested_at

from {{ source('solana_raw', 'offerbook_transactions') }}