with transactions as (

    select *
    from {{ ref('stg_offerbook_transactions') }}

),

instructions as (

    select
        t.signature,
        t.slot,
        t.block_timestamp,
        t.is_success,

        cast(instruction.key as integer)
            as instruction_index,

        cast(
            instruction.value->>'programIdIndex'
            as integer
        ) as program_id_index,

        instruction.value->'accounts'
            as account_indexes,

        instruction.value->>'data'
            as instruction_data,

        try_cast(
            instruction.value->>'stackHeight'
            as integer
        ) as stack_height,

        instruction.value
            as instruction_json,

        t.transaction_json

    from transactions as t,
         json_each(
             t.transaction_json,
             '$.transaction.message.instructions'
         ) as instruction

),

resolved_programs as (

    select
        *,

        json_extract_string(
            transaction_json,
            concat(
                '$.transaction.message.accountKeys[',
                cast(program_id_index as varchar),
                ']'
            )
        ) as program_id

    from instructions

)

select *
from resolved_programs