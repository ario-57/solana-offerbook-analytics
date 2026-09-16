{{ config(
    materialized='view'
) }}

with transactions as (

    select
        signature,
        slot,
        block_timestamp,
        is_success,
        transaction_json

    from {{ ref('stg_offerbook_transactions') }}

),

inner_groups as (

    select
        t.signature,
        t.slot,
        t.block_timestamp,
        t.is_success,

        cast(
            inner_group.value->>'index'
            as integer
        ) as parent_instruction_index,

        inner_group.value
            as inner_group_json,

        t.transaction_json

    from transactions as t,

         json_each(
             t.transaction_json,
             '$.meta.innerInstructions'
         ) as inner_group

),

inner_instructions as (

    select
        g.signature,
        g.slot,
        g.block_timestamp,
        g.is_success,

        g.parent_instruction_index,

        cast(
            inner_instruction.key
            as integer
        ) as inner_instruction_index,

        cast(
            inner_instruction.value->>'programIdIndex'
            as integer
        ) as program_id_index,

        inner_instruction.value->'accounts'
            as account_indexes,

        inner_instruction.value->>'data'
            as instruction_data,

        try_cast(
            inner_instruction.value->>'stackHeight'
            as integer
        ) as stack_height,

        inner_instruction.value
            as instruction_json,

        g.transaction_json

    from inner_groups as g,

         json_each(
             g.inner_group_json,
             '$.instructions'
         ) as inner_instruction

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

    from inner_instructions

)

select *
from resolved_programs