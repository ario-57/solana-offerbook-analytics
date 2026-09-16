{{ config(
    materialized='view'
) }}

select
    signature,
    slot,
    block_timestamp,
    is_success,

    parent_instruction_index,
    inner_instruction_index,

    program_id,

    account_indexes,
    instruction_data,
    stack_height,

    instruction_json,
    transaction_json

from {{ ref('stg_offerbook_inner_instructions') }}

where program_id =
    'offerbkFMvVfpQhL8ZQ5iromnjct5rz3r52B9ewu3ie'