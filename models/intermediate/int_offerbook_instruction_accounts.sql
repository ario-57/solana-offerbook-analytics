{{ config(
    materialized='view'
) }}

with decoded_instructions as (

    select
        signature,
        instruction_index,
        instruction_name,
        decoded_accounts

    from {{ ref('stg_offerbook_decoded_instructions') }}

    where decoded_accounts is not null

),

flattened_accounts as (

    select
        d.signature,
        d.instruction_index,
        d.instruction_name,

        account.key as account_role,

        json_extract_string(
            account.value,
            '$'
        ) as account_address

    from decoded_instructions as d,
         json_each(d.decoded_accounts) as account

)

select *
from flattened_accounts