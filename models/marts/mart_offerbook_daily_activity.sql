{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['block_date', 'instruction_name']
) }}

with instructions as (

    select
        i.signature,
        i.instruction_index,
        i.block_timestamp,
        i.is_success,

        d.instruction_name,
        d.decode_status

    from {{ ref('int_offerbook_instructions') }} as i

    left join {{ ref('stg_offerbook_decoded_instructions') }} as d
        on i.signature = d.signature
        and i.instruction_index = d.instruction_index

    {% if is_incremental() %}

    where cast(i.block_timestamp as date) >= (

        select
            max(block_date) - interval 7 day

        from {{ this }}

    )

    {% endif %}

),

daily_activity as (

    select
        cast(block_timestamp as date) as block_date,

        coalesce(
            instruction_name,
            'unknown'
        ) as instruction_name,

        count(*) as instruction_count,

        count(distinct signature) as transaction_count,

        count(
            distinct case
                when is_success = true
                then signature
            end
        ) as successful_transactions,

        count(
            distinct case
                when is_success = false
                then signature
            end
        ) as failed_transactions,

        sum(
            case
                when is_success = true
                then 1
                else 0
            end
        ) as successful_instructions,

        sum(
            case
                when is_success = false
                then 1
                else 0
            end
        ) as failed_instructions

    from instructions

    group by
        block_date,
        instruction_name

)

select *
from daily_activity