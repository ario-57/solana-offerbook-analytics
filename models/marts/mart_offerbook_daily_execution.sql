{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='block_date'
) }}

with transactions as (

    select
        signature,
        block_timestamp,
        is_success,
        error,
        fee_lamports,

        try_cast(
            json_extract_string(
                transaction_json,
                '$.meta.computeUnitsConsumed'
            )
            as bigint
        ) as compute_units_consumed,

        coalesce(
            json_array_length(
                json_extract(
                    transaction_json,
                    '$.transaction.message.instructions'
                )
            ),
            0
        ) as top_level_instruction_count,

        coalesce(
            json_array_length(
                json_extract(
                    transaction_json,
                    '$.meta.logMessages'
                )
            ),
            0
        ) as log_message_count,

        coalesce(
            json_array_length(
                json_extract(
                    transaction_json,
                    '$.transaction.message.accountKeys'
                )
            ),
            0
        )
        +
        coalesce(
            json_array_length(
                json_extract(
                    transaction_json,
                    '$.meta.loadedAddresses.writable'
                )
            ),
            0
        )
        +
        coalesce(
            json_array_length(
                json_extract(
                    transaction_json,
                    '$.meta.loadedAddresses.readonly'
                )
            ),
            0
        ) as account_count

    from {{ ref('stg_offerbook_transactions') }}

    {% if is_incremental() %}

    where cast(block_timestamp as date) >= (

        select
            max(block_date) - interval 7 day

        from {{ this }}

    )

    {% endif %}
),

inner_counts as (

    select
        signature,
        count(*) as inner_instruction_count

    from {{ ref('stg_offerbook_inner_instructions') }}

    {% if is_incremental() %}

    where cast(block_timestamp as date) >= (

        select
            max(block_date) - interval 7 day

        from {{ this }}

    )

    {% endif %}

    group by 1
),

enriched as (

    select
        t.*,
        coalesce(i.inner_instruction_count, 0)
            as inner_instruction_count

    from transactions as t

    left join inner_counts as i
        using (signature)
)

select
    cast(block_timestamp at time zone 'UTC' as date)
        as block_date,

    count(*) as transactions,

    count(case when is_success then 1 end)
        as successful_transactions,

    count(case when not is_success then 1 end)
        as failed_transactions,

    100.0
    * count(case when is_success then 1 end)
    / nullif(count(*), 0)
        as success_rate_pct,

    sum(compute_units_consumed)
        as total_compute_units,

    avg(compute_units_consumed)
        as avg_compute_units,

    median(compute_units_consumed)
        as median_compute_units,

    quantile_cont(compute_units_consumed, 0.9)
        as p90_compute_units,

    count(case when compute_units_consumed is not null then 1 end)
        as transactions_with_compute_units,

    sum(fee_lamports)
        as total_fee_lamports,

    sum(fee_lamports) / 1000000000.0
        as total_fee_sol,

    avg(fee_lamports)
        as avg_fee_lamports,

    median(fee_lamports)
        as median_fee_lamports,

    avg(top_level_instruction_count)
        as avg_top_level_instructions,

    avg(inner_instruction_count)
        as avg_inner_instructions,

    avg(account_count)
        as avg_accounts,

    avg(log_message_count)
        as avg_log_messages,

    quantile_cont(inner_instruction_count, 0.9)
        as p90_inner_instructions,

    quantile_cont(account_count, 0.9)
        as p90_accounts,

    sum(
        case when is_success
            then compute_units_consumed
        end
    )
    / nullif(
        count(case when is_success then 1 end),
        0
    ) as compute_units_per_successful_tx

from enriched

group by 1

order by 1
