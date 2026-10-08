{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['block_date', 'instruction_name']
) }}

with decoded as (

    select
        i.signature,
        i.block_timestamp,
        i.is_success,
        coalesce(d.instruction_name, 'unknown')
            as instruction_name

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

instruction_counts as (

    select
        cast(block_timestamp at time zone 'UTC' as date)
            as block_date,

        instruction_name,

        count(*) as instruction_count

    from decoded

    group by 1, 2
),

transaction_types as (

    select distinct
        signature,
        block_timestamp,
        is_success,
        instruction_name

    from decoded
),

transaction_metrics as (

    select
        signature,
        fee_lamports,

        try_cast(
            json_extract_string(
                transaction_json,
                '$.meta.computeUnitsConsumed'
            )
            as bigint
        ) as compute_units_consumed

    from {{ ref('stg_offerbook_transactions') }}
),

associated as (

    select
        cast(t.block_timestamp at time zone 'UTC' as date)
            as block_date,

        t.instruction_name,
        t.signature,
        t.is_success,
        m.compute_units_consumed,
        m.fee_lamports

    from transaction_types as t

    left join transaction_metrics as m
        using (signature)
),

aggregated as (

    select
        block_date,
        instruction_name,

        count(*) as transaction_count,

        count(case when is_success then 1 end)
            as successful_transactions,

        count(case when not is_success then 1 end)
            as failed_transactions,

        100.0
        * count(case when is_success then 1 end)
        / nullif(count(*), 0)
            as success_rate_pct,

        sum(compute_units_consumed)
            as associated_compute_units,

        avg(compute_units_consumed)
            as avg_associated_compute_units,

        median(compute_units_consumed)
            as median_associated_compute_units,

        quantile_cont(compute_units_consumed, 0.9)
            as p90_associated_compute_units,

        sum(fee_lamports)
            as associated_fee_lamports,

        sum(fee_lamports) / 1000000000.0
            as associated_fee_sol,

        median(fee_lamports)
            as median_associated_fee_lamports

    from associated

    group by 1, 2
)

select
    a.block_date,
    a.instruction_name,
    c.instruction_count,
    a.transaction_count,
    a.successful_transactions,
    a.failed_transactions,
    a.success_rate_pct,
    a.associated_compute_units,
    a.avg_associated_compute_units,
    a.median_associated_compute_units,
    a.p90_associated_compute_units,
    a.associated_fee_lamports,
    a.associated_fee_sol,
    a.median_associated_fee_lamports

from aggregated as a

left join instruction_counts as c
    using (block_date, instruction_name)

order by
    a.block_date,
    a.transaction_count desc
