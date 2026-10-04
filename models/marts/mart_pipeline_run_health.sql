
{{ config(
    materialized='view'
) }}

with history as (

    select
        github_run_id,
        run_attempt,

        started_at,
        finished_at,
        duration_seconds,

        status,
        github_run_url,

        instructions_processed,
        instruction_issues,

        events_processed,
        event_issues,

        total_offers,
        missing_collateral_decimals

    from {{
        source(
            'pipeline_monitoring',
            'run_history'
        )
    }}

),

classified as (

    select
        *,

        -- A pipeline should normally finish within
        -- its configured four-hour timeout.
        --
        -- After five hours, an unfinished record
        -- is considered potentially stale.

        case
            when status = 'running'
                 and started_at <
                     current_timestamp
                     - interval '5 hours'

            then 'stale'

            else status

        end as effective_status,


        -- Completed executions use stored duration.
        -- Running executions use elapsed wall time.

        case

            when finished_at is not null
            then duration_seconds

            when status = 'running'
            then date_diff(
                'second',
                started_at,
                current_timestamp
            )

            else null

        end as elapsed_seconds

    from history

),

final as (

    select
        *,

        cast(
            started_at at time zone 'UTC'
            as date
        ) as run_date_utc,

        round(
            elapsed_seconds / 60.0,
            2
        ) as elapsed_minutes,


        -- Percentage of instructions with
        -- unsuccessful decoding statuses.

        round(
            100.0 * instruction_issues
            / nullif(
                instructions_processed,
                0
            ),
            2
        ) as instruction_issue_pct,


        -- Percentage of events with
        -- unsuccessful decoding statuses.

        round(
            100.0 * event_issues
            / nullif(
                events_processed,
                0
            ),
            2
        ) as event_issue_pct

    from classified

)

select *
from final
