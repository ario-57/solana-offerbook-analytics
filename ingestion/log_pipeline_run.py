
import argparse
import os

from ingestion.db import get_connection


def required_env(name):
    """Read a required environment variable."""
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Missing environment variable: {name}"
        )

    return value


def create_history_table(con):
    """Create the monitoring table if it doesn't exist."""

    con.execute("""
        create schema if not exists pipeline
    """)

    con.execute("""
        create table if not exists pipeline.run_history (
            github_run_id bigint,
            run_attempt integer,

            started_at timestamptz,
            finished_at timestamptz,
            duration_seconds bigint,

            status varchar,
            github_run_url varchar,

            instructions_processed bigint,
            instruction_issues bigint,

            events_processed bigint,
            event_issues bigint,

            total_offers bigint,
            missing_collateral_decimals bigint,

            primary key (
                github_run_id,
                run_attempt
            )
        )
    """)


def get_run_info():
    """Identify the current GitHub Actions execution."""

    run_id = int(required_env("GITHUB_RUN_ID"))

    attempt = int(
        required_env("GITHUB_RUN_ATTEMPT")
    )

    repository = required_env("GITHUB_REPOSITORY")

    started_at = required_env("PIPELINE_STARTED_AT")

    run_url = (
        f"https://github.com/{repository}"
        f"/actions/runs/{run_id}"
    )

    return run_id, attempt, started_at, run_url


def record_start(con, run_id, attempt, started_at, run_url):
    """Insert a running record."""

    con.execute("""
        insert into pipeline.run_history (
            github_run_id,
            run_attempt,
            started_at,
            status,
            github_run_url
        )
        values (
            ?,
            ?,
            cast(? as timestamptz),
            'running',
            ?
        )

        on conflict (
            github_run_id,
            run_attempt
        )
        do nothing

    """, [
        run_id,
        attempt,
        started_at,
        run_url,
    ])

    print(
        f"Recorded pipeline start: "
        f"run={run_id}, attempt={attempt}"
    )


def get_decoder_metrics(con, table, started_at):
    """Count records written or updated during this run."""

    return con.execute(f"""
        select
            count(*) as processed,

            count(*) filter (
                where decode_status not in (
                    'ok',
                    'ok_with_remaining_bytes'
                )
            ) as issues

        from {table}

        where decoded_at >= cast(? as timestamptz)

    """, [started_at]).fetchone()


def record_finish(con, run_id, attempt, started_at):
    """Update the existing record after ETL execution."""

    status = required_env(
        "PIPELINE_JOB_STATUS"
    ).lower()

    if status not in {
        "success",
        "failure",
        "cancelled",
    }:
        raise ValueError(
            f"Unexpected job status: {status}"
        )

    # Ensure the row exists, even if an earlier
    # logging attempt did not complete.

    repository = required_env("GITHUB_REPOSITORY")

    run_url = (
        f"https://github.com/{repository}"
        f"/actions/runs/{run_id}"
    )

    record_start(
        con,
        run_id,
        attempt,
        started_at,
        run_url,
    )

    instructions, instruction_issues = (
        get_decoder_metrics(
            con,
            "decoded.offerbook_instructions",
            started_at,
        )
    )

    events, event_issues = get_decoder_metrics(
        con,
        "decoded.offerbook_events",
        started_at,
    )

    # Only report enrichment as a completed snapshot
    # when the complete ETL job has succeeded.

    total_offers = None
    missing_decimals = None

    if status == "success":

        total_offers, missing_decimals = con.execute("""
            select
                count(*),

                count(*) filter (
                    where collateral_decimals is null
                )

            from main.int_offerbook_offer_creations_enriched

        """).fetchone()

    con.execute("""
        update pipeline.run_history

        set
            finished_at = current_timestamp,

            duration_seconds = date_diff(
                'second',
                started_at,
                current_timestamp
            ),

            status = ?,

            instructions_processed = ?,
            instruction_issues = ?,

            events_processed = ?,
            event_issues = ?,

            total_offers = ?,
            missing_collateral_decimals = ?

        where github_run_id = ?
          and run_attempt = ?

    """, [
        status,
        instructions,
        instruction_issues,
        events,
        event_issues,
        total_offers,
        missing_decimals,
        run_id,
        attempt,
    ])

    print("\nPipeline execution recorded.")
    print(f"Status: {status}")
    print(f"Instructions processed: {instructions:,}")
    print(f"Instruction issues: {instruction_issues:,}")
    print(f"Events processed: {events:,}")
    print(f"Event issues: {event_issues:,}")


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "phase",
        choices=["start", "finish"],
    )

    args = parser.parse_args()

    run_id, attempt, started_at, run_url = (
        get_run_info()
    )

    con = get_connection()

    try:
        create_history_table(con)

        if args.phase == "start":

            record_start(
                con,
                run_id,
                attempt,
                started_at,
                run_url,
            )

        else:

            record_finish(
                con,
                run_id,
                attempt,
                started_at,
            )

    finally:
        con.close()


if __name__ == "__main__":
    main()
