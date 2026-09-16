import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests


# ============================================================
# Configuration
# ============================================================

PROGRAM_ID = (
    "offerbkFMvVfpQhL8ZQ5iromnjct5rz3r52B9ewu3ie"
)

RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "solana.duckdb"


# Maximum number of signatures returned by
# getSignaturesForAddress in one RPC request.
SIGNATURE_PAGE_SIZE = 1000


# Maximum number of historical transactions
# processed during ONE execution.
#
# Example:
# $env:BACKFILL_MAX_TRANSACTIONS="5000"
#
# This is NOT the historical cutoff.
# It only limits how much work one run performs.
MAX_TRANSACTIONS_PER_RUN = int(
    os.getenv(
        "BACKFILL_MAX_TRANSACTIONS",
        "200",
    )
)


# Minimum date we are willing to backfill to.
#
# The script will NEVER intentionally insert
# transactions older than this date.
#
# You can override it:
#
# $env:BACKFILL_MIN_DATE="2026-03-01"
#
BACKFILL_MIN_DATE = os.getenv(
    "BACKFILL_MIN_DATE",
    "2026-01-01",
)


# Convert:
#
# 2026-01-01
#
# into Unix timestamp at:
#
# 2026-01-01 00:00:00 UTC
#
BACKFILL_MIN_TIMESTAMP = int(
    datetime.strptime(
        BACKFILL_MIN_DATE,
        "%Y-%m-%d",
    )
    .replace(
        tzinfo=timezone.utc
    )
    .timestamp()
)


# Normal delay between successful RPC requests.
#
# Useful for reducing the chance of 429 errors.
REQUEST_DELAY = float(
    os.getenv(
        "SOLANA_REQUEST_DELAY",
        "0.5",
    )
)


MAX_RETRIES = 8


# Reuse the same HTTP connection.
session = requests.Session()


# ============================================================
# RPC helper
# ============================================================

def rpc_call(method, params):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    for attempt in range(MAX_RETRIES):

        try:

            response = session.post(
                RPC_URL,
                json=payload,
                timeout=60,
            )

            # ------------------------------------------------
            # Rate limit
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                if retry_after:

                    try:
                        wait_seconds = float(
                            retry_after
                        )

                    except ValueError:
                        wait_seconds = min(
                            2 ** attempt,
                            30,
                        )

                else:

                    wait_seconds = min(
                        2 ** attempt,
                        30,
                    )

                print(
                    f"Rate limited. "
                    f"Retrying in "
                    f"{wait_seconds}s..."
                )

                time.sleep(
                    wait_seconds
                )

                continue


            # ------------------------------------------------
            # RPC/server error
            # ------------------------------------------------

            if response.status_code >= 500:

                wait_seconds = min(
                    2 ** attempt,
                    30,
                )

                print(
                    f"RPC server returned "
                    f"{response.status_code}. "
                    f"Retrying in "
                    f"{wait_seconds}s..."
                )

                time.sleep(
                    wait_seconds
                )

                continue


            response.raise_for_status()

            data = response.json()


            # ------------------------------------------------
            # JSON-RPC error
            # ------------------------------------------------

            if "error" in data:

                raise RuntimeError(
                    f"RPC error: "
                    f"{data['error']}"
                )


            # Normal rate-control delay.
            time.sleep(
                REQUEST_DELAY
            )

            return data.get(
                "result"
            )


        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
        ) as error:

            wait_seconds = min(
                2 ** attempt,
                30,
            )

            print(
                f"Network error: "
                f"{error}"
            )

            print(
                f"Retrying in "
                f"{wait_seconds}s..."
            )

            time.sleep(
                wait_seconds
            )


    raise RuntimeError(
        f"RPC request failed after "
        f"{MAX_RETRIES} attempts: "
        f"{method}"
    )


# ============================================================
# Database setup
# ============================================================

def ensure_table(con):

    con.execute("""
        create schema if not exists raw
    """)

    con.execute("""
        create table if not exists
        raw.offerbook_transactions
        (
            signature varchar primary key,
            program_id varchar not null,
            slot bigint,
            block_time bigint,
            transaction_json json,
            ingested_at timestamptz
        )
    """)


# ============================================================
# Historical boundary
# ============================================================

def get_earliest_transaction(con):

    return con.execute("""
        select
            signature,
            slot,
            block_time

        from raw.offerbook_transactions

        order by
            slot asc,
            block_time asc nulls last

        limit 1
    """).fetchone()


# ============================================================
# Solana signature retrieval
# ============================================================

def get_signature_page(
    before_signature,
    limit,
):

    options = {
        "limit": limit,
        "commitment": "finalized",
    }


    # "before" means:
    #
    # return signatures older than
    # this signature.
    if before_signature is not None:

        options["before"] = (
            before_signature
        )


    result = rpc_call(
        "getSignaturesForAddress",
        [
            PROGRAM_ID,
            options,
        ],
    )

    return result or []


# ============================================================
# Transaction retrieval
# ============================================================

def get_transaction(signature):

    result = rpc_call(
        "getTransaction",
        [
            signature,
            {
                "encoding": "json",
                "commitment": "finalized",
                "maxSupportedTransactionVersion": 1,
            },
        ],
    )


    # We intentionally stop if a transaction
    # cannot be retrieved.
    #
    # We do NOT silently skip it because that
    # could create a gap in historical data.
    if result is None:

        raise RuntimeError(
            f"getTransaction returned "
            f"null for {signature}"
        )


    return result


# ============================================================
# Insert transaction
# ============================================================

def insert_transaction(
    con,
    signature,
    transaction,
):

    con.execute("""
        insert into
        raw.offerbook_transactions
        (
            signature,
            program_id,
            slot,
            block_time,
            transaction_json,
            ingested_at
        )

        values (
            ?,
            ?,
            ?,
            ?,
            cast(? as json),
            now()
        )

        on conflict(signature)
        do nothing
    """, [
        signature,
        PROGRAM_ID,
        transaction.get(
            "slot"
        ),
        transaction.get(
            "blockTime"
        ),
        json.dumps(
            transaction
        ),
    ])


# ============================================================
# Timestamp formatting
# ============================================================

def format_timestamp(block_time):

    if block_time is None:

        return "unknown"


    return datetime.fromtimestamp(
        block_time,
        tz=timezone.utc,
    ).isoformat()


# ============================================================
# Main
# ============================================================

def main():

    print()
    print(
        "Offerbook historical backfill"
    )
    print(
        "-----------------------------"
    )

    print(
        f"RPC: {RPC_URL}"
    )

    print(
        f"Maximum transactions this run: "
        f"{MAX_TRANSACTIONS_PER_RUN}"
    )

    print(
        f"Minimum historical date: "
        f"{BACKFILL_MIN_DATE} UTC"
    )

    print()


    # --------------------------------------------------------
    # Connect to DuckDB
    # --------------------------------------------------------

    con = duckdb.connect(
        str(DB_PATH)
    )

    ensure_table(
        con
    )


    # --------------------------------------------------------
    # Find where our existing history begins
    # --------------------------------------------------------

    earliest = get_earliest_transaction(
        con
    )


    if earliest is None:

        con.close()

        raise RuntimeError(
            "raw.offerbook_transactions "
            "is empty. Run the initial/"
            "incremental ingestion first."
        )


    (
        earliest_signature,
        earliest_slot,
        earliest_block_time,
    ) = earliest


    print(
        "Current historical boundary:"
    )

    print(
        f"  signature: "
        f"{earliest_signature}"
    )

    print(
        f"  slot: "
        f"{earliest_slot}"
    )

    print(
        f"  time: "
        f"{format_timestamp(earliest_block_time)}"
    )

    print()


    # --------------------------------------------------------
    # Check whether we have already reached the target
    # --------------------------------------------------------

    if (
        earliest_block_time is not None
        and earliest_block_time
        < BACKFILL_MIN_TIMESTAMP
    ):

        print(
            "Historical boundary is already "
            "older than the configured "
            "minimum date."
        )

        print(
            "No additional backfill required."
        )

        con.close()

        return


    # --------------------------------------------------------
    # Start immediately before our oldest stored signature
    # --------------------------------------------------------

    before_signature = (
        earliest_signature
    )


    inserted_count = 0

    reached_min_date = False

    reached_program_start = False


    # ========================================================
    # Backfill loop
    # ========================================================

    while (
        inserted_count
        < MAX_TRANSACTIONS_PER_RUN
        and not reached_min_date
        and not reached_program_start
    ):

        remaining = (
            MAX_TRANSACTIONS_PER_RUN
            - inserted_count
        )


        page_limit = min(
            SIGNATURE_PAGE_SIZE,
            remaining,
        )


        print(
            f"Fetching up to "
            f"{page_limit} signatures "
            f"before current boundary..."
        )


        signatures = (
            get_signature_page(
                before_signature,
                page_limit,
            )
        )


        # ----------------------------------------------------
        # No signatures means we reached the beginning
        # ----------------------------------------------------

        if not signatures:

            print()

            print(
                "No older signatures found."
            )

            print(
                "Historical backfill has "
                "reached the beginning of "
                "Offerbook activity."
            )

            reached_program_start = True

            break


        # ----------------------------------------------------
        # RPC returns signatures:
        #
        # newest
        # ↓
        # oldest
        #
        # We process in that same order.
        #
        # This is important for resumability.
        # ----------------------------------------------------

        for signature_info in signatures:

            if (
                inserted_count
                >= MAX_TRANSACTIONS_PER_RUN
            ):
                break


            signature = (
                signature_info[
                    "signature"
                ]
            )


            signature_block_time = (
                signature_info.get(
                    "blockTime"
                )
            )


            # ------------------------------------------------
            # Historical cutoff
            # ------------------------------------------------
            #
            # getSignaturesForAddress already gives us
            # blockTime.
            #
            # So we can stop BEFORE making the more
            # expensive getTransaction request.
            #
            # Transactions before this timestamp are
            # intentionally excluded.
            # ------------------------------------------------

            if (
                signature_block_time
                is not None
                and signature_block_time
                < BACKFILL_MIN_TIMESTAMP
            ):

                print()

                print(
                    "Reached minimum "
                    "historical date."
                )

                print(
                    f"Configured cutoff: "
                    f"{BACKFILL_MIN_DATE} "
                    f"00:00:00 UTC"
                )

                print(
                    f"Next transaction: "
                    f"{format_timestamp(signature_block_time)}"
                )

                reached_min_date = True

                break


            # ------------------------------------------------
            # Fetch historical transaction
            # ------------------------------------------------

            print(
                f"[{inserted_count + 1}/"
                f"{MAX_TRANSACTIONS_PER_RUN}] "
                f"{signature}"
            )


            try:

                transaction = (
                    get_transaction(
                        signature
                    )
                )


                # Additional safety check.
                #
                # Normally this should agree with the
                # signature's blockTime.
                transaction_block_time = (
                    transaction.get(
                        "blockTime"
                    )
                )


                if (
                    transaction_block_time
                    is not None
                    and transaction_block_time
                    < BACKFILL_MIN_TIMESTAMP
                ):

                    print()

                    print(
                        "Transaction is older "
                        "than minimum historical "
                        "date."
                    )

                    print(
                        f"Transaction time: "
                        f"{format_timestamp(transaction_block_time)}"
                    )

                    reached_min_date = True

                    break


                # ------------------------------------------------
                # Insert only after successful retrieval
                # ------------------------------------------------

                insert_transaction(
                    con,
                    signature,
                    transaction,
                )


                inserted_count += 1


                # ------------------------------------------------
                # Advance our in-memory checkpoint
                #
                # Only advance AFTER the transaction
                # was successfully fetched + inserted.
                # ------------------------------------------------

                before_signature = (
                    signature
                )


            except Exception as error:

                print()

                print(
                    "Backfill stopped to "
                    "avoid creating a "
                    "historical gap."
                )

                print(
                    f"Failed signature: "
                    f"{signature}"
                )

                print(
                    f"Error: {error}"
                )


                con.close()

                raise


        # ----------------------------------------------------
        # If we reached the cutoff, don't request
        # another signature page.
        # ----------------------------------------------------

        if reached_min_date:

            break


        # ----------------------------------------------------
        # Fewer signatures than requested usually means
        # we reached the beginning of the program's history.
        # ----------------------------------------------------

        if (
            len(signatures)
            < page_limit
        ):

            print()

            print(
                "RPC returned fewer "
                "signatures than requested."
            )

            print(
                "Likely reached the beginning "
                "of Offerbook history."
            )

            reached_program_start = True

            break


    # ========================================================
    # Final summary
    # ========================================================

    print()

    print(
        f"Processed "
        f"{inserted_count} "
        f"historical transactions."
    )


    # --------------------------------------------------------
    # New historical boundary
    # --------------------------------------------------------

    new_earliest = (
        get_earliest_transaction(
            con
        )
    )


    if new_earliest:

        (
            new_signature,
            new_slot,
            new_block_time,
        ) = new_earliest


        print()

        print(
            "New historical boundary:"
        )

        print(
            f"  signature: "
            f"{new_signature}"
        )

        print(
            f"  slot: "
            f"{new_slot}"
        )

        print(
            f"  time: "
            f"{format_timestamp(new_block_time)}"
        )


    # --------------------------------------------------------
    # Dataset statistics
    # --------------------------------------------------------

    stats = con.execute("""
        select

            count(*) as transaction_count,

            min(
                to_timestamp(block_time)
            ) as oldest_transaction,

            max(
                to_timestamp(block_time)
            ) as newest_transaction

        from raw.offerbook_transactions
    """).fetchone()


    (
        total_rows,
        oldest_transaction,
        newest_transaction,
    ) = stats


    print()

    print(
        "Dataset:"
    )

    print(
        f"  transactions: "
        f"{total_rows}"
    )

    print(
        f"  oldest: "
        f"{oldest_transaction}"
    )

    print(
        f"  newest: "
        f"{newest_transaction}"
    )


    # --------------------------------------------------------
    # Completion reason
    # --------------------------------------------------------

    print()

    if reached_min_date:

        print(
            "Backfill status: "
            "minimum historical date reached."
        )

    elif reached_program_start:

        print(
            "Backfill status: "
            "beginning of program history reached."
        )

    elif (
        inserted_count
        >= MAX_TRANSACTIONS_PER_RUN
    ):

        print(
            "Backfill status: "
            "per-run transaction limit reached."
        )

        print(
            "Run the script again to "
            "continue further backward."
        )


    print()

    con.close()


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":

    main()