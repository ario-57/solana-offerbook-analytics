import json
import os
import time
from datetime import datetime, timezone

import requests

from ingestion.db import (
    get_connection,
    get_target,
)


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


# Solana allows up to 1000 signatures per
# getSignaturesForAddress request.
SIGNATURE_PAGE_SIZE = 1000


# Maximum number of NEW transactions that we will
# fetch and insert during one execution.
#
# Example:
#
# $env:INCREMENTAL_MAX_TRANSACTIONS="500"
#
MAX_TRANSACTIONS_PER_RUN = int(
    os.getenv(
        "INCREMENTAL_MAX_TRANSACTIONS",
        "500",
    )
)


# Small delay after successful RPC requests.
#
# Public RPC endpoints may rate-limit aggressive clients.
REQUEST_DELAY = float(
    os.getenv(
        "SOLANA_REQUEST_DELAY",
        "0.5",
    )
)


MAX_RETRIES = 8


# Reuse one HTTP connection instead of creating a new
# TCP/TLS connection for every RPC request.
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
            # Rate limiting
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
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
                    f"Retrying in {wait_seconds}s..."
                )

                time.sleep(wait_seconds)

                continue


            # ------------------------------------------------
            # Temporary RPC/server errors
            # ------------------------------------------------

            if response.status_code >= 500:

                wait_seconds = min(
                    2 ** attempt,
                    30,
                )

                print(
                    f"RPC server returned "
                    f"{response.status_code}. "
                    f"Retrying in {wait_seconds}s..."
                )

                time.sleep(wait_seconds)

                continue


            response.raise_for_status()

            data = response.json()


            # JSON-RPC itself can return an error even
            # when HTTP status is 200.
            if "error" in data:

                raise RuntimeError(
                    f"RPC error: {data['error']}"
                )


            time.sleep(
                REQUEST_DELAY
            )

            return data.get("result")


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
                f"Network error: {error}"
            )

            print(
                f"Retrying in {wait_seconds}s..."
            )

            time.sleep(
                wait_seconds
            )


    raise RuntimeError(
        f"RPC request failed after "
        f"{MAX_RETRIES} attempts: {method}"
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
# Current forward boundary
# ============================================================

def get_latest_transaction(con):

    return con.execute("""
        select
            signature,
            slot,
            block_time

        from raw.offerbook_transactions

        order by
            slot desc,
            block_time desc nulls last

        limit 1
    """).fetchone()


# ============================================================
# Signature retrieval
# ============================================================

def get_signature_page(
    before_signature,
    until_signature,
):

    options = {
        "limit": SIGNATURE_PAGE_SIZE,
        "commitment": "finalized",
    }


    # "before" is used for pagination.
    #
    # Page 1:
    # newest → older
    #
    # Page 2:
    # start before the final signature from page 1.
    if before_signature is not None:

        options["before"] = (
            before_signature
        )


    # "until" is our existing database boundary.
    #
    # We only care about signatures NEWER than the
    # latest transaction already stored.
    if until_signature is not None:

        options["until"] = (
            until_signature
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
# Discover missing signatures
# ============================================================

def discover_missing_signatures(
    latest_known_signature,
):

    print()
    print(
        "Discovering signatures newer than "
        "the current database boundary..."
    )

    missing = []

    before_signature = None

    page_number = 0


    while True:

        page_number += 1

        print(
            f"Fetching signature page "
            f"{page_number}..."
        )


        signatures = get_signature_page(
            before_signature,
            latest_known_signature,
        )


        if not signatures:

            break


        missing.extend(
            signatures
        )


        print(
            f"  received: {len(signatures)}"
        )

        print(
            f"  discovered total: "
            f"{len(missing)}"
        )


        # Fewer than 1000 normally means we have
        # reached the `until` boundary.
        if len(signatures) < SIGNATURE_PAGE_SIZE:

            break


        # Continue pagination from the oldest signature
        # in this page.
        before_signature = (
            signatures[-1]["signature"]
        )


    return missing


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
        transaction.get("slot"),
        transaction.get("blockTime"),
        json.dumps(transaction),
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
        "Offerbook incremental ingestion"
    )

    print(
        "-------------------------------"
    )


    # --------------------------------------------------------
    # Connect to selected database
    # --------------------------------------------------------

    con = get_connection()


    try:

        current_database = con.execute(
            "select current_database()"
        ).fetchone()[0]


        print(
            f"Database target: "
            f"{get_target()}"
        )

        print(
            f"Database: "
            f"{current_database}"
        )

        print(
            f"RPC: "
            f"{RPC_URL}"
        )

        print(
            f"Maximum transactions this run: "
            f"{MAX_TRANSACTIONS_PER_RUN}"
        )

        print()


        ensure_table(
            con
        )


        # ----------------------------------------------------
        # Find our current newest stored transaction
        # ----------------------------------------------------

        latest = get_latest_transaction(
            con
        )


        if latest is None:

            raise RuntimeError(
                "raw.offerbook_transactions "
                "is empty. This incremental "
                "loader expects an existing "
                "database boundary."
            )


        (
            latest_signature,
            latest_slot,
            latest_block_time,
        ) = latest


        print(
            "Current forward boundary:"
        )

        print(
            f"  signature: "
            f"{latest_signature}"
        )

        print(
            f"  slot: "
            f"{latest_slot}"
        )

        print(
            f"  time: "
            f"{format_timestamp(latest_block_time)}"
        )


        # ----------------------------------------------------
        # Discover the complete missing gap
        # ----------------------------------------------------

        missing_signatures = (
            discover_missing_signatures(
                latest_signature
            )
        )


        print()

        print(
            f"Missing signatures found: "
            f"{len(missing_signatures)}"
        )


        if not missing_signatures:

            print(
                "Database is already up to date."
            )

            return


        # ====================================================
        # IMPORTANT
        #
        # Solana returns:
        #
        # newest
        # ↓
        # oldest
        #
        # But incremental ingestion MUST process:
        #
        # oldest
        # ↓
        # newest
        #
        # This preserves a contiguous forward boundary.
        # ====================================================

        missing_signatures.reverse()


        # ----------------------------------------------------
        # Limit how much one run processes.
        #
        # Because we reversed the list first, [:N] selects
        # the OLDEST missing transactions, not the newest.
        # ----------------------------------------------------

        signatures_to_process = (
            missing_signatures[
                :MAX_TRANSACTIONS_PER_RUN
            ]
        )


        print(
            f"Transactions to process "
            f"this run: "
            f"{len(signatures_to_process)}"
        )

        print()


        inserted_count = 0


        # ====================================================
        # Forward ingestion loop
        # ====================================================

        for signature_info in (
            signatures_to_process
        ):

            signature = (
                signature_info["signature"]
            )


            signature_block_time = (
                signature_info.get(
                    "blockTime"
                )
            )


            print(
                f"[{inserted_count + 1}/"
                f"{len(signatures_to_process)}] "
                f"{format_timestamp(signature_block_time)} "
                f"{signature}"
            )


            try:

                transaction = get_transaction(
                    signature
                )


                insert_transaction(
                    con,
                    signature,
                    transaction,
                )


                inserted_count += 1


            except Exception as error:

                print()

                print(
                    "Incremental ingestion stopped "
                    "to preserve a continuous "
                    "forward boundary."
                )

                print(
                    f"Failed signature: "
                    f"{signature}"
                )

                print(
                    f"Error: {error}"
                )

                raise


        # ----------------------------------------------------
        # New forward boundary
        # ----------------------------------------------------

        new_latest = get_latest_transaction(
            con
        )


        print()
        print(
            f"Processed "
            f"{inserted_count} "
            f"new transactions."
        )


        if new_latest:

            (
                new_signature,
                new_slot,
                new_block_time,
            ) = new_latest


            print()

            print(
                "New forward boundary:"
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


        # ----------------------------------------------------
        # Dataset statistics
        # ----------------------------------------------------

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
            transaction_count,
            oldest_transaction,
            newest_transaction,
        ) = stats


        print()

        print(
            "Dataset:"
        )

        print(
            f"  transactions: "
            f"{transaction_count}"
        )

        print(
            f"  oldest: "
            f"{oldest_transaction}"
        )

        print(
            f"  newest: "
            f"{newest_transaction}"
        )


        remaining = (
            len(missing_signatures)
            - inserted_count
        )


        print()

        if remaining > 0:

            print(
                f"Still missing approximately "
                f"{remaining} transactions "
                f"after this run."
            )

            print(
                "Run the incremental loader "
                "again to continue forward."
            )

        else:

            print(
                "Incremental catch-up complete."
            )


    finally:

        con.close()


if __name__ == "__main__":
    main()