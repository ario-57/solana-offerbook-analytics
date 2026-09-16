import json
import os
import time

import duckdb
import requests


PROGRAM_ID = "offerbkFMvVfpQhL8ZQ5iromnjct5rz3r52B9ewu3ie"

DB_PATH = "solana.duckdb"

RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com"
)

SIGNATURE_PAGE_SIZE = 1000

REQUEST_DELAY = 0.5

MAX_RETRIES = 8


def rpc_request(method, params):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    for attempt in range(MAX_RETRIES):

        try:
            response = requests.post(
                RPC_URL,
                json=payload,
                timeout=30
            )

            # RPC rate limit
            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:
                    wait_time = float(retry_after)
                else:
                    wait_time = min(
                        2 ** attempt,
                        30
                    )

                print(
                    f"Rate limited (429). "
                    f"Waiting {wait_time:.1f}s..."
                )

                time.sleep(wait_time)

                continue

            response.raise_for_status()

            data = response.json()

            if "error" in data:
                raise RuntimeError(
                    data["error"]
                )

            return data["result"]

        except requests.exceptions.RequestException as e:

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = min(
                2 ** attempt,
                30
            )

            print(
                f"RPC request failed: {e}"
            )

            print(
                f"Retrying in {wait_time}s..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        f"RPC request failed after "
        f"{MAX_RETRIES} attempts"
    )


def get_latest_stored_signature(con):
    row = con.execute("""
        SELECT signature
        FROM raw.offerbook_transactions
        ORDER BY slot DESC
        LIMIT 1
    """).fetchone()

    if row is None:
        return None

    return row[0]


def get_new_signatures(latest_stored_signature):
    new_signatures = []

    before = None

    while True:

        config = {
            "limit": SIGNATURE_PAGE_SIZE,
            "commitment": "finalized"
        }

        if before is not None:
            config["before"] = before

        result = rpc_request(
            "getSignaturesForAddress",
            [
                PROGRAM_ID,
                config
            ]
        )

        if not result:
            break

        for item in result:

            if item["signature"] == latest_stored_signature:
                return new_signatures

            new_signatures.append(item)

        if len(result) < SIGNATURE_PAGE_SIZE:
            break

        before = result[-1]["signature"]

        time.sleep(REQUEST_DELAY)

    return new_signatures


def get_transaction(signature):

    return rpc_request(
        "getTransaction",
        [
            signature,
            {
                "encoding": "json",
                "commitment": "finalized",
                "maxSupportedTransactionVersion": 1
            }
        ]
    )


def main():

    con = duckdb.connect(DB_PATH)

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS raw
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.offerbook_transactions (
            signature VARCHAR PRIMARY KEY,
            program_id VARCHAR NOT NULL,
            slot BIGINT,
            block_time BIGINT,
            transaction_json JSON,
            ingested_at TIMESTAMPTZ
        )
    """)

    latest_signature = get_latest_stored_signature(con)

    print("Latest stored signature:")
    print(latest_signature)
    print()

    if latest_signature is None:
        print(
            "No existing Offerbook transactions found. "
            "Use the initial/backfill loader first."
        )

        con.close()
        return

    new_signatures = get_new_signatures(
        latest_signature
    )

    print(
        f"Found {len(new_signatures)} new transactions"
    )

    if not new_signatures:
        print("Database is already up to date.")

        con.close()
        return

    # RPC returns newest -> oldest.
    # Reverse so we insert oldest -> newest.
    new_signatures.reverse()

    inserted = 0

    for item in new_signatures:

        signature = item["signature"]

        print(f"Fetching {signature}")

        transaction = get_transaction(signature)

        if transaction is None:
            print(
                f"Transaction unavailable: {signature}"
            )
            continue

        con.execute("""
            INSERT INTO raw.offerbook_transactions
            (
                signature,
                program_id,
                slot,
                block_time,
                transaction_json,
                ingested_at
            )
            VALUES (
                ?,
                ?,
                ?,
                ?,
                CAST(? AS JSON),
                current_timestamp
            )
            ON CONFLICT (signature) DO NOTHING
        """, [
            signature,
            PROGRAM_ID,
            transaction["slot"],
            transaction["blockTime"],
            json.dumps(transaction)
        ])

        inserted += 1

        time.sleep(REQUEST_DELAY)

    print()
    print(f"Inserted {inserted} new transactions.")

    total = con.execute("""
        SELECT COUNT(*)
        FROM raw.offerbook_transactions
    """).fetchone()[0]

    print(f"Total transactions stored: {total}")

    con.close()


if __name__ == "__main__":
    main()