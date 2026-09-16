import json
import os
import time
from pathlib import Path

import duckdb
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "solana.duckdb"

RPC_URL = os.getenv(
    "SOLANA_RPC_URL",
    "https://api.mainnet-beta.solana.com"
)

BATCH_SIZE = 100
MAX_RETRIES = 8


def rpc_request(method, params):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    for attempt in range(MAX_RETRIES):

        response = requests.post(
            RPC_URL,
            json=payload,
            timeout=30
        )

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
                f"Rate limited. Waiting "
                f"{wait_time:.1f}s..."
            )

            time.sleep(wait_time)

            continue

        response.raise_for_status()

        data = response.json()

        if "error" in data:
            raise RuntimeError(data["error"])

        return data["result"]

    raise RuntimeError(
        "RPC request failed after maximum retries."
    )


def get_offerbook_mints(con):

    rows = con.execute("""
        select distinct mint_address
        from (

            select
                principal_mint as mint_address
            from main.int_offerbook_offer_accounts

            union

            select
                collateral_mint as mint_address
            from main.int_offerbook_offer_accounts

        )
        where mint_address is not null
        order by mint_address
    """).fetchall()

    return [
        row[0]
        for row in rows
    ]


def chunks(values, size):

    for i in range(0, len(values), size):
        yield values[i:i + size]


def fetch_mint_accounts(mints):

    return rpc_request(
        "getMultipleAccounts",
        [
            mints,
            {
                "encoding": "jsonParsed",
                "commitment": "finalized"
            }
        ]
    )


def main():

    con = duckdb.connect(str(DB_PATH))

    con.execute("""
        create schema if not exists raw
    """)

    con.execute("""
        create table if not exists raw.token_mints (
            mint_address varchar primary key,
            token_program varchar,
            decimals integer,
            supply_raw ubigint,
            is_initialized boolean,
            account_json json,
            fetched_at timestamptz
        )
    """)

    mints = get_offerbook_mints(con)

    print(
        f"Found {len(mints)} unique Offerbook mints."
    )

    if not mints:
        con.close()
        return

    stored = 0

    for batch in chunks(mints, BATCH_SIZE):

        print(
            f"Fetching {len(batch)} mint accounts..."
        )

        result = fetch_mint_accounts(batch)

        accounts = result["value"]

        for mint, account in zip(
            batch,
            accounts
        ):

            if account is None:
                print(
                    f"Mint account not found: {mint}"
                )
                continue

            data = account.get("data", {})

            parsed = (
                data.get("parsed", {})
                if isinstance(data, dict)
                else {}
            )

            parsed_type = parsed.get("type")

            if parsed_type != "mint":
                print(
                    f"Not parsed as mint: "
                    f"{mint} ({parsed_type})"
                )
                continue

            info = parsed.get("info", {})

            decimals = info.get("decimals")
            supply = info.get("supply")
            is_initialized = info.get(
                "isInitialized"
            )

            token_program = account.get(
                "owner"
            )

            con.execute("""
                insert or replace into raw.token_mints
                (
                    mint_address,
                    token_program,
                    decimals,
                    supply_raw,
                    is_initialized,
                    account_json,
                    fetched_at
                )
                values (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    cast(? as json),
                    current_timestamp
                )
            """, [
                mint,
                token_program,
                decimals,
                int(supply)
                    if supply is not None
                    else None,
                is_initialized,
                json.dumps(account)
            ])

            stored += 1

        time.sleep(0.5)

    print()
    print(
        f"Stored metadata for {stored} mints."
    )

    con.close()


if __name__ == "__main__":
    main()