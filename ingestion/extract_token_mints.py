import json
import os
import time
from pathlib import Path
import requests

from ingestion.db import (
    get_connection,
    get_target,
)


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
    """
    Return every fungible-token mint observed by Offerbook that still needs
    mint-account metadata.

    Instruction accounts cover token/token offer instructions. Offer events
    additionally cover mixed asset offers such as Token/CoreNFT, so include
    Token-prefixed event asset keys as well.
    """

    rows = con.execute("""
        with offerbook_mints as (

            select principal_mint as mint_address
            from main.int_offerbook_offer_accounts

            union

            select collateral_mint as mint_address
            from main.int_offerbook_offer_accounts

            union

            select
                split_part(principal_asset_key, ':', 2)
                    as mint_address
            from main.int_offerbook_offer_events
            where principal_asset_type = 'Token'

            union

            select
                split_part(collateral_asset_key, ':', 2)
                    as mint_address
            from main.int_offerbook_offer_events
            where collateral_asset_type = 'Token'

        )

        select
            o.mint_address

        from offerbook_mints o

        left join raw.token_mints m
            on o.mint_address = m.mint_address

        where o.mint_address is not null
          and o.mint_address != ''

          and (
              m.mint_address is null
              or m.decimals is null
          )

        order by o.mint_address
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

    con = get_connection()

    print(
    f"Database target: {get_target()}"
    )   

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
            if decimals is None:

                print(
                    f"No decimals returned for {mint}. "
                    "Skipping this mint."
                )

                continue
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