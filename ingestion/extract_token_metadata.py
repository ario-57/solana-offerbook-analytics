import json
import os
import time
from pathlib import Path

import duckdb
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "solana.duckdb"

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")

BIRDEYE_URL = (
    "https://public-api.birdeye.so"
    "/defi/token_overview"
)

REQUEST_DELAY = 1.1
MAX_RETRIES = 8


def birdeye_request(mint_address):

    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana",
    }

    params = {
        "address": mint_address
    }

    for attempt in range(MAX_RETRIES):

        response = requests.get(
            BIRDEYE_URL,
            headers=headers,
            params=params,
            timeout=30
        )

        if response.status_code == 429:

            wait_time = min(
                2 ** attempt,
                30
            )

            print(
                f"Rate limited. "
                f"Waiting {wait_time}s..."
            )

            time.sleep(wait_time)

            continue

        response.raise_for_status()

        payload = response.json()

        if not payload.get("success"):
            return None

        return payload.get("data")

    raise RuntimeError(
        "Birdeye request failed "
        "after maximum retries."
    )


def get_offerbook_mints(con):

    rows = con.execute("""
        select mint_address
        from raw.token_mints
        where mint_address is not null
        order by mint_address
    """).fetchall()

    return [
        row[0]
        for row in rows
    ]


def main():

    if not BIRDEYE_API_KEY:

        raise RuntimeError(
            "BIRDEYE_API_KEY environment "
            "variable is not set."
        )

    con = duckdb.connect(
        str(DB_PATH)
    )

    con.execute("""
        create schema if not exists raw
    """)

    con.execute("""
        create table if not exists raw.token_metadata (
            mint_address varchar primary key,
            symbol varchar,
            name varchar,
            logo_uri varchar,
            birdeye_response json,
            fetched_at timestamptz
        )
    """)

    mints = get_offerbook_mints(con)

    print(
        f"Found {len(mints)} mints."
    )

    for index, mint in enumerate(
        mints,
        start=1
    ):

        already_exists = con.execute("""
            select 1
            from raw.token_metadata
            where mint_address = ?
        """, [mint]).fetchone()

        if already_exists:

            print(
                f"[{index}/{len(mints)}] "
                f"Already cached: {mint}"
            )

            continue

        print(
            f"[{index}/{len(mints)}] "
            f"Fetching {mint}"
        )

        try:

            data = birdeye_request(
                mint
            )

            if data is None:

                print(
                    "  No metadata returned."
                )

                time.sleep(
                    REQUEST_DELAY
                )

                continue

            symbol = data.get(
                "symbol"
            )

            name = data.get(
                "name"
            )

            logo_uri = (
                data.get("logoURI")
                or data.get("logo_uri")
                or data.get("logo")
            )

            con.execute("""
                insert into raw.token_metadata (
                    mint_address,
                    symbol,
                    name,
                    logo_uri,
                    birdeye_response,
                    fetched_at
                )
                values (
                    ?,
                    ?,
                    ?,
                    ?,
                    cast(? as json),
                    current_timestamp
                )

                on conflict (mint_address)
                do nothing
            """, [
                mint,
                symbol,
                name,
                logo_uri,
                json.dumps(data),
            ])

            print(
                f"  {symbol} | {name}"
            )

        except Exception as error:

            print(
                f"  Error: {error}"
            )

        time.sleep(
            REQUEST_DELAY
        )

    con.close()


if __name__ == "__main__":
    main()