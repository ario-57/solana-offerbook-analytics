import argparse
import json
import os
import time

import requests

from ingestion.db import get_connection


BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")

BIRDEYE_URL = (
    "https://public-api.birdeye.so"
    "/defi/token_overview"
)

REQUEST_DELAY_SECONDS = 1.1
MAX_RETRIES = 5


def create_tables(con):
    """
    Ensure the metadata table and metadata fetch-state table exist.

    raw.token_metadata stores successfully returned metadata.

    pipeline.token_metadata_fetch_state remembers empty/error attempts so
    the production pipeline does not repeatedly spend API credits on the
    same mint every day.
    """

    con.execute("""
        create schema if not exists raw
    """)

    con.execute("""
        create schema if not exists pipeline
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

    con.execute("""
        create table if not exists pipeline.token_metadata_fetch_state (
            mint_address varchar primary key,
            status varchar,
            attempts integer,
            last_error varchar,
            attempted_at timestamptz
        )
    """)


def get_missing_metadata_mints(con):
    """
    Return token mints that still need useful metadata.

    A token is considered incomplete when:
      - it has no raw.token_metadata row, OR
      - both symbol and name are NULL.

    Empty API responses are retried only after 7 days.
    Errors are retried after 6 hours.
    """

    return con.execute("""
        select
            m.mint_address,
            md.symbol,
            md.name,
            s.status as previous_status,
            s.attempted_at as previous_attempt_at

        from raw.token_mints as m

        left join raw.token_metadata as md
            on m.mint_address = md.mint_address

        left join pipeline.token_metadata_fetch_state as s
            on m.mint_address = s.mint_address

        where m.mint_address is not null

          and (
                md.mint_address is null
                or (
                    md.symbol is null
                    and md.name is null
                )
          )

          and (
                s.mint_address is null

                or (
                    s.status = 'error'
                    and s.attempted_at
                        < current_timestamp - interval '6 hours'
                )

                or (
                    s.status = 'empty'
                    and s.attempted_at
                        < current_timestamp - interval '7 days'
                )
          )

        order by
            case
                when exists (
                    select 1
                    from main.int_offerbook_offer_lifecycle o
                    where o.lifecycle_status in ('Active', 'PartiallyFilled')
                      and (
                          (
                              o.principal_asset_type = 'Token'
                              and split_part(o.principal_asset_key, ':', 2)
                                  = m.mint_address
                          )
                          or (
                              o.collateral_asset_type = 'Token'
                              and split_part(o.collateral_asset_key, ':', 2)
                                  = m.mint_address
                          )
                      )
                )
                then 0
                else 1
            end,
            m.mint_address
    """).fetchall()


def birdeye_request(mint_address):
    """
    Fetch token metadata from Birdeye.

    Retries transient rate-limit, server and network failures.
    """

    if not BIRDEYE_API_KEY:
        raise RuntimeError(
            "BIRDEYE_API_KEY environment variable is not set."
        )

    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana",
    }

    params = {
        "address": mint_address,
    }

    for attempt in range(MAX_RETRIES):

        try:
            response = requests.get(
                BIRDEYE_URL,
                headers=headers,
                params=params,
                timeout=30,
            )

            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After"
                )

                wait_time = (
                    float(retry_after)
                    if retry_after
                    else min(2 ** attempt, 30)
                )

                print(
                    f"  Rate limited. Waiting {wait_time}s..."
                )

                time.sleep(wait_time)
                continue

            if response.status_code >= 500:
                wait_time = min(2 ** attempt, 30)

                print(
                    f"  Birdeye server error "
                    f"{response.status_code}. "
                    f"Retrying in {wait_time}s..."
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

            payload = response.json()

            if not payload.get("success"):
                return None

            return payload.get("data")

        except (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = min(2 ** attempt, 30)

            print(
                f"  Network error: {error}"
            )
            print(
                f"  Retrying in {wait_time}s..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        "Birdeye metadata request failed after maximum retries."
    )


def upsert_metadata(con, mint_address, data):
    """
    Insert or update one token's metadata.

    Upsert is important because an older row may exist with NULL fields.
    """

    symbol = data.get("symbol")
    name = data.get("name")

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
            now()
        )

        on conflict (mint_address)

        do update set
            symbol =
                excluded.symbol,

            name =
                excluded.name,

            logo_uri =
                excluded.logo_uri,

            birdeye_response =
                excluded.birdeye_response,

            fetched_at =
                excluded.fetched_at
    """, [
        mint_address,
        symbol,
        name,
        logo_uri,
        json.dumps(data),
    ])

    return symbol, name


def record_fetch_state(
    con,
    mint_address,
    status,
    error=None,
):
    """
    Record success/empty/error for one metadata request.
    """

    con.execute("""
        insert into pipeline.token_metadata_fetch_state (
            mint_address,
            status,
            attempts,
            last_error,
            attempted_at
        )

        values (?, ?, 1, ?, now())

        on conflict (mint_address)

        do update set
            status =
                excluded.status,

            attempts =
                pipeline.token_metadata_fetch_state.attempts + 1,

            last_error =
                excluded.last_error,

            attempted_at =
                excluded.attempted_at
    """, [
        mint_address,
        status,
        error,
    ])


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch missing Offerbook token metadata "
            "and store it in the selected database target."
        )
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=20,
        help=(
            "Maximum number of Birdeye metadata requests "
            "during this execution."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Show which mints need metadata without "
            "calling Birdeye or writing metadata."
        ),
    )

    args = parser.parse_args()

    if args.max_requests < 0:
        raise ValueError(
            "--max-requests must be zero or greater."
        )

    con = get_connection()

    try:
        create_tables(con)

        rows = get_missing_metadata_mints(con)

        selected = rows[
            :args.max_requests
        ]

        print(
            f"Missing/incomplete metadata mints: "
            f"{len(rows):,}"
        )

        print(
            f"Birdeye requests selected this run: "
            f"{len(selected):,}"
        )

        if args.dry_run:
            print(
                "\nDry run only. "
                "No API requests or metadata writes."
            )

            for (
                mint_address,
                symbol,
                name,
                previous_status,
                previous_attempt_at,
            ) in selected:

                print(
                    f"  {mint_address} | "
                    f"symbol={symbol!r} | "
                    f"name={name!r} | "
                    f"previous_status={previous_status!r}"
                )

            return

        successful = 0
        empty = 0
        failed = 0

        for index, (
            mint_address,
            _symbol,
            _name,
            _previous_status,
            _previous_attempt_at,
        ) in enumerate(
            selected,
            start=1,
        ):
            print()
            print(
                f"[{index}/{len(selected)}] "
                f"Fetching {mint_address}"
            )

            try:
                data = birdeye_request(
                    mint_address
                )

                if not data:
                    empty += 1

                    print(
                        "  No metadata returned."
                    )

                    record_fetch_state(
                        con,
                        mint_address,
                        status="empty",
                    )

                else:
                    symbol, name = upsert_metadata(
                        con,
                        mint_address,
                        data,
                    )

                    successful += 1

                    record_fetch_state(
                        con,
                        mint_address,
                        status="success",
                    )

                    print(
                        f"  {symbol} | {name}"
                    )

            except Exception as error:
                failed += 1

                print(
                    f"  ERROR: {error}"
                )

                record_fetch_state(
                    con,
                    mint_address,
                    status="error",
                    error=str(error)[:1000],
                )

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

        print()
        print("Metadata ingestion complete.")
        print(
            f"Successful metadata rows: {successful:,}"
        )
        print(
            f"Empty responses: {empty:,}"
        )
        print(
            f"Failed requests: {failed:,}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    main()
