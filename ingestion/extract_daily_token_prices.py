import argparse
import os
import time
from datetime import datetime, timezone

import requests

from ingestion.db import get_connection


BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")

BIRDEYE_URL = (
    "https://public-api.birdeye.so"
    "/defi/historical_price_unix"
)

REQUEST_DELAY_SECONDS = 1.1
MAX_RETRIES = 5

# Trusted Solana stablecoin mints that we intentionally value at $1.
#
# IMPORTANT:
# Use mint addresses rather than symbols. Symbols are not unique and can
# be spoofed by unrelated tokens.
STABLECOIN_FIXED_PRICES = {
    # Native USDC on Solana
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": 1.0,

    # USDT on Solana
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": 1.0,
}


def create_tables(con):
    """
    Create the daily reference-price table and a small ingestion-state table.

    raw.token_prices_daily:
        One row per token mint per UTC date.

    pipeline.token_price_daily_fetch_state:
        Records attempted Birdeye requests so failed/empty requests are not
        immediately repeated on every pipeline execution.
    """

    con.execute("""
        create schema if not exists raw
    """)

    con.execute("""
        create schema if not exists pipeline
    """)

    con.execute("""
        create table if not exists raw.token_prices_daily (
            mint_address varchar,
            price_date date,
            reference_event_at timestamptz,
            provider_price_at timestamptz,
            price_usd double,
            price_source varchar,
            fetched_at timestamptz,

            primary key (
                mint_address,
                price_date
            )
        )
    """)

    con.execute("""
        create table if not exists
        pipeline.token_price_daily_fetch_state (
            mint_address varchar,
            price_date date,
            reference_event_at timestamptz,
            status varchar,
            attempts integer,
            last_error varchar,
            attempted_at timestamptz,

            primary key (
                mint_address,
                price_date
            )
        )
    """)


def get_required_token_days(con):
    """
    Build the set of prices needed by Offerbook loan analytics.

    We collect Token assets from:
      1. loan origination
      2. loan repayment
      3. loan default

    Multiple events using the same mint on the same UTC date collapse into
    exactly one requirement.

    The earliest event timestamp is used as that day's reference timestamp.
    """

    return con.execute("""
        with token_events as (

            ------------------------------------------------------------
            -- Loan originations: principal
            ------------------------------------------------------------

            select
                principal_mint as mint_address,
                loan_created_at as event_at

            from main.int_offerbook_loans

            where principal_asset_type = 'Token'
              and principal_mint is not null
              and loan_created_at is not null


            union all


            ------------------------------------------------------------
            -- Loan originations: collateral
            ------------------------------------------------------------

            select
                collateral_mint as mint_address,
                loan_created_at as event_at

            from main.int_offerbook_loans

            where collateral_asset_type = 'Token'
              and collateral_mint is not null
              and loan_created_at is not null


            union all


            ------------------------------------------------------------
            -- Repayments: principal
            ------------------------------------------------------------

            select
                principal_mint as mint_address,
                repaid_at as event_at

            from main.int_offerbook_loan_repayments

            where principal_asset_type = 'Token'
              and principal_mint is not null
              and repaid_at is not null


            union all


            ------------------------------------------------------------
            -- Repayments: collateral
            ------------------------------------------------------------

            select
                collateral_mint as mint_address,
                repaid_at as event_at

            from main.int_offerbook_loan_repayments

            where collateral_asset_type = 'Token'
              and collateral_mint is not null
              and repaid_at is not null


            union all


            ------------------------------------------------------------
            -- Defaults: principal
            ------------------------------------------------------------

            select
                principal_mint as mint_address,
                defaulted_at as event_at

            from main.int_offerbook_loan_defaults

            where principal_asset_type = 'Token'
              and principal_mint is not null
              and defaulted_at is not null


            union all


            ------------------------------------------------------------
            -- Defaults: collateral
            ------------------------------------------------------------

            select
                collateral_mint as mint_address,
                defaulted_at as event_at

            from main.int_offerbook_loan_defaults

            where collateral_asset_type = 'Token'
              and collateral_mint is not null
              and defaulted_at is not null
        ),

        required as (

            select
                mint_address,

                cast(
                    event_at at time zone 'UTC'
                    as date
                ) as price_date,

                min(event_at) as reference_event_at

            from token_events

            group by
                mint_address,
                cast(
                    event_at at time zone 'UTC'
                    as date
                )
        )

        select
            r.mint_address,
            r.price_date,
            r.reference_event_at,

            s.status as previous_status,
            s.attempted_at as previous_attempt_at

        from required as r

        left join raw.token_prices_daily as p
            on r.mint_address = p.mint_address
            and r.price_date = p.price_date

        left join pipeline.token_price_daily_fetch_state as s
            on r.mint_address = s.mint_address
            and r.price_date = s.price_date

        where p.mint_address is null

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
            r.price_date desc,
            r.mint_address
    """).fetchall()


def to_unix(timestamp):
    """Convert a Python datetime into Unix seconds."""

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return int(timestamp.timestamp())


def birdeye_request(mint_address, reference_event_at):
    """
    Request one historical price near one Unix timestamp.

    Unlike /defi/history_price, this endpoint returns one point rather than
    an hourly time series.
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
        "unixtime": to_unix(reference_event_at),
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

            data = payload.get("data") or {}

            price = data.get("value")

            if price is None:
                return None

            provider_unix = (
                data.get("updateUnixTime")
                or data.get("update_unix_time")
            )

            provider_price_at = None

            if provider_unix is not None:
                provider_price_at = datetime.fromtimestamp(
                    int(provider_unix),
                    tz=timezone.utc,
                )

            return {
                "price_usd": float(price),
                "provider_price_at": provider_price_at,
            }

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
        "Birdeye request failed after maximum retries."
    )


def upsert_price(
    con,
    mint_address,
    price_date,
    reference_event_at,
    provider_price_at,
    price_usd,
    price_source,
):
    """Insert or update one token/day reference price."""

    con.execute("""
        insert into raw.token_prices_daily (
            mint_address,
            price_date,
            reference_event_at,
            provider_price_at,
            price_usd,
            price_source,
            fetched_at
        )

        values (?, ?, ?, ?, ?, ?, now())

        on conflict (
            mint_address,
            price_date
        )

        do update set
            reference_event_at =
                excluded.reference_event_at,

            provider_price_at =
                excluded.provider_price_at,

            price_usd =
                excluded.price_usd,

            price_source =
                excluded.price_source,

            fetched_at =
                excluded.fetched_at
    """, [
        mint_address,
        price_date,
        reference_event_at,
        provider_price_at,
        price_usd,
        price_source,
    ])


def record_fetch_state(
    con,
    mint_address,
    price_date,
    reference_event_at,
    status,
    error=None,
):
    """Record the result of one Birdeye request attempt."""

    con.execute("""
        insert into pipeline.token_price_daily_fetch_state (
            mint_address,
            price_date,
            reference_event_at,
            status,
            attempts,
            last_error,
            attempted_at
        )

        values (?, ?, ?, ?, 1, ?, now())

        on conflict (
            mint_address,
            price_date
        )

        do update set
            reference_event_at =
                excluded.reference_event_at,

            status =
                excluded.status,

            attempts =
                pipeline.token_price_daily_fetch_state.attempts + 1,

            last_error =
                excluded.last_error,

            attempted_at =
                excluded.attempted_at
    """, [
        mint_address,
        price_date,
        reference_event_at,
        status,
        error,
    ])


def seed_stablecoin_prices(con, rows, dry_run):
    """
    Write $1 reference prices for explicitly allow-listed stablecoins.

    Stablecoin rows do not consume Birdeye API requests.
    """

    seeded = 0

    for (
        mint_address,
        price_date,
        reference_event_at,
        _previous_status,
        _previous_attempt_at,
    ) in rows:

        fixed_price = STABLECOIN_FIXED_PRICES.get(
            mint_address
        )

        if fixed_price is None:
            continue

        seeded += 1

        print(
            f"Stablecoin: {mint_address} | "
            f"{price_date} | ${fixed_price:.2f}"
        )

        if not dry_run:
            upsert_price(
                con=con,
                mint_address=mint_address,
                price_date=price_date,
                reference_event_at=reference_event_at,
                provider_price_at=None,
                price_usd=fixed_price,
                price_source="fixed_usd_stablecoin",
            )

    return seeded


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Ingest one Offerbook reference price per "
            "token per UTC day."
        )
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=20,
        help=(
            "Maximum number of Birdeye API requests. "
            "Stablecoin rows do not count toward this limit."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Show required token/day prices without "
            "calling Birdeye or inserting price rows."
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

        required_rows = get_required_token_days(con)

        print(
            f"Missing eligible token/day prices: "
            f"{len(required_rows):,}"
        )

        stablecoin_rows = seed_stablecoin_prices(
            con,
            required_rows,
            dry_run=args.dry_run,
        )

        api_rows = [
            row
            for row in required_rows
            if row[0]
            not in STABLECOIN_FIXED_PRICES
        ]

        selected_api_rows = api_rows[
            :args.max_requests
        ]

        print(
            f"Stablecoin rows requiring no API: "
            f"{stablecoin_rows:,}"
        )

        print(
            f"Non-stablecoin rows still needing API: "
            f"{len(api_rows):,}"
        )

        print(
            f"Birdeye requests selected this run: "
            f"{len(selected_api_rows):,}"
        )

        if args.dry_run:
            print("\nDry run only. No prices were written.")

            for (
                mint_address,
                price_date,
                reference_event_at,
                _previous_status,
                _previous_attempt_at,
            ) in selected_api_rows:

                print(
                    f"  API → {mint_address} | "
                    f"{price_date} | "
                    f"reference={reference_event_at}"
                )

            return

        successful = 0
        empty = 0
        failed = 0

        for index, (
            mint_address,
            price_date,
            reference_event_at,
            _previous_status,
            _previous_attempt_at,
        ) in enumerate(
            selected_api_rows,
            start=1,
        ):
            print()
            print(
                f"[{index}/{len(selected_api_rows)}] "
                f"{mint_address}"
            )
            print(
                f"  Date: {price_date}"
            )
            print(
                f"  Reference event: {reference_event_at}"
            )

            try:
                result = birdeye_request(
                    mint_address,
                    reference_event_at,
                )

                if result is None:
                    empty += 1

                    print(
                        "  No historical price returned."
                    )

                    record_fetch_state(
                        con,
                        mint_address,
                        price_date,
                        reference_event_at,
                        status="empty",
                    )

                else:
                    successful += 1

                    upsert_price(
                        con=con,
                        mint_address=mint_address,
                        price_date=price_date,
                        reference_event_at=reference_event_at,
                        provider_price_at=result[
                            "provider_price_at"
                        ],
                        price_usd=result[
                            "price_usd"
                        ],
                        price_source=(
                            "birdeye_historical_price_unix"
                        ),
                    )

                    record_fetch_state(
                        con,
                        mint_address,
                        price_date,
                        reference_event_at,
                        status="success",
                    )

                    print(
                        f"  Price: "
                        f"${result['price_usd']:,.8f}"
                    )

                    if (
                        result["provider_price_at"]
                        is not None
                    ):
                        print(
                            "  Provider timestamp: "
                            f"{result['provider_price_at']}"
                        )

            except Exception as error:
                failed += 1

                print(
                    f"  ERROR: {error}"
                )

                record_fetch_state(
                    con,
                    mint_address,
                    price_date,
                    reference_event_at,
                    status="error",
                    error=str(error)[:1000],
                )

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

        print()
        print("Daily reference-price ingestion complete.")
        print(
            f"Stablecoin rows seeded: "
            f"{stablecoin_rows:,}"
        )
        print(
            f"Birdeye requests attempted: "
            f"{len(selected_api_rows):,}"
        )
        print(
            f"Successful Birdeye prices: "
            f"{successful:,}"
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
