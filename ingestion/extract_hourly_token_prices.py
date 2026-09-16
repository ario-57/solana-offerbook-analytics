import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "solana.duckdb"

BIRDEYE_API_KEY = os.getenv(
    "BIRDEYE_API_KEY"
)

BIRDEYE_URL = (
    "https://public-api.birdeye.so"
    "/defi/history_price"
)

REQUEST_DELAY = 1.25
MAX_RETRIES = 8

HOUR_SECONDS = 3600

# Keep individual requests reasonably sized.
# 4000 hours is about 166 days.
MAX_WINDOW_HOURS = 4000


def floor_to_hour(timestamp):

    unix_time = int(
        timestamp.timestamp()
    )

    return (
        unix_time
        // HOUR_SECONDS
        * HOUR_SECONDS
    )


def birdeye_request(
    mint_address,
    time_from,
    time_to
):

    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": "solana",
    }

    params = {
        "address": mint_address,
        "address_type": "token",
        "type": "1H",
        "time_from": time_from,
        "time_to": time_to,
    }

    for attempt in range(MAX_RETRIES):

        try:

            response = requests.get(
                BIRDEYE_URL,
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:
                    wait_time = float(
                        retry_after
                    )
                else:
                    wait_time = min(
                        2 ** attempt,
                        30
                    )

                print(
                    f"  Rate limited. "
                    f"Waiting {wait_time}s..."
                )

                time.sleep(wait_time)

                continue

            if response.status_code >= 500:

                wait_time = min(
                    2 ** attempt,
                    30
                )

                print(
                    f"  Birdeye server error "
                    f"{response.status_code}. "
                    f"Retrying in "
                    f"{wait_time}s..."
                )

                time.sleep(wait_time)

                continue

            response.raise_for_status()

            payload = response.json()

            if not payload.get("success"):
                return []

            data = payload.get("data") or {}

            return data.get("items") or []

        except (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = min(
                2 ** attempt,
                30
            )

            print(
                f"  Network error: {error}"
            )

            print(
                f"  Retrying in "
                f"{wait_time}s..."
            )

            time.sleep(wait_time)

    raise RuntimeError(
        "Birdeye request failed after "
        "maximum retries."
    )


def get_required_mint_ranges(con):

    rows = con.execute("""
        with token_usage as (

            ------------------------------------------------
            -- 1. LOAN ORIGINATION
            -- Principal
            ------------------------------------------------

            select
                principal_mint as mint_address,
                loan_created_at as event_at

            from main.int_offerbook_loans

            where principal_mint is not null
              and principal_asset_type = 'Token'
              and loan_created_at is not null


            union all


            ------------------------------------------------
            -- 1. LOAN ORIGINATION
            -- Collateral
            ------------------------------------------------

            select
                collateral_mint as mint_address,
                loan_created_at as event_at

            from main.int_offerbook_loans

            where collateral_mint is not null
              and collateral_asset_type = 'Token'
              and loan_created_at is not null


            union all


            ------------------------------------------------
            -- 2. REPAYMENT
            -- Principal
            ------------------------------------------------

            select
                principal_mint as mint_address,
                repaid_at as event_at

            from main.int_offerbook_loan_repayments

            where principal_mint is not null
              and principal_asset_type = 'Token'
              and repaid_at is not null


            union all


            ------------------------------------------------
            -- 2. REPAYMENT
            -- Collateral
            ------------------------------------------------

            select
                collateral_mint as mint_address,
                repaid_at as event_at

            from main.int_offerbook_loan_repayments

            where collateral_mint is not null
              and collateral_asset_type = 'Token'
              and repaid_at is not null


            union all


            ------------------------------------------------
            -- 3. DEFAULT
            -- Principal
            ------------------------------------------------

            select
                principal_mint as mint_address,
                defaulted_at as event_at

            from main.int_offerbook_loan_defaults

            where principal_mint is not null
              and principal_asset_type = 'Token'
              and defaulted_at is not null


            union all


            ------------------------------------------------
            -- 3. DEFAULT
            -- Collateral
            ------------------------------------------------

            select
                collateral_mint as mint_address,
                defaulted_at as event_at

            from main.int_offerbook_loan_defaults

            where collateral_mint is not null
              and collateral_asset_type = 'Token'
              and defaulted_at is not null

        ),

        required_ranges as (

            select
                mint_address,

                min(event_at)
                    as first_event_at,

                max(event_at)
                    as last_event_at

            from token_usage

            group by 1

        )

        select
            mint_address,

            ------------------------------------------------
            -- Add one-hour buffer before first event.
            --
            -- Our pricing rule allows the previous price
            -- within one hour.
            ------------------------------------------------

            first_event_at
                - interval '1 hour'
                as first_seen,

            ------------------------------------------------
            -- Add one-hour buffer after last event.
            --
            -- This helps ensure the API returns the entire
            -- final hourly bucket. We will still NEVER use
            -- a future price in dbt.
            ------------------------------------------------

            last_event_at
                + interval '1 hour'
                as last_seen

        from required_ranges

        order by mint_address

    """).fetchall()

    return rows


def get_cached_range(
    con,
    mint_address
):

    return con.execute("""
        select
            min(price_hour),
            max(price_hour)

        from raw.token_prices_hourly

        where mint_address = ?
    """, [
        mint_address
    ]).fetchone()


def build_windows(
    start_unix,
    end_unix
):

    max_seconds = (
        MAX_WINDOW_HOURS
        * HOUR_SECONDS
    )

    current = start_unix

    while current <= end_unix:

        window_end = min(
            current + max_seconds,
            end_unix
        )

        yield current, window_end

        current = (
            window_end
            + HOUR_SECONDS
        )


def store_items(
    con,
    mint_address,
    items
):

    stored = 0

    for item in items:

        unix_time = (
            item.get("unixTime")
            or item.get("unix_time")
        )

        price = item.get("value")

        if (
            unix_time is None
            or price is None
        ):
            continue

        # Normalize Birdeye timestamp
        # to the start of its UTC hour.
        hour_unix = (
            int(unix_time)
            // HOUR_SECONDS
            * HOUR_SECONDS
        )

        price_hour = (
            datetime.fromtimestamp(
                hour_unix,
                tz=timezone.utc
            )
        )

        con.execute("""
            insert into raw.token_prices_hourly
            (
                mint_address,
                price_hour,
                price_usd,
                price_source,
                fetched_at
            )

            values (
                ?,
                ?,
                ?,
                'birdeye_history_price_1h',
                now()
            )

            on conflict (
                mint_address,
                price_hour
            )

            do update set
                price_usd =
                    excluded.price_usd,

                price_source =
                    excluded.price_source,

                fetched_at =
                    now()
        """, [
            mint_address,
            price_hour,
            float(price)
        ])

        stored += 1

    return stored


def fetch_range(
    con,
    mint_address,
    start_unix,
    end_unix
):

    stored = 0

    for (
        window_start,
        window_end
    ) in build_windows(
        start_unix,
        end_unix
    ):

        print(
            "    "
            f"{datetime.fromtimestamp(window_start, timezone.utc)}"
            " → "
            f"{datetime.fromtimestamp(window_end, timezone.utc)}"
        )

        items = birdeye_request(
            mint_address,
            window_start,
            window_end
        )

        stored += store_items(
            con,
            mint_address,
            items
        )

        time.sleep(
            REQUEST_DELAY
        )

    return stored


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
        create table if not exists
        raw.token_prices_hourly
        (
            mint_address varchar,
            price_hour timestamptz,
            price_usd double,
            price_source varchar,
            fetched_at timestamptz,

            primary key (
                mint_address,
                price_hour
            )
        )
    """)

    mint_ranges = (
        get_required_mint_ranges(con)
    )

    print(
        f"Found {len(mint_ranges)} "
        f"Offerbook mints."
    )

    total_stored = 0

    for index, (
        mint,
        first_seen,
        last_seen
    ) in enumerate(
        mint_ranges,
        start=1
    ):

        print()
        print(
            f"[{index}/{len(mint_ranges)}] "
            f"{mint}"
        )

        required_start = (
            floor_to_hour(
                first_seen
            )
        )

        required_end = (
            floor_to_hour(
                last_seen
            )
        )

        (
            cached_min,
            cached_max
        ) = get_cached_range(
            con,
            mint
        )

        # No price history yet.
        if cached_min is None:

            stored = fetch_range(
                con,
                mint,
                required_start,
                required_end
            )

            total_stored += stored

            print(
                f"  Stored {stored} "
                f"hourly prices."
            )

            continue

        cached_start = (
            floor_to_hour(
                cached_min
            )
        )

        cached_end = (
            floor_to_hour(
                cached_max
            )
        )

        stored = 0

        # Historical backfill expanded
        # further into the past.
        if required_start < cached_start:

            stored += fetch_range(
                con,
                mint,
                required_start,
                cached_start
                - HOUR_SECONDS
            )

        # New Offerbook activity expanded
        # the range forward.
        if required_end > cached_end:

            stored += fetch_range(
                con,
                mint,
                cached_end
                + HOUR_SECONDS,
                required_end
            )

        if stored == 0:

            print(
                "  Price range already cached."
            )

        else:

            print(
                f"  Stored {stored} "
                f"new hourly prices."
            )

        total_stored += stored

    print()
    print(
        f"Stored/updated "
        f"{total_stored} hourly prices."
    )

    con.close()


if __name__ == "__main__":
    main()