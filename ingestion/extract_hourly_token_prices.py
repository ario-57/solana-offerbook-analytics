"""Incrementally backfill the hourly prices needed by Offerbook analytics.

Run from the project root, for example:
    python -u -m ingestion.extract_hourly_token_prices --dry-run
    python -u -m ingestion.extract_hourly_token_prices --max-requests 40

Set DB_TARGET=prod, MOTHERDUCK_TOKEN, and BIRDEYE_API_KEY securely first.
"""

import argparse
import math
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from ingestion.db import get_connection, get_target


API_URL = "https://public-api.birdeye.so/defi/history_price"
HOUR_SECONDS = 3600
DAY_SECONDS = 24 * HOUR_SECONDS
MAX_RETRIES = 5
REQUEST_DELAY_SECONDS = 1.25


def create_tables(con):
    """Prepare durable prices and per-mint/day API attempt history."""
    con.execute("create schema if not exists raw")
    con.execute("create schema if not exists pipeline")
    con.execute("""
        create table if not exists raw.token_prices_hourly (
            mint_address varchar,
            price_hour timestamptz,
            price_usd double,
            price_source varchar,
            fetched_at timestamptz,
            primary key (mint_address, price_hour)
        )
    """)
    con.execute("""
        create table if not exists pipeline.token_price_fetch_state (
            mint_address varchar,
            price_day date,
            attempted_at timestamptz,
            status varchar,
            api_points integer,
            stored_points integer,
            primary key (mint_address, price_day)
        )
    """)


def get_missing_days(con, max_requests, ignore_cooldown=False):
    """Find UTC days containing unpriced token event-hours (including gaps).

    Cover offer creations, loan originations, repayments, and defaults.  Do
    not request unfinished current-hour bars.  A successfully queried day
    is retried after seven days; recent hours are eligible after 20 hours.
    """
    rows = con.execute("""
        with token_events as (
            select principal_mint as mint_address, block_timestamp as event_at
            from main.int_offerbook_offer_creations
            where is_success = true
            union all
            select collateral_mint, block_timestamp
            from main.int_offerbook_offer_creations
            where is_success = true
            union all
            select principal_mint, loan_created_at
            from main.int_offerbook_loans
            where principal_asset_type = 'Token'
            union all
            select collateral_mint, loan_created_at
            from main.int_offerbook_loans
            where collateral_asset_type = 'Token'
            union all
            select principal_mint, repaid_at
            from main.int_offerbook_loan_repayments
            where principal_asset_type = 'Token'
            union all
            select collateral_mint, repaid_at
            from main.int_offerbook_loan_repayments
            where collateral_asset_type = 'Token'
            union all
            select principal_mint, defaulted_at
            from main.int_offerbook_loan_defaults
            where principal_asset_type = 'Token'
            union all
            select collateral_mint, defaulted_at
            from main.int_offerbook_loan_defaults
            where collateral_asset_type = 'Token'
        ),
        required_hours as (
            select distinct
                mint_address,
                date_trunc('hour', event_at) as price_hour
            from token_events
            where mint_address is not null
              and event_at is not null
              and event_at < date_trunc('hour', current_timestamp)
        ),
        missing_hours as (
            select
                r.mint_address,
                r.price_hour,
                cast(r.price_hour as date) as price_day
            from required_hours r
            left join raw.token_prices_hourly p
                on p.mint_address = r.mint_address
               and p.price_hour = r.price_hour
               and p.price_usd > 0
            where p.mint_address is null
        )
        select
            m.mint_address,
            m.price_day,
            count(*) as missing_event_hours
        from missing_hours m
        left join pipeline.token_price_fetch_state s
            on s.mint_address = m.mint_address
           and s.price_day = m.price_day
        where ?
           or s.attempted_at is null
           or s.attempted_at < current_timestamp - interval '7 days'
           or (
               m.price_hour >= current_timestamp - interval '48 hours'
               and s.attempted_at < current_timestamp - interval '20 hours'
           )
        group by m.mint_address, m.price_day
        order by m.price_day desc, missing_event_hours desc, m.mint_address
        limit ?
    """, [ignore_cooldown, max_requests + 1]).fetchall()
    return rows


def birdeye_request(session, api_key, mint, start_unix, end_unix):
    """Fetch one UTC day's 1H prices; never hide a rejected API response."""
    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": "solana",
    }
    params = {
        "address": mint,
        "address_type": "token",
        "type": "1H",
        "time_from": start_unix,
        "time_to": end_unix,
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=30,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError("Birdeye network retries exhausted") from exc
            delay = min(2 ** attempt, 30)
            print(f"  Network error; retrying in {delay}s")
            time.sleep(delay)
            continue

        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES - 1:
                response.raise_for_status()
                raise RuntimeError("Birdeye retries exhausted")
            try:
                delay = float(response.headers.get("Retry-After", ""))
            except (TypeError, ValueError):
                delay = min(2 ** attempt, 30)
            delay = max(1.0, min(delay, 120.0))
            print(f"  HTTP {response.status_code}; retrying in {delay:g}s")
            time.sleep(delay)
            continue

        # 400 / 401 / 403 / other client errors must stop the job; they
        # are not proof that a token has no historical price.
        response.raise_for_status()
        payload = response.json()
        if payload.get("success") is not True:
            raise RuntimeError(
                f"Birdeye rejected {mint}: {payload.get('message', payload)}"
            )
        data = payload.get("data") or {}
        items = data.get("items") or []
        if not isinstance(items, list):
            raise RuntimeError("Unexpected Birdeye history_price response")
        return items

    raise RuntimeError("Birdeye request failed after maximum retries")


def store_items(con, mint, items, start_unix, end_unix):
    """Validate and bulk-upsert 1H price observations into MotherDuck."""
    rows = []
    for item in items:
        stamp = item.get("unixTime", item.get("unix_time"))
        value = item.get("value")
        if stamp is None or value is None:
            continue
        try:
            stamp = int(stamp)
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if not (math.isfinite(value) and value > 0):
            continue
        if not (start_unix <= stamp <= end_unix):
            continue
        price_hour = datetime.fromtimestamp(
            stamp // HOUR_SECONDS * HOUR_SECONDS,
            tz=timezone.utc,
        )
        rows.append((mint, price_hour, value))

    if not rows:
        return 0

    batch = pd.DataFrame(
        rows, columns=["mint_address", "price_hour", "price_usd"]
    )
    # Avoid duplicate keys if an API response repeats an hourly item.
    batch = batch.drop_duplicates(
        subset=["mint_address", "price_hour"], keep="last"
    )
    con.register("birdeye_price_batch", batch)
    try:
        con.execute("""
            insert into raw.token_prices_hourly (
                mint_address, price_hour, price_usd, price_source, fetched_at
            )
            select
                mint_address, price_hour, price_usd,
                'birdeye_history_price_1h', current_timestamp
            from birdeye_price_batch
            where true
            on conflict (mint_address, price_hour)
            do update set
                price_usd = excluded.price_usd,
                price_source = excluded.price_source,
                fetched_at = excluded.fetched_at
        """)
    finally:
        con.unregister("birdeye_price_batch")
    return len(batch)


def save_fetch_state(con, mint, day, api_points, stored_points):
    """Remember attempted days, including valid empty API responses."""
    con.execute("""
        insert into pipeline.token_price_fetch_state (
            mint_address, price_day, attempted_at,
            status, api_points, stored_points
        )
        values (?, ?, current_timestamp, ?, ?, ?)
        on conflict (mint_address, price_day)
        do update set
            attempted_at = excluded.attempted_at,
            status = excluded.status,
            api_points = excluded.api_points,
            stored_points = excluded.stored_points
    """, [
        mint, day, "stored" if stored_points else "empty",
        api_points, stored_points,
    ])


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing Offerbook token-hour prices in MotherDuck"
    )
    parser.add_argument(
        "--max-requests", type=int,
        default=int(os.getenv("PRICE_MAX_REQUESTS", "40")),
        help="Maximum Birdeye day-windows per execution (default: 40)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show selected mint/day windows without calling Birdeye",
    )
    parser.add_argument(
        "--ignore-cooldown", action="store_true",
        help="Retry previously attempted days (use sparingly)",
    )
    args = parser.parse_args()
    if args.max_requests < 1:
        parser.error("--max-requests must be positive")

    api_key = os.getenv("BIRDEYE_API_KEY")
    if not args.dry_run and not api_key:
        raise RuntimeError("BIRDEYE_API_KEY must be set for real ingestion")

    print(f"Database target: {get_target()}")
    con = get_connection()
    session = requests.Session()
    try:
        con.execute("SET TimeZone = 'UTC'")
        create_tables(con)
        candidates = get_missing_days(
            con, args.max_requests, args.ignore_cooldown
        )
        overflow = len(candidates) > args.max_requests
        selected = candidates[:args.max_requests]
        print(f"Selected {len(selected)} mint/day price windows")
        if overflow:
            print("Additional missing windows remain for a later execution")

        total_prices = 0
        for index, (mint, day, missing_hours) in enumerate(selected, 1):
            day_start = datetime.combine(
                day, datetime.min.time(), tzinfo=timezone.utc
            )
            start_unix = int(day_start.timestamp())
            # Exclude the unfinished current UTC hour (and future hours).
            last_complete_hour_end = (
                int(time.time()) // HOUR_SECONDS * HOUR_SECONDS - 1
            )
            end_unix = min(
                start_unix + DAY_SECONDS - 1, last_complete_hour_end
            )
            print(
                f"[{index}/{len(selected)}] {day} {mint} "
                f"({missing_hours} missing event-hours)"
            )
            if args.dry_run:
                continue

            # Request outside the transaction: never lock database rows
            # while waiting on an external HTTP service.
            items = birdeye_request(
                session, api_key, mint, start_unix, end_unix
            )
            con.execute("begin transaction")
            try:
                stored = store_items(
                    con, mint, items, start_unix, end_unix
                )
                save_fetch_state(con, mint, day, len(items), stored)
                con.execute("commit")
            except Exception:
                con.execute("rollback")
                raise
            total_prices += stored
            print(f"  API points: {len(items)} | Stored/updated: {stored}")
            time.sleep(REQUEST_DELAY_SECONDS)

        if args.dry_run:
            print("Dry run complete: no price API calls or inserts performed")
        else:
            print(f"Completed. Stored/updated {total_prices} hourly price rows")
    finally:
        session.close()
        con.close()


if __name__ == "__main__":
    main()
