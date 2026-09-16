import hashlib
import json
from pathlib import Path

import base58
import duckdb

from decode_offerbook_instructions import (
    BorshReader,
    build_type_map,
    decode_defined_type,
    load_idl,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DB_PATH = PROJECT_ROOT / "solana.duckdb"

IDL_PATH = (
    PROJECT_ROOT
    / "idl"
    / "offerbook.json"
)

SOURCE_MODEL = (
    "main.int_offerbook_inner_instructions"
)

BUILD_TABLE = (
    "decoded.offerbook_events_build"
)

FINAL_TABLE = (
    "decoded.offerbook_events"
)


ANCHOR_EVENT_TAG = bytes([
    0xE4,
    0x45,
    0xA5,
    0x2E,
    0x51,
    0xCB,
    0x9A,
    0x1D,
])


def get_idl_sha256(path):

    with path.open("rb") as file:
        return hashlib.sha256(
            file.read()
        ).hexdigest()


def build_event_map(idl):

    events = {}

    for event in idl.get(
        "events",
        []
    ):

        discriminator = bytes(
            event["discriminator"]
        )

        events[discriminator] = (
            event["name"]
        )

    return events


def decode_event(
    instruction_data,
    event_map,
    type_map,
):

    raw = base58.b58decode(
        instruction_data
    )

    if len(raw) < 16:
        return None

    # Anchor emit_cpi event prefix
    if raw[:8] != ANCHOR_EVENT_TAG:
        return None

    event_discriminator = (
        raw[8:16]
    )

    event_name = event_map.get(
        event_discriminator
    )

    if event_name is None:

        return {
            "event_name": None,
            "event_discriminator":
                event_discriminator.hex(),

            "decoded_event": None,

            "remaining_bytes_hex":
                raw[16:].hex(),

            "decode_status":
                "unknown_event",

            "decode_error": None,
        }

    if event_name not in type_map:

        return {
            "event_name":
                event_name,

            "event_discriminator":
                event_discriminator.hex(),

            "decoded_event": None,

            "remaining_bytes_hex":
                raw[16:].hex(),

            "decode_status":
                "missing_event_type",

            "decode_error":
                (
                    f"{event_name} "
                    f"not found in IDL types"
                ),
        }

    try:

        reader = BorshReader(
            raw[16:]
        )

        decoded = decode_defined_type(
            event_name,
            type_map[event_name],
            reader,
            type_map,
        )

        remaining = (
            reader.remaining_bytes()
        )

        status = (
            "ok"
            if not remaining
            else "ok_with_remaining_bytes"
        )

        return {
            "event_name":
                event_name,

            "event_discriminator":
                event_discriminator.hex(),

            "decoded_event":
                decoded,

            "remaining_bytes_hex":
                remaining.hex(),

            "decode_status":
                status,

            "decode_error":
                None,
        }

    except Exception as error:

        return {
            "event_name":
                event_name,

            "event_discriminator":
                event_discriminator.hex(),

            "decoded_event":
                None,

            "remaining_bytes_hex":
                raw[16:].hex(),

            "decode_status":
                "decode_error",

            "decode_error":
                str(error),
        }


def main():

    idl = load_idl(
        IDL_PATH
    )

    idl_version = (
        idl
        .get("metadata", {})
        .get("version")
    )

    idl_sha256 = get_idl_sha256(
        IDL_PATH
    )

    type_map = build_type_map(
        idl
    )

    event_map = build_event_map(
        idl
    )

    print(
        f"IDL version: {idl_version}"
    )

    print(
        f"IDL SHA256: {idl_sha256}"
    )

    print(
        f"Loaded {len(event_map)} "
        f"events from IDL"
    )

    con = duckdb.connect(
        str(DB_PATH)
    )

    con.execute("""
        create schema if not exists decoded
    """)

    con.execute(f"""
        drop table if exists
        {BUILD_TABLE}
    """)

    con.execute(f"""
        create table {BUILD_TABLE} (

            signature varchar,

            parent_instruction_index integer,

            inner_instruction_index integer,

            block_timestamp timestamptz,

            is_success boolean,

            event_name varchar,

            event_discriminator varchar,

            decoded_event json,

            remaining_bytes_hex varchar,

            decode_status varchar,

            decode_error varchar,

            idl_version varchar,

            idl_sha256 varchar,

            decoded_at timestamptz,

            primary key (
                signature,
                parent_instruction_index,
                inner_instruction_index
            )
        )
    """)

    rows = con.execute(f"""
        select
            signature,
            parent_instruction_index,
            inner_instruction_index,
            block_timestamp,
            is_success,
            instruction_data

        from {SOURCE_MODEL}

        where instruction_data is not null

        order by
            signature,
            parent_instruction_index,
            inner_instruction_index
    """).fetchall()

    print(
        f"Found {len(rows)} "
        f"Offerbook inner instructions"
    )

    event_count = 0

    for (
        signature,
        parent_index,
        inner_index,
        block_timestamp,
        is_success,
        instruction_data,
    ) in rows:

        result = decode_event(
            instruction_data,
            event_map,
            type_map,
        )

        # Not an Anchor CPI event
        if result is None:
            continue

        event_count += 1

        decoded_json = (
            json.dumps(
                result["decoded_event"]
            )
            if result[
                "decoded_event"
            ] is not None
            else None
        )

        con.execute(f"""
            insert into {BUILD_TABLE}
            (
                signature,
                parent_instruction_index,
                inner_instruction_index,
                block_timestamp,
                is_success,

                event_name,
                event_discriminator,

                decoded_event,

                remaining_bytes_hex,

                decode_status,
                decode_error,

                idl_version,
                idl_sha256,

                decoded_at
            )

            values (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                cast(? as json),
                ?,
                ?,
                ?,
                ?,
                ?,
                now()
            )
        """, [
            signature,
            parent_index,
            inner_index,
            block_timestamp,
            is_success,

            result["event_name"],
            result[
                "event_discriminator"
            ],

            decoded_json,

            result[
                "remaining_bytes_hex"
            ],

            result[
                "decode_status"
            ],

            result[
                "decode_error"
            ],

            idl_version,
            idl_sha256,
        ])

    con.execute(
        "begin transaction"
    )

    con.execute(f"""
        drop table if exists
        {FINAL_TABLE}
    """)

    con.execute(f"""
        alter table {BUILD_TABLE}
        rename to offerbook_events
    """)

    con.execute(
        "commit"
    )

    print()
    print(
        f"Decoded {event_count} events"
    )

    print()
    print("Event counts:")

    counts = con.execute(f"""
        select
            event_name,
            decode_status,
            count(*) as event_count

        from {FINAL_TABLE}

        group by 1, 2

        order by
            event_count desc,
            event_name
    """).fetchall()

    for row in counts:
        print(row)

    con.close()


if __name__ == "__main__":
    main()