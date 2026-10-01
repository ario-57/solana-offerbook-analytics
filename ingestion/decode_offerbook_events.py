import hashlib
import json
import os
from pathlib import Path

import base58
import pandas as pd

from ingestion.db import (
    get_connection,
    get_target,
)

from ingestion.decode_offerbook_instructions import (
    BorshReader,
    build_type_map,
    decode_defined_type,
    load_idl,
)


# ============================================================
# Paths / tables
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

IDL_PATH = (
    PROJECT_ROOT
    / "idl"
    / "offerbook.json"
)


SOURCE_MODEL = (
    "main.int_offerbook_inner_instructions"
)


FINAL_TABLE = (
    "decoded.offerbook_events"
)


SCAN_STATE_TABLE = (
    "pipeline.offerbook_event_scan_state"
)


# ============================================================
# Configuration
# ============================================================

# Maximum number of transaction signatures scanned
# during one execution.
#
# Important:
#
# This is NOT the number of inner instructions.
# One transaction may contain multiple inner instructions.
MAX_SIGNATURES_PER_RUN = int(
    os.getenv(
        "EVENT_DECODE_MAX_SIGNATURES_PER_RUN",
        "5000",
    )
)


# Number of decoded event rows uploaded to MotherDuck
# in one batch.
EVENT_BATCH_SIZE = int(
    os.getenv(
        "EVENT_DECODE_BATCH_SIZE",
        "1000",
    )
)


# ============================================================
# Anchor emit_cpi prefix
# ============================================================

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


# ============================================================
# IDL fingerprint
# ============================================================

def get_idl_sha256(
    path: Path,
):

    with path.open(
        "rb"
    ) as file:

        return hashlib.sha256(
            file.read()
        ).hexdigest()


# ============================================================
# Event map
# ============================================================

def build_event_map(
    idl: dict,
):

    events = {}


    for event in idl.get(
        "events",
        [],
    ):

        discriminator = bytes(
            event[
                "discriminator"
            ]
        )


        events[
            discriminator
        ] = event[
            "name"
        ]


    return events


# ============================================================
# Database setup
# ============================================================

def ensure_event_table(
    con,
):

    con.execute("""
        create schema
        if not exists decoded
    """)


    con.execute(f"""
        create table
        if not exists
        {FINAL_TABLE}
        (
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


def ensure_scan_state_table(
    con,
):

    con.execute("""
        create schema
        if not exists pipeline
    """)


    con.execute(f"""
        create table
        if not exists
        {SCAN_STATE_TABLE}
        (
            signature varchar
                primary key,

            inner_instruction_count integer,

            event_count integer,

            decode_error_count integer,

            idl_sha256 varchar,

            scanned_at timestamptz
        )
    """)


# ============================================================
# Bootstrap scan state
# ============================================================

def bootstrap_scan_state(
    con,
    idl_sha256,
):

    # --------------------------------------------------------
    # Why?
    #
    # We may already have historical decoded events from
    # previous full decoder runs.
    #
    # Those signatures have definitely been scanned before.
    #
    # If their decoded events were created with the same IDL
    # and have no decode errors, we can safely mark those
    # signatures as already scanned.
    #
    # This saves unnecessary work during migration to the
    # incremental methodology.
    # --------------------------------------------------------

    con.execute(
        f"""
            insert into
            {SCAN_STATE_TABLE}
            (
                signature,
                inner_instruction_count,
                event_count,
                decode_error_count,
                idl_sha256,
                scanned_at
            )

            with event_stats as (

                select
                    signature,

                    count(*) as event_count,

                    count(*) filter (
                        where decode_status
                        = 'decode_error'
                    ) as decode_error_count

                from {FINAL_TABLE}

                where
                    idl_sha256 = ?

                group by
                    signature
            ),

            inner_stats as (

                select
                    signature,

                    count(*) as
                        inner_instruction_count

                from {SOURCE_MODEL}

                where
                    instruction_data
                    is not null

                group by
                    signature
            )

            select
                e.signature,

                i.inner_instruction_count,

                e.event_count,

                e.decode_error_count,

                ?,

                current_timestamp

            from event_stats e

            inner join inner_stats i
                on e.signature = i.signature

            where
                e.decode_error_count = 0

            on conflict(signature)
            do nothing
        """,
        [
            idl_sha256,
            idl_sha256,
        ],
    )


# ============================================================
# Decode one possible Anchor event
# ============================================================

def decode_event(
    instruction_data,
    event_map,
    type_map,
):

    raw = base58.b58decode(
        instruction_data
    )


    # Anchor emit_cpi event data needs at least:
    #
    # 8 bytes event prefix
    # +
    # 8 bytes event discriminator
    if len(raw) < 16:

        return None


    # --------------------------------------------------------
    # Is this an Anchor CPI event?
    # --------------------------------------------------------

    if (
        raw[:8]
        != ANCHOR_EVENT_TAG
    ):

        return None


    event_discriminator = (
        raw[8:16]
    )


    event_name = (
        event_map.get(
            event_discriminator
        )
    )


    # --------------------------------------------------------
    # Event prefix exists but discriminator unknown
    # --------------------------------------------------------

    if event_name is None:

        return {
            "event_name":
                None,

            "event_discriminator":
                event_discriminator.hex(),

            "decoded_event":
                None,

            "remaining_bytes_hex":
                raw[16:].hex(),

            "decode_status":
                "unknown_event",

            "decode_error":
                None,
        }


    # --------------------------------------------------------
    # Event exists in events list but corresponding
    # type definition is missing.
    # --------------------------------------------------------

    if event_name not in type_map:

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
                "missing_event_type",

            "decode_error":
                (
                    f"{event_name} "
                    f"not found in IDL types"
                ),
        }


    # --------------------------------------------------------
    # Decode Borsh event body
    # --------------------------------------------------------

    try:

        reader = BorshReader(
            raw[16:]
        )


        decoded = (
            decode_defined_type(
                event_name,
                type_map[
                    event_name
                ],
                reader,
                type_map,
            )
        )


        remaining = (
            reader.remaining_bytes()
        )


        status = (
            "ok"
            if not remaining
            else
            "ok_with_remaining_bytes"
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


# ============================================================
# Count pending transaction signatures
# ============================================================

def get_pending_signature_count(
    con,
    idl_sha256,
):

    return con.execute(
        f"""
            select
                count(*)

            from (

                select distinct
                    i.signature

                from {SOURCE_MODEL} i

                left join
                    {SCAN_STATE_TABLE} s

                    on
                        i.signature =
                        s.signature

                where
                    i.instruction_data
                    is not null

                    and (
                        s.signature
                        is null

                        or s.idl_sha256
                        is distinct from ?
                    )
            )
        """,
        [
            idl_sha256
        ],
    ).fetchone()[0]


# ============================================================
# Select signatures for this run
# ============================================================

def get_signatures_to_scan(
    con,
    idl_sha256,
):

    rows = con.execute(
        f"""
            select
                i.signature,

                min(
                    i.block_timestamp
                ) as first_timestamp

            from {SOURCE_MODEL} i

            left join
                {SCAN_STATE_TABLE} s

                on
                    i.signature =
                    s.signature

            where
                i.instruction_data
                is not null

                and (
                    s.signature
                    is null

                    or s.idl_sha256
                    is distinct from ?
                )

            group by
                i.signature

            order by
                first_timestamp,
                i.signature

            limit ?
        """,
        [
            idl_sha256,
            MAX_SIGNATURES_PER_RUN,
        ],
    ).fetchall()


    return [
        row[0]
        for row in rows
    ]


# ============================================================
# Fetch inner instructions for selected signatures
# ============================================================

def get_inner_instructions(
    con,
    signatures,
):

    if not signatures:

        return []


    signature_df = pd.DataFrame(
        {
            "signature":
                signatures
        }
    )


    con.register(
        "selected_signatures_df",
        signature_df,
    )


    try:

        rows = con.execute(f"""
            select
                i.signature,

                i.parent_instruction_index,

                i.inner_instruction_index,

                i.block_timestamp,

                i.is_success,

                i.instruction_data

            from {SOURCE_MODEL} i

            inner join
                selected_signatures_df s

                on
                    i.signature =
                    s.signature

            where
                i.instruction_data
                is not null

            order by
                i.signature,
                i.parent_instruction_index,
                i.inner_instruction_index
        """).fetchall()


    finally:

        con.unregister(
            "selected_signatures_df"
        )


    return rows


# ============================================================
# Insert event batch
# ============================================================

def insert_event_batch(
    con,
    event_rows,
):

    if not event_rows:
        return


    event_df = pd.DataFrame(
        event_rows
    )


    con.register(
        "event_batch_df",
        event_df,
    )


    try:

        con.execute(f"""
            insert into
            {FINAL_TABLE}
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

            select
                signature,

                parent_instruction_index,
                inner_instruction_index,

                block_timestamp,

                is_success,

                event_name,
                event_discriminator,

                cast(
                    decoded_event
                    as json
                ),

                remaining_bytes_hex,

                decode_status,
                decode_error,

                idl_version,
                idl_sha256,

                current_timestamp

            from event_batch_df

            on conflict (
                signature,
                parent_instruction_index,
                inner_instruction_index
            )

            do update set

                block_timestamp =
                    excluded.block_timestamp,

                is_success =
                    excluded.is_success,

                event_name =
                    excluded.event_name,

                event_discriminator =
                    excluded.event_discriminator,

                decoded_event =
                    excluded.decoded_event,

                remaining_bytes_hex =
                    excluded.remaining_bytes_hex,

                decode_status =
                    excluded.decode_status,

                decode_error =
                    excluded.decode_error,

                idl_version =
                    excluded.idl_version,

                idl_sha256 =
                    excluded.idl_sha256,

                decoded_at =
                    excluded.decoded_at
        """)


    finally:

        con.unregister(
            "event_batch_df"
        )


# ============================================================
# Insert scan-state batch
# ============================================================

def insert_scan_state_batch(
    con,
    scan_rows,
):

    if not scan_rows:
        return


    scan_df = pd.DataFrame(
        scan_rows
    )


    con.register(
        "scan_state_df",
        scan_df,
    )


    try:

        con.execute(f"""
            insert into
            {SCAN_STATE_TABLE}
            (
                signature,

                inner_instruction_count,

                event_count,

                decode_error_count,

                idl_sha256,

                scanned_at
            )

            select
                signature,

                inner_instruction_count,

                event_count,

                decode_error_count,

                idl_sha256,

                current_timestamp

            from scan_state_df

            on conflict(signature)

            do update set

                inner_instruction_count =
                    excluded.inner_instruction_count,

                event_count =
                    excluded.event_count,

                decode_error_count =
                    excluded.decode_error_count,

                idl_sha256 =
                    excluded.idl_sha256,

                scanned_at =
                    excluded.scanned_at
        """)


    finally:

        con.unregister(
            "scan_state_df"
        )


# ============================================================
# Remove previous event rows for selected signatures
# ============================================================

def delete_existing_events_for_signatures(
    con,
    signatures,
):

    if not signatures:
        return


    signature_df = pd.DataFrame(
        {
            "signature":
                signatures
        }
    )


    con.register(
        "delete_signatures_df",
        signature_df,
    )


    try:

        con.execute(f"""
            delete from
            {FINAL_TABLE}

            where signature in (

                select
                    signature

                from
                    delete_signatures_df
            )
        """)


    finally:

        con.unregister(
            "delete_signatures_df"
        )


# ============================================================
# Main
# ============================================================

def main():

    print()
    print(
        "Offerbook incremental event decoder"
    )

    print(
        "-----------------------------------"
    )


    # --------------------------------------------------------
    # IDL
    # --------------------------------------------------------

    idl = load_idl(
        IDL_PATH
    )


    idl_version = (
        idl
        .get(
            "metadata",
            {},
        )
        .get(
            "version"
        )
    )


    idl_sha256 = (
        get_idl_sha256(
            IDL_PATH
        )
    )


    type_map = (
        build_type_map(
            idl
        )
    )


    event_map = (
        build_event_map(
            idl
        )
    )


    print(
        f"IDL version: "
        f"{idl_version}"
    )


    print(
        f"IDL SHA256: "
        f"{idl_sha256}"
    )


    print(
        f"Loaded "
        f"{len(event_map)} "
        f"events from IDL"
    )


    print(
        f"Maximum signatures this run: "
        f"{MAX_SIGNATURES_PER_RUN:,}"
    )


    print(
        f"Event batch size: "
        f"{EVENT_BATCH_SIZE:,}"
    )


    # --------------------------------------------------------
    # Database connection
    # --------------------------------------------------------

    con = get_connection()


    try:

        current_database = (
            con.execute(
                "select current_database()"
            )
            .fetchone()[0]
        )


        print(
            f"Database target: "
            f"{get_target()}"
        )


        print(
            f"Database: "
            f"{current_database}"
        )

        print()


        # ----------------------------------------------------
        # Ensure required production tables exist
        # ----------------------------------------------------

        ensure_event_table(
            con
        )


        ensure_scan_state_table(
            con
        )


        # ----------------------------------------------------
        # Bootstrap state from already decoded events
        # ----------------------------------------------------

        # Check whether scan state has already been initialized.

        has_scan_state = con.execute(f"""
            select exists (
                select 1
                from {SCAN_STATE_TABLE}
                limit 1
            )
        """).fetchone()[0]


        # Bootstrap only when the table is empty.

        if not has_scan_state:

            print(
                "Initializing historical scan state..."
            )

            bootstrap_scan_state(
                con,
                idl_sha256,
            )

        else:

            print(
                "Existing scan state found. "
                "Skipping bootstrap."
            )

        # ----------------------------------------------------
        # How much work remains?
        # ----------------------------------------------------

        pending_before = (
            get_pending_signature_count(
                con,
                idl_sha256,
            )
        )


        print(
            f"Pending transaction signatures: "
            f"{pending_before:,}"
        )


        if pending_before == 0:

            print()

            print(
                "Event decoder is already "
                "up to date."
            )

            return


        # ----------------------------------------------------
        # Select manageable transaction batch
        # ----------------------------------------------------

        signatures = (
            get_signatures_to_scan(
                con,
                idl_sha256,
            )
        )


        print(
            f"Signatures selected this run: "
            f"{len(signatures):,}"
        )


        # ----------------------------------------------------
        # Fetch all inner instructions belonging to those
        # transaction signatures.
        # ----------------------------------------------------

        rows = (
            get_inner_instructions(
                con,
                signatures,
            )
        )


        print(
            f"Inner instructions selected: "
            f"{len(rows):,}"
        )

        print()


        # ----------------------------------------------------
        # Per-signature statistics
        # ----------------------------------------------------

        stats = {
            signature: {
                "inner_instruction_count": 0,
                "event_count": 0,
                "decode_error_count": 0,
            }

            for signature
            in signatures
        }


        event_rows = []


        processed_inner = 0

        total_events = 0

        total_errors = 0


        # ====================================================
        # Decode
        # ====================================================

        for (
            signature,
            parent_index,
            inner_index,
            block_timestamp,
            is_success,
            instruction_data,
        ) in rows:


            stats[
                signature
            ][
                "inner_instruction_count"
            ] += 1


            result = decode_event(
                instruction_data,
                event_map,
                type_map,
            )


            processed_inner += 1


            # ------------------------------------------------
            # Normal inner instruction, not an Anchor CPI event
            # ------------------------------------------------

            if result is None:

                if (
                    processed_inner
                    % 10000
                    == 0
                ):

                    print(
                        f"Scanned "
                        f"{processed_inner:,}/"
                        f"{len(rows):,} "
                        f"inner instructions"
                    )

                continue


            # ------------------------------------------------
            # Event found
            # ------------------------------------------------

            total_events += 1


            stats[
                signature
            ][
                "event_count"
            ] += 1


            if (
                result[
                    "decode_status"
                ]
                == "decode_error"
            ):

                total_errors += 1

                stats[
                    signature
                ][
                    "decode_error_count"
                ] += 1


            decoded_json = (
                json.dumps(
                    result[
                        "decoded_event"
                    ]
                )

                if result[
                    "decoded_event"
                ] is not None

                else None
            )


            event_rows.append(
                {
                    "signature":
                        signature,

                    "parent_instruction_index":
                        parent_index,

                    "inner_instruction_index":
                        inner_index,

                    "block_timestamp":
                        block_timestamp,

                    "is_success":
                        is_success,

                    "event_name":
                        result[
                            "event_name"
                        ],

                    "event_discriminator":
                        result[
                            "event_discriminator"
                        ],

                    "decoded_event":
                        decoded_json,

                    "remaining_bytes_hex":
                        result[
                            "remaining_bytes_hex"
                        ],

                    "decode_status":
                        result[
                            "decode_status"
                        ],

                    "decode_error":
                        result[
                            "decode_error"
                        ],

                    "idl_version":
                        idl_version,

                    "idl_sha256":
                        idl_sha256,
                }
            )


            if (
                processed_inner
                % 10000
                == 0
            ):

                print(
                    f"Scanned "
                    f"{processed_inner:,}/"
                    f"{len(rows):,} "
                    f"inner instructions"
                )


        # ----------------------------------------------------
        # Scan-state rows
        # ----------------------------------------------------

        scan_rows = []


        for signature in signatures:

            signature_stats = (
                stats[
                    signature
                ]
            )


            scan_rows.append(
                {
                    "signature":
                        signature,

                    "inner_instruction_count":
                        signature_stats[
                            "inner_instruction_count"
                        ],

                    "event_count":
                        signature_stats[
                            "event_count"
                        ],

                    "decode_error_count":
                        signature_stats[
                            "decode_error_count"
                        ],

                    "idl_sha256":
                        idl_sha256,
                }
            )


        # ====================================================
        # Database write
        # ====================================================
        #
        # Important:
        #
        # Decode everything in Python BEFORE modifying
        # production tables.
        #
        # Then perform:
        #
        # delete old rows
        # insert new event rows
        # update scan state
        #
        # inside one database transaction.
        #
        # If anything fails, ROLLBACK protects the previous
        # production state.
        # ====================================================

        con.execute(
            "begin transaction"
        )


        try:

            # ------------------------------------------------
            # Remove any old event interpretation for the
            # signatures we're rescanning.
            # ------------------------------------------------

            delete_existing_events_for_signatures(
                con,
                signatures,
            )


            # ------------------------------------------------
            # Write decoded events in manageable batches
            # ------------------------------------------------

            for start in range(
                0,
                len(event_rows),
                EVENT_BATCH_SIZE,
            ):

                batch = (
                    event_rows[
                        start:
                        start
                        + EVENT_BATCH_SIZE
                    ]
                )


                insert_event_batch(
                    con,
                    batch,
                )


            # ------------------------------------------------
            # Mark every selected transaction as scanned,
            # including transactions where event_count = 0.
            # ------------------------------------------------

            insert_scan_state_batch(
                con,
                scan_rows,
            )


            con.execute(
                "commit"
            )


        except Exception:

            con.execute(
                "rollback"
            )

            raise


        # ----------------------------------------------------
        # Final status
        # ----------------------------------------------------

        pending_after = (
            get_pending_signature_count(
                con,
                idl_sha256,
            )
        )


        print()
        print(
            f"Transaction signatures scanned: "
            f"{len(signatures):,}"
        )


        print(
            f"Inner instructions scanned: "
            f"{processed_inner:,}"
        )


        print(
            f"Events found: "
            f"{total_events:,}"
        )


        print(
            f"Event decode errors: "
            f"{total_errors:,}"
        )


        print(
            f"Pending signatures remaining: "
            f"{pending_after:,}"
        )


        print()
        print(
            "Event counts:"
        )


        counts = con.execute(f"""
            select
                event_name,
                decode_status,
                count(*) as event_count

            from {FINAL_TABLE}

            group by
                1,
                2

            order by
                event_count desc,
                event_name
        """).fetchall()


        for row in counts:

            print(row)


    finally:

        con.close()


if __name__ == "__main__":

    main()