import copy
import json
import os
from pathlib import Path

import base58
import pandas as pd

from ingestion.db import (
    get_connection,
    get_target,
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

SOURCE_MODEL = "main.int_offerbook_instructions"

FINAL_TABLE = "decoded.offerbook_instructions"

BUILD_TABLE = "decoded.offerbook_instructions_build"


# ============================================================
# Decoder configuration
# ============================================================

# Normal:
#
#   DECODE_FULL_REFRESH=0
#   DECODE_RETRY_ERRORS=0
#
# Decode only completely new instructions.
#
#
# Retry errors:
#
#   DECODE_FULL_REFRESH=0
#   DECODE_RETRY_ERRORS=1
#
# Reprocess rows currently marked decode_error.
#
#
# Full refresh:
#
#   DECODE_FULL_REFRESH=1
#   DECODE_RETRY_ERRORS=0
#
# Rebuild the whole decoded table.


FULL_REFRESH = (
    os.getenv(
        "DECODE_FULL_REFRESH",
        "0",
    )
    == "1"
)


RETRY_ERRORS = (
    os.getenv(
        "DECODE_RETRY_ERRORS",
        "0",
    )
    == "1"
)


if FULL_REFRESH and RETRY_ERRORS:
    raise ValueError(
        "DECODE_FULL_REFRESH and "
        "DECODE_RETRY_ERRORS cannot both be enabled."
    )


# Number of rows sent to MotherDuck in one upload.
#
# Keep this reasonably small because decoded_args and
# decoded_accounts can contain fairly large JSON values.
DECODE_BATCH_SIZE = int(
    os.getenv(
        "DECODE_BATCH_SIZE",
        "1000",
    )
)


# Maximum rows processed during one incremental/retry run.
#
# This gives us resumability and prevents one execution
# from trying to process hundreds of thousands of rows.
MAX_ROWS_PER_RUN = int(
    os.getenv(
        "DECODE_MAX_ROWS_PER_RUN",
        "20000",
    )
)


# ============================================================
# Known historical schema evolution
# ============================================================

# These instructions kept the same Anchor discriminator,
# but their serialized arguments changed historically.

LEGACY_OFFER_INSTRUCTIONS = {
    "create_token_principal_offer",
    "create_token_collateral_offer",
    "create_nft_principal_offer",
    "create_nft_collateral_offer",
}


# In the historical schema these parameter structs did not
# contain the final:
#
#     allow_extend: bool
#
LEGACY_OFFER_PARAM_TYPES = {
    "TokenPrincipalOfferParams",
    "TokenCollateralOfferParams",
    "NftPrincipalOfferParams",
    "NftCollateralOfferParams",
}


# ============================================================
# Database setup
# ============================================================

def create_decoded_table(
    con,
    table_name,
):

    con.execute("""
        create schema if not exists decoded
    """)

    con.execute(f"""
        create table if not exists
        {table_name}
        (
            signature varchar,
            instruction_index integer,

            instruction_data varchar,
            instruction_data_hex varchar,

            discriminator varchar,
            instruction_name varchar,

            decoded_args json,
            decoded_accounts json,

            remaining_bytes_hex varchar,
            decoded_length integer,

            decode_status varchar,
            decode_error varchar,

            decoder_schema varchar,

            idl_version varchar,
            decoded_at timestamptz,

            primary key (
                signature,
                instruction_index
            )
        )
    """)


def ensure_decoded_table(
    con,
):

    create_decoded_table(
        con,
        FINAL_TABLE,
    )

    # Existing production tables were created before
    # decoder_schema existed.
    #
    # CREATE TABLE IF NOT EXISTS would not add this
    # column to an existing table, so add it explicitly.
    con.execute(f"""
        alter table
        {FINAL_TABLE}

        add column
        if not exists
        decoder_schema varchar
    """)


# ============================================================
# Borsh reader
# ============================================================

class BorshReader:

    def __init__(
        self,
        data: bytes,
    ):

        self.data = data
        self.offset = 0


    def read(
        self,
        length: int,
    ) -> bytes:

        end = (
            self.offset
            + length
        )

        if end > len(self.data):

            raise ValueError(
                f"Not enough bytes: "
                f"need {length}, "
                f"have "
                f"{len(self.data) - self.offset}"
            )

        value = (
            self.data[
                self.offset:end
            ]
        )

        self.offset = end

        return value


    def read_int(
        self,
        length: int,
        signed: bool = False,
    ) -> int:

        return int.from_bytes(
            self.read(length),
            byteorder="little",
            signed=signed,
        )


    def remaining_bytes(
        self,
    ) -> bytes:

        return self.data[
            self.offset:
        ]


# ============================================================
# IDL helpers
# ============================================================

def load_idl(
    path: Path,
) -> dict:

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(
            file
        )


def get_defined_name(
    type_definition: dict,
) -> str:

    defined = (
        type_definition[
            "defined"
        ]
    )

    if isinstance(
        defined,
        str,
    ):
        return defined

    return defined[
        "name"
    ]


def build_type_map(
    idl: dict,
) -> dict:

    return {
        item["name"]:
            item["type"]

        for item
        in idl.get(
            "types",
            [],
        )
    }


# ============================================================
# Build historical Offerbook type map
# ============================================================

def build_legacy_type_map(
    idl: dict,
) -> dict:

    # Make a completely independent copy.
    #
    # We must NOT modify the authoritative current map.
    legacy_type_map = copy.deepcopy(
        build_type_map(idl)
    )


    for type_name in (
        LEGACY_OFFER_PARAM_TYPES
    ):

        definition = (
            legacy_type_map.get(
                type_name
            )
        )


        if definition is None:

            raise ValueError(
                f"Legacy type not found "
                f"in IDL: {type_name}"
            )


        if (
            definition.get("kind")
            != "struct"
        ):

            raise ValueError(
                f"Expected {type_name} "
                f"to be a struct."
            )


        fields = (
            definition.get(
                "fields",
                [],
            )
        )


        # Historical Offerbook schema:
        #
        # remove only the trailing
        # allow_extend bool.
        definition["fields"] = [
            field

            for field in fields

            if field.get("name")
            != "allow_extend"
        ]


    return legacy_type_map


def build_instruction_discriminators(
    idl: dict,
) -> list:

    instructions = []


    for instruction in idl.get(
        "instructions",
        [],
    ):

        discriminator = bytes(
            instruction[
                "discriminator"
            ]
        )

        instructions.append(
            (
                discriminator,
                instruction,
            )
        )


    # Defensive:
    #
    # If discriminator lengths ever differ,
    # match the longest first.
    instructions.sort(
        key=lambda item:
            len(item[0]),
        reverse=True,
    )


    return instructions


# ============================================================
# Borsh primitive integers
# ============================================================

INTEGER_TYPES = {

    "u8":
        (1, False),

    "u16":
        (2, False),

    "u32":
        (4, False),

    "u64":
        (8, False),

    "u128":
        (16, False),

    "i8":
        (1, True),

    "i16":
        (2, True),

    "i32":
        (4, True),

    "i64":
        (8, True),

    "i128":
        (16, True),
}


# ============================================================
# Generic Borsh type decoder
# ============================================================

def decode_type(
    type_definition,
    reader: BorshReader,
    type_map: dict,
):

    # --------------------------------------------------------
    # Primitive types
    # --------------------------------------------------------

    if isinstance(
        type_definition,
        str,
    ):

        if (
            type_definition
            in INTEGER_TYPES
        ):

            (
                length,
                signed,
            ) = INTEGER_TYPES[
                type_definition
            ]

            return reader.read_int(
                length,
                signed=signed,
            )


        if (
            type_definition
            == "bool"
        ):

            value = (
                reader.read_int(1)
            )


            if value not in (
                0,
                1,
            ):

                raise ValueError(
                    f"Invalid Borsh "
                    f"bool value: "
                    f"{value}"
                )


            return value == 1


        if (
            type_definition
            == "pubkey"
        ):

            raw_pubkey = (
                reader.read(32)
            )

            return (
                base58
                .b58encode(
                    raw_pubkey
                )
                .decode(
                    "utf-8"
                )
            )


        if (
            type_definition
            == "string"
        ):

            length = (
                reader.read_int(4)
            )

            return (
                reader
                .read(length)
                .decode(
                    "utf-8"
                )
            )


        if (
            type_definition
            == "bytes"
        ):

            length = (
                reader.read_int(4)
            )

            return (
                reader
                .read(length)
                .hex()
            )


        raise ValueError(
            f"Unsupported primitive "
            f"type: "
            f"{type_definition}"
        )


    # --------------------------------------------------------
    # Compound IDL types
    # --------------------------------------------------------

    if isinstance(
        type_definition,
        dict,
    ):

        # ----------------------------------------------------
        # Defined type
        # ----------------------------------------------------

        if (
            "defined"
            in type_definition
        ):

            name = get_defined_name(
                type_definition
            )


            if name not in type_map:

                raise ValueError(
                    f"Unknown defined "
                    f"type: {name}"
                )


            return decode_defined_type(
                name,
                type_map[name],
                reader,
                type_map,
            )


        # ----------------------------------------------------
        # Fixed-length array
        # ----------------------------------------------------

        if (
            "array"
            in type_definition
        ):

            (
                item_type,
                length,
            ) = (
                type_definition[
                    "array"
                ]
            )


            return [
                decode_type(
                    item_type,
                    reader,
                    type_map,
                )

                for _
                in range(length)
            ]


        # ----------------------------------------------------
        # Vector
        # ----------------------------------------------------

        if (
            "vec"
            in type_definition
        ):

            item_type = (
                type_definition[
                    "vec"
                ]
            )

            length = (
                reader.read_int(4)
            )


            return [
                decode_type(
                    item_type,
                    reader,
                    type_map,
                )

                for _
                in range(length)
            ]


        # ----------------------------------------------------
        # Option
        # ----------------------------------------------------

        if (
            "option"
            in type_definition
        ):

            tag = (
                reader.read_int(1)
            )


            if tag == 0:
                return None


            if tag != 1:

                raise ValueError(
                    f"Invalid Borsh "
                    f"option tag: "
                    f"{tag}"
                )


            return decode_type(
                type_definition[
                    "option"
                ],
                reader,
                type_map,
            )


        # ----------------------------------------------------
        # Tuple
        # ----------------------------------------------------

        if (
            "tuple"
            in type_definition
        ):

            return [
                decode_type(
                    item,
                    reader,
                    type_map,
                )

                for item
                in type_definition[
                    "tuple"
                ]
            ]


    raise ValueError(
        f"Unsupported IDL "
        f"type definition: "
        f"{type_definition}"
    )


# ============================================================
# Defined type decoder
# ============================================================

def decode_defined_type(
    name: str,
    definition: dict,
    reader: BorshReader,
    type_map: dict,
):

    kind = definition[
        "kind"
    ]


    # --------------------------------------------------------
    # Struct
    # --------------------------------------------------------

    if kind == "struct":

        result = {}


        for field in definition.get(
            "fields",
            [],
        ):

            result[
                field["name"]
            ] = decode_type(
                field["type"],
                reader,
                type_map,
            )


        return result


    # --------------------------------------------------------
    # Enum
    # --------------------------------------------------------

    if kind == "enum":

        variant_index = (
            reader.read_int(1)
        )

        variants = definition[
            "variants"
        ]


        if (
            variant_index
            >= len(variants)
        ):

            raise ValueError(
                f"Invalid enum variant "
                f"index {variant_index} "
                f"for {name}"
            )


        variant = (
            variants[
                variant_index
            ]
        )


        fields = (
            variant.get(
                "fields",
                [],
            )
        )


        result = {
            "variant":
                variant["name"],

            "variant_index":
                variant_index,
        }


        if not fields:
            return result


        named_fields = all(
            isinstance(
                field,
                dict,
            )
            and "name" in field
            and "type" in field

            for field
            in fields
        )


        if named_fields:

            result[
                "fields"
            ] = {

                field["name"]:
                    decode_type(
                        field["type"],
                        reader,
                        type_map,
                    )

                for field
                in fields
            }


        else:

            result[
                "fields"
            ] = [

                decode_type(
                    field,
                    reader,
                    type_map,
                )

                for field
                in fields
            ]


        return result


    raise ValueError(
        f"Unsupported defined type "
        f"kind for {name}: "
        f"{kind}"
    )


# ============================================================
# JSON helpers
# ============================================================

def normalize_json(
    value,
):

    if value is None:
        return None


    if isinstance(
        value,
        str,
    ):

        return json.loads(
            value
        )


    return value


def normalize_account_key(
    value,
):

    if isinstance(
        value,
        str,
    ):

        return value


    if isinstance(
        value,
        dict,
    ):

        return value.get(
            "pubkey"
        )


    return None


# ============================================================
# Transaction account keys
# ============================================================

def get_transaction_account_keys(
    transaction_json: dict,
) -> list:

    message = (
        transaction_json
        .get(
            "transaction",
            {},
        )
        .get(
            "message",
            {},
        )
    )


    static_keys = [

        normalize_account_key(
            value
        )

        for value
        in message.get(
            "accountKeys",
            [],
        )
    ]


    loaded_addresses = (
        transaction_json
        .get(
            "meta",
            {},
        )
        .get(
            "loadedAddresses"
        )
        or {}
    )


    writable = (
        loaded_addresses.get(
            "writable",
            [],
        )
    )


    readonly = (
        loaded_addresses.get(
            "readonly",
            [],
        )
    )


    return (
        static_keys
        + writable
        + readonly
    )


# ============================================================
# Decode instruction accounts
# ============================================================

def decode_accounts(
    instruction_definition: dict,
    account_indexes,
    transaction_json,
) -> dict:

    account_indexes = (
        normalize_json(
            account_indexes
        )
        or []
    )


    transaction_json = (
        normalize_json(
            transaction_json
        )
        or {}
    )


    transaction_keys = (
        get_transaction_account_keys(
            transaction_json
        )
    )


    result = {}


    for (
        position,
        account_definition,
    ) in enumerate(
        instruction_definition
        .get(
            "accounts",
            [],
        )
    ):

        account_name = (
            account_definition[
                "name"
            ]
        )


        if (
            position
            >= len(account_indexes)
        ):

            result[
                account_name
            ] = None

            continue


        account_index = int(
            account_indexes[
                position
            ]
        )


        if (
            account_index
            >= len(
                transaction_keys
            )
        ):

            result[
                account_name
            ] = None

            continue


        result[
            account_name
        ] = (
            transaction_keys[
                account_index
            ]
        )


    return result


# ============================================================
# Decode one instruction using one schema
# ============================================================

def decode_instruction(
    instruction_data: str,
    instruction_definitions: list,
    type_map: dict,
):

    raw = base58.b58decode(
        instruction_data
    )


    matched_discriminator = None
    matched_instruction = None


    for (
        discriminator,
        instruction,
    ) in instruction_definitions:

        if raw.startswith(
            discriminator
        ):

            matched_discriminator = (
                discriminator
            )

            matched_instruction = (
                instruction
            )

            break


    # --------------------------------------------------------
    # Unknown instruction discriminator
    # --------------------------------------------------------

    if matched_instruction is None:

        return {
            "instruction_data_hex":
                raw.hex(),

            "discriminator":
                raw[:8].hex(),

            "instruction_name":
                None,

            "decoded_args":
                None,

            "remaining_bytes_hex":
                raw.hex(),

            "decoded_length":
                len(raw),

            "decode_status":
                "unknown_discriminator",

            "decode_error":
                None,

            "instruction_definition":
                None,
        }


    # --------------------------------------------------------
    # Decode arguments after Anchor discriminator
    # --------------------------------------------------------

    reader = BorshReader(
        raw[
            len(
                matched_discriminator
            ):
        ]
    )


    decoded_args = {}


    for argument in (
        matched_instruction
        .get(
            "args",
            [],
        )
    ):

        decoded_args[
            argument["name"]
        ] = decode_type(
            argument["type"],
            reader,
            type_map,
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
        "instruction_data_hex":
            raw.hex(),

        "discriminator":
            matched_discriminator.hex(),

        "instruction_name":
            matched_instruction[
                "name"
            ],

        "decoded_args":
            decoded_args,

        "remaining_bytes_hex":
            remaining.hex(),

        "decoded_length":
            len(raw),

        "decode_status":
            status,

        "decode_error":
            None,

        "instruction_definition":
            matched_instruction,
    }


# ============================================================
# Version-aware instruction decoder
# ============================================================

def decode_instruction_with_schema(
    instruction_data: str,
    instruction_definitions: list,
    current_type_map: dict,
    legacy_type_map: dict,
):

    raw = base58.b58decode(
        instruction_data
    )


    matched_instruction = None


    for (
        discriminator,
        instruction,
    ) in instruction_definitions:

        if raw.startswith(
            discriminator
        ):

            matched_instruction = (
                instruction
            )

            break


    # --------------------------------------------------------
    # Unknown discriminator
    # --------------------------------------------------------

    if matched_instruction is None:

        result = decode_instruction(
            instruction_data,
            instruction_definitions,
            current_type_map,
        )

        result[
            "decoder_schema"
        ] = "unknown"

        return result


    instruction_name = (
        matched_instruction[
            "name"
        ]
    )


    legacy_candidate = (
        instruction_name
        in LEGACY_OFFER_INSTRUCTIONS
    )


    # ========================================================
    # Attempt 1: current schema
    # ========================================================

    current_result = None
    current_error = None


    try:

        current_result = (
            decode_instruction(
                instruction_data,
                instruction_definitions,
                current_type_map,
            )
        )


        # Exact decode means we trust current schema.
        if (
            current_result[
                "decode_status"
            ]
            == "ok"
        ):

            current_result[
                "decoder_schema"
            ] = "current_v2"

            return current_result


        # If this instruction is unrelated to known schema
        # evolution, don't attempt the legacy map.
        if not legacy_candidate:

            current_result[
                "decoder_schema"
            ] = "current_v2"

            return current_result


    except Exception as error:

        current_error = error


        if not legacy_candidate:
            raise


    # ========================================================
    # Attempt 2: historical schema
    # ========================================================

    try:

        legacy_result = (
            decode_instruction(
                instruction_data,
                instruction_definitions,
                legacy_type_map,
            )
        )


        # Important:
        #
        # We only accept legacy decoding when it consumes
        # the payload EXACTLY.
        if (
            legacy_result[
                "decode_status"
            ]
            == "ok"
        ):

            legacy_result[
                "decoder_schema"
            ] = "legacy_v1"

            return legacy_result


    except Exception as legacy_error:

        if current_error is not None:

            raise ValueError(
                f"Current schema failed: "
                f"{current_error}; "
                f"Legacy schema failed: "
                f"{legacy_error}"
            )


        # Current parsing at least produced a result, but
        # legacy decoding threw an exception.
        if current_result is not None:

            current_result[
                "decoder_schema"
            ] = "current_v2"

            return current_result


        raise


    # --------------------------------------------------------
    # Legacy did not produce an exact decode.
    # Keep the current interpretation if one exists.
    # --------------------------------------------------------

    if current_result is not None:

        current_result[
            "decoder_schema"
        ] = "current_v2"

        return current_result


    if current_error is not None:

        raise current_error


    raise ValueError(
        "Instruction could not be decoded "
        "using current or legacy schema."
    )


# ============================================================
# Batch insert / upsert
# ============================================================

def insert_decoded_batch(
    con,
    table_name,
    decoded_rows,
):

    if not decoded_rows:
        return 0


    batch_df = pd.DataFrame(
        decoded_rows
    )


    # DuckDB can query a registered pandas DataFrame as if
    # it were a temporary table.
    con.register(
        "decoded_batch_df",
        batch_df,
    )


    try:

        con.execute(f"""
            insert into
            {table_name}
            (
                signature,
                instruction_index,

                instruction_data,
                instruction_data_hex,

                discriminator,
                instruction_name,

                decoded_args,
                decoded_accounts,

                remaining_bytes_hex,
                decoded_length,

                decode_status,
                decode_error,

                decoder_schema,

                idl_version,
                decoded_at
            )

            select
                signature,
                instruction_index,

                instruction_data,
                instruction_data_hex,

                discriminator,
                instruction_name,

                cast(
                    decoded_args
                    as json
                ),

                cast(
                    decoded_accounts
                    as json
                ),

                remaining_bytes_hex,
                decoded_length,

                decode_status,
                decode_error,

                decoder_schema,

                idl_version,
                current_timestamp

            from decoded_batch_df

            on conflict (
                signature,
                instruction_index
            )

            do update set

                instruction_data =
                    excluded.instruction_data,

                instruction_data_hex =
                    excluded.instruction_data_hex,

                discriminator =
                    excluded.discriminator,

                instruction_name =
                    excluded.instruction_name,

                decoded_args =
                    excluded.decoded_args,

                decoded_accounts =
                    excluded.decoded_accounts,

                remaining_bytes_hex =
                    excluded.remaining_bytes_hex,

                decoded_length =
                    excluded.decoded_length,

                decode_status =
                    excluded.decode_status,

                decode_error =
                    excluded.decode_error,

                decoder_schema =
                    excluded.decoder_schema,

                idl_version =
                    excluded.idl_version,

                decoded_at =
                    excluded.decoded_at
        """)


    finally:

        con.unregister(
            "decoded_batch_df"
        )


    return len(
        decoded_rows
    )


# ============================================================
# Source selection
# ============================================================

def get_rows_to_decode(
    con,
):

    # ========================================================
    # FULL REFRESH
    #
    # Decode every source instruction.
    #
    # No LIMIT here because the build table must contain
    # the complete dataset before it replaces FINAL_TABLE.
    # ========================================================

    if FULL_REFRESH:

        return con.execute(f"""
            select
                signature,
                instruction_index,
                instruction_data,
                account_indexes,
                transaction_json

            from {SOURCE_MODEL}

            where
                instruction_data
                is not null

            order by
                signature,
                instruction_index
        """).fetchall()


    # ========================================================
    # RETRY ERRORS
    #
    # Only reprocess rows already present in the decoded
    # table whose previous decode failed.
    # ========================================================

    if RETRY_ERRORS:

        return con.execute(
            f"""
                select
                    s.signature,
                    s.instruction_index,
                    s.instruction_data,
                    s.account_indexes,
                    s.transaction_json

                from {SOURCE_MODEL} as s

                inner join
                    {FINAL_TABLE} as d

                    on
                        s.signature =
                        d.signature

                    and
                        s.instruction_index =
                        d.instruction_index

                where
                    s.instruction_data
                    is not null

                    and d.decode_status =
                        'decode_error'

                order by
                    s.signature,
                    s.instruction_index

                limit ?
            """,
            [
                MAX_ROWS_PER_RUN
            ],
        ).fetchall()


    # ========================================================
    # NORMAL INCREMENTAL
    #
    # Only source instructions that do not yet exist in
    # the decoded table.
    # ========================================================

    return con.execute(
        f"""
            select
                s.signature,
                s.instruction_index,
                s.instruction_data,
                s.account_indexes,
                s.transaction_json

            from {SOURCE_MODEL} as s

            left join
                {FINAL_TABLE} as d

                on
                    s.signature =
                    d.signature

                and
                    s.instruction_index =
                    d.instruction_index

            where
                s.instruction_data
                is not null

                and d.signature
                is null

            order by
                s.signature,
                s.instruction_index

            limit ?
        """,
        [
            MAX_ROWS_PER_RUN
        ],
    ).fetchall()


# ============================================================
# Main
# ============================================================

def main():

    print()
    print(
        "Offerbook instruction decoder"
    )

    print(
        "-----------------------------"
    )


    # --------------------------------------------------------
    # Load current IDL
    # --------------------------------------------------------

    idl = load_idl(
        IDL_PATH
    )


    current_type_map = (
        build_type_map(
            idl
        )
    )


    legacy_type_map = (
        build_legacy_type_map(
            idl
        )
    )


    instruction_definitions = (
        build_instruction_discriminators(
            idl
        )
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


    # --------------------------------------------------------
    # Determine execution mode
    # --------------------------------------------------------

    if FULL_REFRESH:

        decoder_mode = (
            "FULL REFRESH"
        )

    elif RETRY_ERRORS:

        decoder_mode = (
            "RETRY ERRORS"
        )

    else:

        decoder_mode = (
            "INCREMENTAL"
        )


    print(
        f"Loaded "
        f"{len(instruction_definitions)} "
        f"instructions from IDL"
    )

    print(
        f"IDL version: "
        f"{idl_version}"
    )

    print(
        f"Mode: "
        f"{decoder_mode}"
    )

    print(
        f"Batch size: "
        f"{DECODE_BATCH_SIZE:,}"
    )


    if not FULL_REFRESH:

        print(
            f"Maximum rows this run: "
            f"{MAX_ROWS_PER_RUN:,}"
        )


    # --------------------------------------------------------
    # Connect to selected database
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


        # ====================================================
        # Full refresh setup
        # ====================================================

        if FULL_REFRESH:

            con.execute(f"""
                drop table
                if exists
                {BUILD_TABLE}
            """)


            create_decoded_table(
                con,
                BUILD_TABLE,
            )


            target_table = (
                BUILD_TABLE
            )


        # ====================================================
        # Incremental / retry setup
        # ====================================================

        else:

            ensure_decoded_table(
                con
            )


            target_table = (
                FINAL_TABLE
            )


        # ----------------------------------------------------
        # Source rows
        # ----------------------------------------------------

        rows = (
            get_rows_to_decode(
                con
            )
        )


        print(
            f"Instructions selected this run: "
            f"{len(rows):,}"
        )


        if not rows:

            print()

            if RETRY_ERRORS:

                print(
                    "No decode_error rows "
                    "remain to retry."
                )

            elif FULL_REFRESH:

                print(
                    "No instructions found."
                )

            else:

                print(
                    "No new instructions "
                    "require decoding."
                )

            return


        # ----------------------------------------------------
        # Counters
        # ----------------------------------------------------

        success_count = 0
        error_count = 0

        current_schema_count = 0
        legacy_schema_count = 0

        processed_count = 0

        decoded_rows = []


        # ====================================================
        # Decode loop
        # ====================================================

        for (
            signature,
            instruction_index,
            instruction_data,
            account_indexes,
            transaction_json,
        ) in rows:


            try:

                result = (
                    decode_instruction_with_schema(
                        instruction_data,
                        instruction_definitions,
                        current_type_map,
                        legacy_type_map,
                    )
                )


                instruction_definition = (
                    result[
                        "instruction_definition"
                    ]
                )


                decoded_accounts = None


                if (
                    instruction_definition
                    is not None
                ):

                    decoded_accounts = (
                        decode_accounts(
                            instruction_definition,
                            account_indexes,
                            transaction_json,
                        )
                    )


                decode_error = (
                    result.get(
                        "decode_error"
                    )
                )


                if (
                    result[
                        "decode_status"
                    ]
                    .startswith(
                        "ok"
                    )
                ):

                    success_count += 1

                else:

                    error_count += 1


                if (
                    result.get(
                        "decoder_schema"
                    )
                    == "legacy_v1"
                ):

                    legacy_schema_count += 1


                elif (
                    result.get(
                        "decoder_schema"
                    )
                    == "current_v2"
                ):

                    current_schema_count += 1


            # ------------------------------------------------
            # Decode failure
            # ------------------------------------------------

            except Exception as error:

                try:

                    raw = (
                        base58.b58decode(
                            instruction_data
                        )
                    )

                    raw_hex = (
                        raw.hex()
                    )

                    discriminator = (
                        raw[:8].hex()
                    )

                    decoded_length = (
                        len(raw)
                    )


                except Exception:

                    raw_hex = None
                    discriminator = None
                    decoded_length = None


                result = {
                    "instruction_data_hex":
                        raw_hex,

                    "discriminator":
                        discriminator,

                    "instruction_name":
                        None,

                    "decoded_args":
                        None,

                    "remaining_bytes_hex":
                        raw_hex,

                    "decoded_length":
                        decoded_length,

                    "decode_status":
                        "decode_error",

                    "decoder_schema":
                        None,
                }


                decoded_accounts = None

                decode_error = (
                    str(error)
                )

                error_count += 1


            # ------------------------------------------------
            # Add result to in-memory batch
            # ------------------------------------------------

            decoded_rows.append(
                {
                    "signature":
                        signature,

                    "instruction_index":
                        instruction_index,

                    "instruction_data":
                        instruction_data,

                    "instruction_data_hex":
                        result[
                            "instruction_data_hex"
                        ],

                    "discriminator":
                        result[
                            "discriminator"
                        ],

                    "instruction_name":
                        result[
                            "instruction_name"
                        ],

                    "decoded_args":
                        (
                            json.dumps(
                                result[
                                    "decoded_args"
                                ]
                            )

                            if result[
                                "decoded_args"
                            ]
                            is not None

                            else None
                        ),

                    "decoded_accounts":
                        (
                            json.dumps(
                                decoded_accounts
                            )

                            if decoded_accounts
                            is not None

                            else None
                        ),

                    "remaining_bytes_hex":
                        result[
                            "remaining_bytes_hex"
                        ],

                    "decoded_length":
                        result[
                            "decoded_length"
                        ],

                    "decode_status":
                        result[
                            "decode_status"
                        ],

                    "decode_error":
                        decode_error,

                    "decoder_schema":
                        result.get(
                            "decoder_schema"
                        ),

                    "idl_version":
                        idl_version,
                }
            )


            processed_count += 1


            # ------------------------------------------------
            # Flush full batch
            # ------------------------------------------------

            if (
                len(decoded_rows)
                >= DECODE_BATCH_SIZE
            ):

                insert_decoded_batch(
                    con,
                    target_table,
                    decoded_rows,
                )


                print(
                    f"Processed "
                    f"{processed_count:,}/"
                    f"{len(rows):,} "
                    f"instructions"
                )


                decoded_rows = []


        # ====================================================
        # Final partial batch
        # ====================================================

        if decoded_rows:

            insert_decoded_batch(
                con,
                target_table,
                decoded_rows,
            )


            print(
                f"Processed "
                f"{processed_count:,}/"
                f"{len(rows):,} "
                f"instructions"
            )


        # ====================================================
        # Full refresh swap
        # ====================================================

        if FULL_REFRESH:

            con.execute(
                "begin transaction"
            )


            try:

                con.execute(f"""
                    drop table
                    if exists
                    {FINAL_TABLE}
                """)


                con.execute(f"""
                    alter table
                    {BUILD_TABLE}

                    rename to
                    offerbook_instructions
                """)


                con.execute(
                    "commit"
                )


            except Exception:

                con.execute(
                    "rollback"
                )

                raise


        # ====================================================
        # Run summary
        # ====================================================

        print()

        print(
            f"Decoded successfully: "
            f"{success_count:,}"
        )

        print(
            f"Unknown/failed: "
            f"{error_count:,}"
        )

        print(
            f"Current schema: "
            f"{current_schema_count:,}"
        )

        print(
            f"Legacy schema: "
            f"{legacy_schema_count:,}"
        )

        print(
            f"Processed this run: "
            f"{processed_count:,}"
        )


        # ----------------------------------------------------
        # Remaining error count
        # ----------------------------------------------------

        remaining_errors = (
            con.execute(f"""
                select count(*)

                from {FINAL_TABLE}

                where
                    decode_status =
                    'decode_error'
            """)
            .fetchone()[0]
        )


        print(
            f"Remaining decode errors: "
            f"{remaining_errors:,}"
        )


        # ----------------------------------------------------
        # Decoder schema counts
        # ----------------------------------------------------

        print()
        print(
            "Decoder schema counts:"
        )


        schema_counts = (
            con.execute(f"""
                select
                    decoder_schema,
                    decode_status,
                    count(*) as row_count

                from {FINAL_TABLE}

                group by
                    1,
                    2

                order by
                    row_count desc
            """)
            .fetchall()
        )


        for row in schema_counts:

            print(row)


        # ----------------------------------------------------
        # Instruction counts
        # ----------------------------------------------------

        print()
        print(
            "Instruction counts:"
        )


        counts = (
            con.execute(f"""
                select
                    instruction_name,
                    decode_status,
                    count(*) as
                        instruction_count

                from {FINAL_TABLE}

                group by
                    1,
                    2

                order by
                    instruction_count desc,
                    instruction_name
            """)
            .fetchall()
        )


        for row in counts:

            print(row)


    finally:

        con.close()


if __name__ == "__main__":

    main()