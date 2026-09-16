import json
from pathlib import Path

import base58
import duckdb


DB_PATH = "solana.duckdb"
IDL_PATH = Path("idl/offerbook.json")

SOURCE_MODEL = "main.int_offerbook_instructions"
BUILD_TABLE = "decoded.offerbook_instructions_build"
FINAL_TABLE = "decoded.offerbook_instructions"


class BorshReader:
    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0

    def read(self, length: int) -> bytes:
        end = self.offset + length
        if end > len(self.data):
            raise ValueError(
                f"Not enough bytes: need {length}, "
                f"have {len(self.data) - self.offset}"
            )
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def read_int(self, length: int, signed: bool = False) -> int:
        return int.from_bytes(
            self.read(length),
            byteorder="little",
            signed=signed,
        )

    def remaining_bytes(self) -> bytes:
        return self.data[self.offset:]


def load_idl(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def get_defined_name(type_definition: dict) -> str:
    defined = type_definition["defined"]
    if isinstance(defined, str):
        return defined
    return defined["name"]


def build_type_map(idl: dict) -> dict:
    return {
        item["name"]: item["type"]
        for item in idl.get("types", [])
    }


def build_instruction_discriminators(idl: dict) -> list:
    instructions = []

    for instruction in idl.get("instructions", []):
        discriminator = bytes(instruction["discriminator"])
        instructions.append((discriminator, instruction))

    # If discriminator lengths ever differ, match longest first.
    instructions.sort(
        key=lambda item: len(item[0]),
        reverse=True,
    )
    return instructions


INTEGER_TYPES = {
    "u8": (1, False),
    "u16": (2, False),
    "u32": (4, False),
    "u64": (8, False),
    "u128": (16, False),
    "i8": (1, True),
    "i16": (2, True),
    "i32": (4, True),
    "i64": (8, True),
    "i128": (16, True),
}


def decode_type(type_definition, reader: BorshReader, type_map: dict):
    if isinstance(type_definition, str):
        if type_definition in INTEGER_TYPES:
            length, signed = INTEGER_TYPES[type_definition]
            return reader.read_int(length, signed=signed)

        if type_definition == "bool":
            value = reader.read_int(1)
            if value not in (0, 1):
                raise ValueError(f"Invalid Borsh bool value: {value}")
            return value == 1

        if type_definition == "pubkey":
            raw_pubkey = reader.read(32)
            return base58.b58encode(raw_pubkey).decode("utf-8")

        if type_definition == "string":
            length = reader.read_int(4)
            return reader.read(length).decode("utf-8")

        if type_definition == "bytes":
            length = reader.read_int(4)
            return reader.read(length).hex()

        raise ValueError(
            f"Unsupported primitive type: {type_definition}"
        )

    if isinstance(type_definition, dict):
        if "defined" in type_definition:
            name = get_defined_name(type_definition)

            if name not in type_map:
                raise ValueError(f"Unknown defined type: {name}")

            return decode_defined_type(
                name,
                type_map[name],
                reader,
                type_map,
            )

        if "array" in type_definition:
            item_type, length = type_definition["array"]
            return [
                decode_type(item_type, reader, type_map)
                for _ in range(length)
            ]

        if "vec" in type_definition:
            item_type = type_definition["vec"]
            length = reader.read_int(4)
            return [
                decode_type(item_type, reader, type_map)
                for _ in range(length)
            ]

        if "option" in type_definition:
            tag = reader.read_int(1)

            if tag == 0:
                return None

            if tag != 1:
                raise ValueError(
                    f"Invalid Borsh option tag: {tag}"
                )

            return decode_type(
                type_definition["option"],
                reader,
                type_map,
            )

        if "tuple" in type_definition:
            return [
                decode_type(item, reader, type_map)
                for item in type_definition["tuple"]
            ]

    raise ValueError(
        f"Unsupported IDL type definition: {type_definition}"
    )


def decode_defined_type(
    name: str,
    definition: dict,
    reader: BorshReader,
    type_map: dict,
):
    kind = definition["kind"]

    if kind == "struct":
        result = {}

        for field in definition.get("fields", []):
            result[field["name"]] = decode_type(
                field["type"],
                reader,
                type_map,
            )

        return result

    if kind == "enum":
        variant_index = reader.read_int(1)
        variants = definition["variants"]

        if variant_index >= len(variants):
            raise ValueError(
                f"Invalid enum variant index {variant_index} "
                f"for {name}"
            )

        variant = variants[variant_index]
        fields = variant.get("fields", [])

        result = {
            "variant": variant["name"],
            "variant_index": variant_index,
        }

        if not fields:
            return result

        named_fields = all(
            isinstance(field, dict)
            and "name" in field
            and "type" in field
            for field in fields
        )

        if named_fields:
            result["fields"] = {
                field["name"]: decode_type(
                    field["type"],
                    reader,
                    type_map,
                )
                for field in fields
            }
        else:
            result["fields"] = [
                decode_type(field, reader, type_map)
                for field in fields
            ]

        return result

    raise ValueError(
        f"Unsupported defined type kind for {name}: {kind}"
    )


def normalize_json(value):
    if value is None:
        return None

    if isinstance(value, str):
        return json.loads(value)

    return value


def normalize_account_key(value):
    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        return value.get("pubkey")

    return None


def get_transaction_account_keys(transaction_json: dict) -> list:
    message = (
        transaction_json
        .get("transaction", {})
        .get("message", {})
    )

    static_keys = [
        normalize_account_key(value)
        for value in message.get("accountKeys", [])
    ]

    loaded_addresses = (
        transaction_json
        .get("meta", {})
        .get("loadedAddresses")
        or {}
    )

    writable = loaded_addresses.get("writable", [])
    readonly = loaded_addresses.get("readonly", [])

    return static_keys + writable + readonly


def decode_accounts(
    instruction_definition: dict,
    account_indexes,
    transaction_json,
) -> dict:
    account_indexes = normalize_json(account_indexes) or []
    transaction_json = normalize_json(transaction_json) or {}

    transaction_keys = get_transaction_account_keys(
        transaction_json
    )

    result = {}

    for position, account_definition in enumerate(
        instruction_definition.get("accounts", [])
    ):
        account_name = account_definition["name"]

        if position >= len(account_indexes):
            result[account_name] = None
            continue

        account_index = int(account_indexes[position])

        if account_index >= len(transaction_keys):
            result[account_name] = None
            continue

        result[account_name] = transaction_keys[account_index]

    return result


def decode_instruction(
    instruction_data: str,
    instruction_definitions: list,
    type_map: dict,
):
    raw = base58.b58decode(instruction_data)

    matched_discriminator = None
    matched_instruction = None

    for discriminator, instruction in instruction_definitions:
        if raw.startswith(discriminator):
            matched_discriminator = discriminator
            matched_instruction = instruction
            break

    if matched_instruction is None:
        return {
            "instruction_data_hex": raw.hex(),
            "discriminator": raw[:8].hex(),
            "instruction_name": None,
            "decoded_args": None,
            "remaining_bytes_hex": raw.hex(),
            "decoded_length": len(raw),
            "decode_status": "unknown_discriminator",
            "decode_error": None,
            "instruction_definition": None,
        }

    reader = BorshReader(
        raw[len(matched_discriminator):]
    )

    decoded_args = {}

    for argument in matched_instruction.get("args", []):
        decoded_args[argument["name"]] = decode_type(
            argument["type"],
            reader,
            type_map,
        )

    remaining = reader.remaining_bytes()

    status = (
        "ok"
        if not remaining
        else "ok_with_remaining_bytes"
    )

    return {
        "instruction_data_hex": raw.hex(),
        "discriminator": matched_discriminator.hex(),
        "instruction_name": matched_instruction["name"],
        "decoded_args": decoded_args,
        "remaining_bytes_hex": remaining.hex(),
        "decoded_length": len(raw),
        "decode_status": status,
        "decode_error": None,
        "instruction_definition": matched_instruction,
    }


def main():
    idl = load_idl(IDL_PATH)

    type_map = build_type_map(idl)
    instruction_definitions = (
        build_instruction_discriminators(idl)
    )

    print(
        f"Loaded {len(instruction_definitions)} "
        f"instructions from IDL"
    )

    con = duckdb.connect(DB_PATH)

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS decoded
    """)

    # Build separately so an error does not destroy the
    # previous successful decoded table.
    con.execute(f"""
        DROP TABLE IF EXISTS {BUILD_TABLE}
    """)

    con.execute(f"""
        CREATE TABLE {BUILD_TABLE} (
            signature VARCHAR,
            instruction_index INTEGER,
            instruction_data VARCHAR,
            instruction_data_hex VARCHAR,
            discriminator VARCHAR,
            instruction_name VARCHAR,
            decoded_args JSON,
            decoded_accounts JSON,
            remaining_bytes_hex VARCHAR,
            decoded_length INTEGER,
            decode_status VARCHAR,
            decode_error VARCHAR,
            idl_version VARCHAR,
            decoded_at TIMESTAMPTZ,

            PRIMARY KEY (
                signature,
                instruction_index
            )
        )
    """)

    rows = con.execute(f"""
        SELECT
            signature,
            instruction_index,
            instruction_data,
            account_indexes,
            transaction_json
        FROM {SOURCE_MODEL}
        WHERE instruction_data IS NOT NULL
        ORDER BY signature, instruction_index
    """).fetchall()

    print(f"Found {len(rows)} Offerbook instructions")

    success_count = 0
    error_count = 0

    for (
        signature,
        instruction_index,
        instruction_data,
        account_indexes,
        transaction_json,
    ) in rows:

        try:
            result = decode_instruction(
                instruction_data,
                instruction_definitions,
                type_map,
            )

            instruction_definition = (
                result["instruction_definition"]
            )

            decoded_accounts = None

            if instruction_definition is not None:
                decoded_accounts = decode_accounts(
                    instruction_definition,
                    account_indexes,
                    transaction_json,
                )

            decode_error = result["decode_error"]

            if result["decode_status"].startswith("ok"):
                success_count += 1
            else:
                error_count += 1

        except Exception as error:
            try:
                raw = base58.b58decode(instruction_data)
                raw_hex = raw.hex()
                discriminator = raw[:8].hex()
                decoded_length = len(raw)
            except Exception:
                raw_hex = None
                discriminator = None
                decoded_length = None

            result = {
                "instruction_data_hex": raw_hex,
                "discriminator": discriminator,
                "instruction_name": None,
                "decoded_args": None,
                "remaining_bytes_hex": raw_hex,
                "decoded_length": decoded_length,
                "decode_status": "decode_error",
            }

            decoded_accounts = None
            decode_error = str(error)
            error_count += 1

        con.execute(f"""
            INSERT INTO {BUILD_TABLE}
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
                idl_version,
                decoded_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, CAST(? AS JSON),
                CAST(? AS JSON), ?, ?, ?, ?, ?, current_timestamp
            )
        """, [
            signature,
            instruction_index,
            instruction_data,
            result["instruction_data_hex"],
            result["discriminator"],
            result["instruction_name"],
            (
                json.dumps(result["decoded_args"])
                if result["decoded_args"] is not None
                else None
            ),
            (
                json.dumps(decoded_accounts)
                if decoded_accounts is not None
                else None
            ),
            result["remaining_bytes_hex"],
            result["decoded_length"],
            result["decode_status"],
            decode_error,
            idl.get("metadata", {}).get("version"),
        ])

    # Swap only after the whole build succeeds.
    con.execute("BEGIN TRANSACTION")

    con.execute(f"""
        DROP TABLE IF EXISTS {FINAL_TABLE}
    """)

    con.execute(f"""
        ALTER TABLE {BUILD_TABLE}
        RENAME TO offerbook_instructions
    """)

    con.execute("COMMIT")

    print()
    print(f"Decoded successfully: {success_count}")
    print(f"Unknown/failed:      {error_count}")

    print()
    print("Instruction counts:")

    counts = con.execute(f"""
        SELECT
            instruction_name,
            decode_status,
            COUNT(*) AS instruction_count
        FROM {FINAL_TABLE}
        GROUP BY 1, 2
        ORDER BY instruction_count DESC
    """).fetchall()

    for row in counts:
        print(row)

    con.close()


if __name__ == "__main__":
    main()
