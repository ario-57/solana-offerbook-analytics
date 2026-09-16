import requests
import duckdb
import json
import time


RPC_URL = "https://api.mainnet-beta.solana.com"

PROGRAM_ID = "offerbkFMvVfpQhL8ZQ5iromnjct5rz3r52B9ewu3ie"

DB_PATH = "solana.duckdb"

LIMIT = 10


# --------------------------------------------------
# 1. Get recent Offerbook transaction signatures
# --------------------------------------------------

signature_payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getSignaturesForAddress",
    "params": [
        PROGRAM_ID,
        {
            "limit": LIMIT
        }
    ]
}

response = requests.post(
    RPC_URL,
    json=signature_payload,
    timeout=30
)

response.raise_for_status()

signatures = response.json()["result"]

print(f"Found {len(signatures)} transactions")


# --------------------------------------------------
# 2. Connect to DuckDB
# --------------------------------------------------

con = duckdb.connect(DB_PATH)

con.execute("""
    CREATE SCHEMA IF NOT EXISTS raw
""")


# --------------------------------------------------
# 3. Create raw table
# --------------------------------------------------

con.execute("""
    CREATE TABLE IF NOT EXISTS raw.offerbook_transactions (
        signature VARCHAR PRIMARY KEY,
        program_id VARCHAR NOT NULL,
        slot BIGINT,
        block_time BIGINT,
        transaction_json JSON,
        ingested_at TIMESTAMPTZ
    )
""")


# --------------------------------------------------
# 4. Fetch every full transaction
# --------------------------------------------------

for item in signatures:

    signature = item["signature"]

    print(f"Fetching {signature}")

    transaction_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "json",
                "maxSupportedTransactionVersion": 1,
                "commitment": "finalized"
            }
        ]
    }

    response = requests.post(
        RPC_URL,
        json=transaction_payload,
        timeout=30
    )

    response.raise_for_status()

    rpc_response = response.json()

    transaction = rpc_response.get("result")

    if transaction is None:
        print(f"Transaction not found: {signature}")
        continue

    # ----------------------------------------------
    # 5. Insert raw transaction into DuckDB
    # ----------------------------------------------

    con.execute("""
        INSERT OR REPLACE INTO raw.offerbook_transactions
        (
            signature,
            program_id,
            slot,
            block_time,
            transaction_json,
            ingested_at
        )
        VALUES (
            ?,
            ?,
            ?,
            ?,
            CAST(? AS JSON),
            current_timestamp
        )
    """, [
        signature,
        PROGRAM_ID,
        transaction["slot"],
        transaction["blockTime"],
        json.dumps(transaction)
    ])

    time.sleep(0.2)


# --------------------------------------------------
# 6. Check result
# --------------------------------------------------

rows = con.execute("""
    SELECT
        signature,
        slot,
        block_time,
        ingested_at
    FROM raw.offerbook_transactions
    ORDER BY block_time DESC
""").fetchall()


print()
print("Transactions stored in DuckDB:")
print()

for row in rows:
    print(row)


con.close()