import duckdb

con = duckdb.connect("solana.duckdb")

row = con.execute("""
    select 
        signature,
        to_timestamp(block_time) AS block_timestamp,
        transaction_json->'meta'->'err' AS error,
        CAST(transaction_json->'meta'->>'fee' AS BIGINT) AS fee_lamports
    from raw.offerbook_transactions
    ORDER BY block_time DESC
""").fetchall()

print(row)

con.close()