import duckdb

con = duckdb.connect("solana.duckdb")

rows = con.execute("""
    select
        instruction_name,
        decoded_accounts
    from main.stg_offerbook_decoded_instructions
    where instruction_name = 'create_token_principal_offer'
    limit 3
""").fetchall()

for instruction_name, decoded_accounts in rows:
    print()
    print("INSTRUCTION:")
    print(instruction_name)

    print()
    print("DECODED ACCOUNTS:")
    print(decoded_accounts)

    print("-" * 80)

con.close()