import duckdb

con = duckdb.connect("solana.duckdb")

rows = con.execute("""
    select 
        instruction_name,
        account_role,
        account_value_type,
        account_value
    from main.int_offerbook_instruction_accounts
    where instruction_name = 'create_token_principal_offer'
    order by
        instruction_name,
        account_role
    limit 100
    """).fetchall()

for row in rows:
    print()
    print("INSTRUCTION:")
    print(row[0])

    print()
    print("ACCOUNT ROLE:")
    print(row[1])

    print()
    print("ACCOUNT VALUE TYPE:")
    print(row[2])

    print()
    print("ACCOUNT VALUE:")
    print(row[3])

    print("-" * 80)


con.close()