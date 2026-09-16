import duckdb

con = duckdb.connect("solana.duckdb")

rows = con.execute("""
    select
        instruction_name,
        account_role,
        account_value_type,
        count(*) as occurrences
    from main.int_offerbook_instruction_accounts
    where instruction_name = 'create_token_principal_offer'
    group by
        instruction_name,
        account_role,
        account_value_type
    order by account_role
""").fetchall()

for row in rows:
    print(row)

con.close()