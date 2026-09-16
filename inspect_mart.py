import duckdb

con = duckdb.connect("solana.duckdb")

rows = con.execute("""
    select *
    from main.mart_offerbook_daily_activity
    order by block_date desc, instruction_count desc
""").fetchall()

for row in rows:
    print(row)

con.close()