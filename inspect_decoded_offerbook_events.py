import duckdb

con = duckdb.connect("solana.duckdb")

rows = con.execute("""
    select 
        event_name,
        decoded_event
    from decoded.offerbook_events
    where event_name in ('LoanCreated', 'LoanCreatedV1')
    limit 5
""").fetchall()

for event_name, decoded_event in rows:
    print()
    print("EVENT NAME:")
    print(event_name)

    print()
    print("DECODED EVENT:")
    print(decoded_event)

    print("-" * 80)

con.close()