from ingestion.db import get_connection

con = get_connection()

result = con.execute("""
    select

        (
            select count(*)
            from main.int_offerbook_instructions
            where instruction_data is not null
        ) as total_source,

        (
            select count(*)
            from decoded.offerbook_instructions
        ) as decoded_rows,

        (
            select count(*)
            from main.int_offerbook_instructions s
            inner join decoded.offerbook_instructions d
                on s.signature = d.signature
                and s.instruction_index = d.instruction_index
            where s.instruction_data is not null
        ) as matched_rows,

        (
            select count(*)
            from main.int_offerbook_instructions s
            left join decoded.offerbook_instructions d
                on s.signature = d.signature
                and s.instruction_index = d.instruction_index
            where s.instruction_data is not null
              and d.signature is null
        ) as missing_rows
""").fetchone()

print(f"Total source: {result[0]:,}")
print(f"Decoded table: {result[1]:,}")
print(f"Matched:       {result[2]:,}")
print(f"Missing:       {result[3]:,}")

con.close()