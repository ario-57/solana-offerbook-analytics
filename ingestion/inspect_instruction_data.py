import duckdb
import base58
from collections import Counter


con = duckdb.connect("solana.duckdb")


rows = con.execute("""
    SELECT
        instruction_data
    FROM main.int_offerbook_instructions
""").fetchall()


discriminators = []


for (instruction_data,) in rows:

    decoded = base58.b58decode(instruction_data)

    discriminator = decoded[:8].hex()

    discriminators.append(discriminator)


counts = Counter(discriminators)


print("Offerbook instruction discriminators:")
print()

for discriminator, count in counts.most_common():
    print(discriminator, count)


con.close()