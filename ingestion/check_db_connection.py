from ingestion.db import (
    get_connection,
    get_target,
)


def main():
    con = get_connection()

    try:
        current_database = con.execute(
            "select current_database()"
        ).fetchone()[0]

        transaction_count = con.execute(
            """
            select count(*)
            from raw.offerbook_transactions
            """
        ).fetchone()[0]

        print(
            f"DB_TARGET: {get_target()}"
        )

        print(
            f"Current database: {current_database}"
        )

        print(
            f"Offerbook transactions: {transaction_count}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    main()