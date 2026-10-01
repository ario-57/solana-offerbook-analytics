import os
from pathlib import Path

import duckdb


# Project root:
#
# solana_analytics/
# ├── solana.duckdb
# └── ingestion/
#     └── db.py
#
# db.py -> ingestion -> solana_analytics
PROJECT_ROOT = Path(__file__).resolve().parents[1]

LOCAL_DB_PATH = PROJECT_ROOT / "solana.duckdb"

DEFAULT_MOTHERDUCK_DATABASE = "solana_offerbook"


def get_target() -> str:
    """
    Return the database environment we want to use.

    Supported values:
        dev  -> local DuckDB
        prod -> MotherDuck

    If DB_TARGET is not defined, default to dev.
    """

    target = os.getenv(
        "DB_TARGET",
        "dev",
    ).strip().lower()

    if target not in {"dev", "prod"}:
        raise ValueError(
            "DB_TARGET must be either 'dev' or 'prod'. "
            f"Received: {target!r}"
        )

    return target


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Create and return the correct DuckDB connection
    for the selected environment.
    """

    target = get_target()

    # --------------------------------------------------
    # Development
    # --------------------------------------------------

    if target == "dev":
        return duckdb.connect(
            str(LOCAL_DB_PATH)
        )

    # --------------------------------------------------
    # Production
    # --------------------------------------------------

    token = os.getenv("MOTHERDUCK_TOKEN")

    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN is required when "
            "DB_TARGET=prod."
        )

    database = os.getenv(
        "MOTHERDUCK_DATABASE",
        DEFAULT_MOTHERDUCK_DATABASE,
    )

    connection_string = (
        f"md:{database}"
        f"?motherduck_token={token}"
    )

    return duckdb.connect(
        connection_string
    )