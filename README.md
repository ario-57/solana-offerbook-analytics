# Jupiter Offerbook Analytics

An end-to-end data engineering and analytics pipeline for **Jupiter Offerbook**, a lending protocol on Solana.

The project ingests raw transactions from Solana RPC, decodes Anchor instructions and events using the protocol's IDL, and transforms the data into analytics-ready models with **dbt and DuckDB/MotherDuck**.

It includes a Streamlit dashboard for exploring lending activity, loan lifecycles, market liquidity, lender and borrower performance, wallet retention, and protocol execution metrics such as compute-unit consumption and transaction fees.

**Tech stack:** Python, SQL, dbt, DuckDB, MotherDuck, Solana RPC, GitHub Actions, Streamlit.

**Dashboard:** https://jupiter-offerbook.streamlit.app/

## Architecture

The pipeline follows an ELT approach, combining Python-based ingestion and decoding with SQL transformations managed by dbt.

```text
Solana RPC
    |
    v
Python Ingestion (Incremental + Backfill)
    |
    v
Raw Transactions (DuckDB / MotherDuck)
    |
    v
dbt Staging + Instruction Extraction
    |
    v
Python IDL Decoder (Anchor Instructions + Events)
    |
    v
dbt Intermediate Models
    |  + Token Metadata & USD Price Enrichment
    v
dbt Analytics Marts
    |
    v
Streamlit Dashboard
```

**Main components:**

- **Ingestion:** Fetches historical and new Offerbook transactions from Solana RPC.
- **Decoding:** Parses Anchor/Borsh instructions and events using the protocol IDL.
- **Transformation:** Uses dbt to model loan and offer lifecycles, enrich token data, and calculate analytical metrics.
- **Storage:** DuckDB for local development and MotherDuck for production datasets.
- **Visualization:** Streamlit reads the prepared analytics marts.
- **Orchestration:** GitHub Actions runs the ingestion, decoding, transformation, and data-quality workflows.
