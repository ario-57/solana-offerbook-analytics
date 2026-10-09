# Jupiter Offerbook Analytics

An end-to-end data engineering and analytics pipeline for **Jupiter Offerbook**, a lending protocol on Solana.

The project ingests raw transactions from Solana RPC, decodes Anchor instructions and events using the protocol's IDL, and transforms the data into analytics-ready models with **dbt and DuckDB/MotherDuck**.

It includes a Streamlit dashboard for exploring lending activity, loan lifecycles, market liquidity, lender and borrower performance, wallet retention, and protocol execution metrics such as compute-unit consumption and transaction fees.

**Tech stack:** Python, SQL, dbt, DuckDB, MotherDuck, Solana RPC, GitHub Actions, Streamlit.