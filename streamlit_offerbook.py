"""Jupiter Offerbook analytics dashboard.

Read-only Streamlit app using existing MotherDuck/dbt marts.
Run from the solana_analytics project root:
    python -m streamlit run streamlit_offerbook.py
Environment: DB_TARGET=prod and MOTHERDUCK_TOKEN set securely.
"""

import pandas as pd
import streamlit as st

from ingestion.db import get_connection


# Streamlit executes the script again when a user changes a widget.
st.set_page_config(
    page_title="Jupiter Offerbook | Onchain Analytics",
    page_icon="📊",
    layout="wide",
)

WINDOWS = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "All history": None,
}


# ----------------------------------------------------------
# Database helpers
# ----------------------------------------------------------
def read_motherduck(query: str, parameters=None) -> pd.DataFrame:
    """Execute a read-only query, return a DataFrame, close the connection."""
    con = get_connection()
    try:
        return con.execute(query, parameters or []).fetchdf()
    finally:
        con.close()


def date_filter(days: int | None) -> tuple[str, list]:
    """Return a safe WHERE clause and bindings for a UTC calendar-day window."""
    if days is None:
        return "", []

    return (
        """
        where block_date >= (
            cast(current_timestamp at time zone 'UTC' as date)
            - (cast(? as integer) - 1)
        )
        """,
        [days],
    )


# ----------------------------------------------------------
# Cached queries. Each page loads only the marts it needs.
# ----------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def load_snapshot() -> pd.DataFrame:
    """Current lifetime loan-state snapshot; independent of the date filter."""
    return read_motherduck("""
        select
            snapshot_at,
            total_loans,
            active_loans,
            repaid_loans,
            defaulted_loans,
            past_due_open_loans,
            extended_loans,
            unique_users,
            total_origination_volume_usd,
            open_principal_at_origination_usd,
            price_coverage_pct
        from main.mart_offerbook_lifecycle_overview
        limit 1
    """)


@st.cache_data(ttl=600, show_spinner=False)
def load_daily_lifecycle(days: int | None) -> pd.DataFrame:
    """One record per UTC activity date: originations, repayments, defaults."""
    clause, parameters = date_filter(days)
    return read_motherduck(f"""
        select
            block_date,
            originated_loans,
            repaid_loans,
            defaulted_loans,
            unique_active_users,
            origination_volume_usd,
            repaid_principal_usd,
            realized_interest_usd,
            defaulted_principal_usd,
            priced_originations,
            unpriced_originations
        from main.mart_offerbook_daily_lifecycle
        {clause}
        order by block_date
    """, parameters)


@st.cache_data(ttl=600, show_spinner=False)
def load_daily_lending(days: int | None) -> pd.DataFrame:
    """Daily origination/lender/borrower statistics from the lending mart."""
    clause, parameters = date_filter(days)
    return read_motherduck(f"""
        select
            block_date,
            loan_count,
            filled_offer_count,
            unique_lenders,
            unique_borrowers,
            unique_users,
            token_pair_count,
            origination_volume_usd,
            priced_loan_count,
            unpriced_loan_count
        from main.mart_offerbook_daily_lending
        {clause}
        order by block_date
    """, parameters)


@st.cache_data(ttl=600, show_spinner=False)
def load_markets(days: int | None) -> pd.DataFrame:
    """Aggregate additive daily market metrics by stable asset-pair keys."""
    clause, parameters = date_filter(days)
    return read_motherduck(f"""
        select
            principal_asset_key,
            collateral_asset_key,
            max_by(market_name, block_date) as market_name,
            sum(originated_loans) as originated_loans,
            sum(repaid_loans) as repaid_loans,
            sum(defaulted_loans) as defaulted_loans,
            sum(origination_volume_usd) as origination_volume_usd,
            sum(priced_originations) as priced_originations,
            sum(unpriced_originations) as unpriced_originations
        from main.mart_offerbook_daily_markets
        {clause}
        group by principal_asset_key, collateral_asset_key
        order by originated_loans desc
    """, parameters)


@st.cache_data(ttl=600, show_spinner=False)
def load_activity(days: int | None) -> pd.DataFrame:
    """Fetch daily instruction counts by decoded instruction type."""
    clause, parameters = date_filter(days)
    return read_motherduck(f"""
        select block_date, instruction_name, instruction_count
        from main.mart_offerbook_daily_activity
        {clause}
        order by block_date, instruction_name
    """, parameters)


# ----------------------------------------------------------
# Presentation / numerical helpers
# ----------------------------------------------------------
def count_text(value) -> str:
    """Format counts without pretending an absent value is zero."""
    return "N/A" if pd.isna(value) else f"{int(value):,}"


def usd_text(value) -> str:
    """Compact USD formatting; NULL remains N/A when prices are missing."""
    if pd.isna(value):
        return "N/A"
    value = float(value)
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.2f}B"
    if absolute >= 1_000_000:
        return f"${value / 1_000_000:,.2f}M"
    if absolute >= 1_000:
        return f"${value / 1_000:,.1f}K"
    return f"${value:,.2f}"


def sum_count(df: pd.DataFrame, column: str) -> int:
    """Sum additive counts over the selected dates, handling no rows."""
    if df.empty:
        return 0
    return int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def sum_usd(df: pd.DataFrame, column: str):
    """Sum available USD values, preserving N/A if all values are missing."""
    if df.empty:
        return float("nan")
    return pd.to_numeric(df[column], errors="coerce").sum(min_count=1)


def coverage_text(priced: int, unpriced: int) -> str:
    """Share of loans with USD valuation, not share of USD volume covered."""
    total = priced + unpriced
    return "N/A" if total == 0 else f"{100 * priced / total:.1f}%"


def date_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DuckDB DATE results into Streamlit-friendly timestamps."""
    result = df.copy()
    result["block_date"] = pd.to_datetime(result["block_date"])
    return result


def chart_volume(daily: pd.DataFrame, amount_column: str, title: str):
    """Preserve unknown USD values; use zero only when there were no loans."""
    volume = date_ready(daily)
    volume[title] = pd.to_numeric(volume[amount_column], errors="coerce")
    if "originated_loans" in volume.columns and amount_column == "origination_volume_usd":
        volume.loc[volume["originated_loans"].eq(0), title] = 0
    st.bar_chart(volume, x="block_date", y=title)


# ----------------------------------------------------------
# Dashboard pages
# ----------------------------------------------------------
def render_overview(snapshot: pd.DataFrame, daily: pd.DataFrame, window: str):
    """Combine unfiltered lifetime KPIs with period-specific activity."""
    if snapshot.empty:
        st.warning("No lifecycle snapshot was returned by MotherDuck.")
        return

    s = snapshot.iloc[0]
    st.subheader("Lifetime protocol snapshot")
    a, b, c, d = st.columns(4)
    a.metric("Originated loans (lifetime)", count_text(s["total_loans"]))
    b.metric("Active loans (current)", count_text(s["active_loans"]))
    c.metric("Unique wallets (lifetime)", count_text(s["unique_users"]))
    d.metric("Priced origination USD (lifetime)", usd_text(s["total_origination_volume_usd"]))

    price_coverage = s["price_coverage_pct"]
    coverage_label = (
        f"{float(price_coverage):.1f}%" if pd.notna(price_coverage) else "N/A"
    )
    st.caption(
        "USD origination totals exclude loans without historical prices. "
        f"Lifetime loan price coverage: {coverage_label}."
    )

    st.subheader(f"Selected period · {window}")
    if daily.empty:
        st.info("No lifecycle activity was found in this period.")
        return

    priced = sum_count(daily, "priced_originations")
    unpriced = sum_count(daily, "unpriced_originations")
    active_days_avg = pd.to_numeric(daily["unique_active_users"], errors="coerce").mean()

    a, b, c, d = st.columns(4)
    a.metric("Originated loans", count_text(sum_count(daily, "originated_loans")))
    b.metric("Priced origination USD", usd_text(sum_usd(daily, "origination_volume_usd")))
    c.metric("Repaid loans", count_text(sum_count(daily, "repaid_loans")))
    d.metric("Avg wallets / activity day", count_text(round(active_days_avg)) if pd.notna(active_days_avg) else "N/A")

    st.caption(f"Origination price coverage in this period: {coverage_text(priced, unpriced)}. Average daily wallets does not represent period-unique wallets.")
    left, right = st.columns(2)

    with left:
        st.markdown("#### Daily loan events")
        chart = date_ready(daily).rename(columns={
            "originated_loans": "Originated",
            "repaid_loans": "Repaid",
            "defaulted_loans": "Defaulted",
        })
        st.line_chart(chart, x="block_date", y=["Originated", "Repaid", "Defaulted"])

    with right:
        st.markdown("#### Daily priced origination (USD)")
        chart_volume(daily, "origination_volume_usd", "Priced origination USD")

    st.markdown("#### Current loan position")
    a, b, c, d = st.columns(4)
    a.metric("Repaid loans", count_text(s["repaid_loans"]))
    b.metric("Defaulted loans", count_text(s["defaulted_loans"]))
    c.metric("Past-due open loans", count_text(s["past_due_open_loans"]))
    d.metric("Open principal at origination prices", usd_text(s["open_principal_at_origination_usd"]))
    st.caption("Open principal is valued at loan origination prices. It is not current marked-to-market TVL.")


def render_lending(daily: pd.DataFrame, window: str):
    """Show funded-loan activity and daily participant metrics."""
    st.subheader(f"Lending activity · {window}")
    if daily.empty:
        st.info("No funded loan originations were found in this period.")
        return

    priced = sum_count(daily, "priced_loan_count")
    unpriced = sum_count(daily, "unpriced_loan_count")
    avg_wallets = pd.to_numeric(daily["unique_users"], errors="coerce").mean()

    a, b, c, d = st.columns(4)
    a.metric("Funded loans", count_text(sum_count(daily, "loan_count")))
    b.metric("Priced origination USD", usd_text(sum_usd(daily, "origination_volume_usd")))
    c.metric("Sum of daily filled offers", count_text(sum_count(daily, "filled_offer_count")))
    d.metric("Avg wallets / lending day", count_text(round(avg_wallets)) if pd.notna(avg_wallets) else "N/A")

    st.caption(f"Loan-level price coverage: {coverage_text(priced, unpriced)}. Daily distinct-wallet counts are not additive across dates.")
    left, right = st.columns(2)
    with left:
        st.markdown("#### Funded loans per day")
        st.line_chart(date_ready(daily), x="block_date", y="loan_count")
    with right:
        st.markdown("#### Priced origination volume (USD)")
        temp = daily.rename(columns={"loan_count": "originated_loans"})
        chart_volume(temp, "origination_volume_usd", "Priced origination USD")

    st.markdown("#### Daily participation")
    participants = date_ready(daily).rename(columns={
        "unique_lenders": "Lenders",
        "unique_borrowers": "Borrowers",
        "unique_users": "Distinct wallets",
    })
    st.line_chart(participants, x="block_date", y=["Lenders", "Borrowers", "Distinct wallets"])
    st.caption("Each participation count is distinct within one date; lenders and borrowers may overlap.")


def render_lifecycle(snapshot: pd.DataFrame, daily: pd.DataFrame, window: str):
    """Separate current loan states from events occurring during the filter."""
    st.subheader("Current lifecycle state · lifetime snapshot")
    if not snapshot.empty:
        s = snapshot.iloc[0]
        a, b, c, d = st.columns(4)
        a.metric("Active", count_text(s["active_loans"]))
        b.metric("Repaid", count_text(s["repaid_loans"]))
        c.metric("Defaulted", count_text(s["defaulted_loans"]))
        d.metric("Extended at least once", count_text(s["extended_loans"]))

        breakdown = pd.DataFrame({
            "Loan status": ["Active", "Repaid", "Defaulted"],
            "Loans": [s["active_loans"], s["repaid_loans"], s["defaulted_loans"]],
        })
        st.bar_chart(breakdown, x="Loan status", y="Loans")

    st.subheader(f"Lifecycle events · {window}")
    if daily.empty:
        st.info("No lifecycle activity was found in this period.")
        return

    a, b, c, d = st.columns(4)
    a.metric("Originations", count_text(sum_count(daily, "originated_loans")))
    b.metric("Repayments", count_text(sum_count(daily, "repaid_loans")))
    c.metric("Defaults", count_text(sum_count(daily, "defaulted_loans")))
    d.metric("Realized interest (priced USD)", usd_text(sum_usd(daily, "realized_interest_usd")))

    activity = date_ready(daily).rename(columns={
        "originated_loans": "Originations",
        "repaid_loans": "Repayments",
        "defaulted_loans": "Defaults",
    })
    st.line_chart(activity, x="block_date", y=["Originations", "Repayments", "Defaults"])
    st.caption("USD values for origination, repayment, and default may use prices from different event times; do not net them as a cash-flow measure.")


def render_markets(markets: pd.DataFrame, window: str):
    """Show market rankings, using principal/collateral keys as identity."""
    st.subheader(f"Principal / collateral markets · {window}")
    if markets.empty:
        st.info("No market activity was found in this period.")
        return

    markets = markets.copy()
    priced = pd.to_numeric(markets["priced_originations"], errors="coerce").fillna(0)
    unpriced = pd.to_numeric(markets["unpriced_originations"], errors="coerce").fillna(0)
    denom = priced + unpriced
    markets["price_coverage_pct"] = (100 * priced / denom.where(denom.ne(0))).round(1)

    # Market names are descriptive. Stable keys prevent aggregating
    # two distinct mint pairs with the same human-readable symbols.
    markets["market_label"] = markets["market_name"].fillna("Unknown market").astype(str)
    repeated = markets["market_label"].duplicated(keep=False)
    markets.loc[repeated, "market_label"] = markets.loc[repeated].apply(
        lambda r: (
            f"{r['market_label']} "
            f"({str(r['principal_asset_key'])[:6]}/{str(r['collateral_asset_key'])[:6]})"
        ),
        axis=1,
    )

    a, b, c, d = st.columns(4)
    a.metric("Active market pairs in period", count_text(len(markets)))
    b.metric("Originated loans", count_text(sum_count(markets, "originated_loans")))
    c.metric("Priced origination USD", usd_text(sum_usd(markets, "origination_volume_usd")))
    d.metric("Origination price coverage", coverage_text(int(priced.sum()), int(unpriced.sum())))

    left, right = st.columns(2)
    with left:
        st.markdown("#### Top markets by originated loans")
        top = markets.sort_values("originated_loans", ascending=False).head(10)
        st.bar_chart(top, x="market_label", y="originated_loans")
    with right:
        st.markdown("#### Top markets by priced USD originations")
        priced_markets = markets[priced.gt(0)].dropna(subset=["origination_volume_usd"])
        top = priced_markets.sort_values("origination_volume_usd", ascending=False).head(10)
        if top.empty:
            st.info("No historical USD prices are available for this period.")
        else:
            st.bar_chart(top, x="market_label", y="origination_volume_usd")

    st.markdown("#### Market leaderboard")
    table = markets[[
        "market_name", "originated_loans", "repaid_loans", "defaulted_loans",
        "origination_volume_usd", "price_coverage_pct",
        "principal_asset_key", "collateral_asset_key",
    ]].rename(columns={
        "market_name": "Market",
        "originated_loans": "Originations",
        "repaid_loans": "Repayments",
        "defaulted_loans": "Defaults",
        "origination_volume_usd": "Priced origination USD",
        "price_coverage_pct": "Priced originations (%)",
        "principal_asset_key": "Principal key",
        "collateral_asset_key": "Collateral key",
    })
    st.dataframe(table, hide_index=True, use_container_width=True)
    st.download_button(
        "Download market data (CSV)",
        data=markets.to_csv(index=False).encode("utf-8"),
        file_name="offerbook_market_summary.csv",
        mime="text/csv",
    )
    st.caption("Market totals aggregate additive daily events, not outstanding positions. Distinct daily lender counts are deliberately not summed across dates.")


def render_activity(activity: pd.DataFrame, window: str):
    """Summarize program-instruction activity without double-counting transactions."""
    st.subheader(f"Protocol instruction activity · {window}")
    if activity.empty:
        st.info("No instruction activity was found in this period.")
        return

    total = sum_count(activity, "instruction_count")
    unknown = sum_count(
        activity[activity["instruction_name"].eq("unknown")],
        "instruction_count",
    )
    a, b, c = st.columns(3)
    a.metric("Instructions", count_text(total))
    b.metric("Instruction categories", count_text(activity["instruction_name"].nunique()))
    c.metric("Unclassified instructions", count_text(unknown))

    daily = activity.groupby("block_date", as_index=False)["instruction_count"].sum()
    st.markdown("#### Instructions per day")
    st.line_chart(date_ready(daily), x="block_date", y="instruction_count")

    rankings = (
        activity.groupby("instruction_name", as_index=False)["instruction_count"]
        .sum()
        .sort_values("instruction_count", ascending=False)
    )
    left, right = st.columns(2)
    with left:
        st.markdown("#### Top instruction types")
        st.bar_chart(rankings.head(12), x="instruction_name", y="instruction_count")
    with right:
        st.markdown("#### Full instruction breakdown")
        st.dataframe(rankings, hide_index=True, use_container_width=True)

    st.caption(
        "Do not sum per-instruction-type transaction_count to estimate unique "
        "protocol transactions: the same signature can contain multiple types. "
        "This page counts instructions instead."
    )


# ----------------------------------------------------------
# Main Streamlit controller
# ----------------------------------------------------------
def main():
    """Render sidebar, fetch only the selected page's data, and show charts."""
    st.title("Jupiter Offerbook | Onchain Analytics")
    st.caption("MotherDuck + dbt marts · UTC calendar-day filters · read-only dashboard")

    st.sidebar.header("Explore Offerbook")
    page = st.sidebar.radio(
        "Section",
        ["Overview", "Lending", "Loan Lifecycle", "Markets", "Protocol Activity"],
    )
    label = st.sidebar.selectbox("Time window", list(WINDOWS), index=1)
    days = WINDOWS[label]

    if st.sidebar.button("Refresh MotherDuck data"):
        for loader in (
            load_snapshot, load_daily_lifecycle, load_daily_lending,
            load_markets, load_activity,
        ):
            loader.clear()

    st.sidebar.caption("Results are cached for 10 minutes. Lifetime snapshot KPIs do not change when the date window changes.")

    try:
        with st.spinner("Loading Offerbook analytics from MotherDuck..."):
            if page == "Overview":
                render_overview(load_snapshot(), load_daily_lifecycle(days), label)
            elif page == "Lending":
                render_lending(load_daily_lending(days), label)
            elif page == "Loan Lifecycle":
                render_lifecycle(load_snapshot(), load_daily_lifecycle(days), label)
            elif page == "Markets":
                render_markets(load_markets(days), label)
            else:
                render_activity(load_activity(days), label)
    except Exception as exc:
        st.error("Unable to load this page. Check your MotherDuck connection and that the dbt marts exist.")
        st.caption(f"Error type: {type(exc).__name__}")
        st.stop()


if __name__ == "__main__":
    main()
