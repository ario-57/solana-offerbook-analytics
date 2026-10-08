"""Jupiter Offerbook analytics dashboard.

Read-only Streamlit app using existing MotherDuck/dbt marts.
Run from the solana_analytics project root:
    python -m streamlit run streamlit_offerbook.py
Environment: DB_TARGET=prod and MOTHERDUCK_TOKEN set securely.
"""

import html

import altair as alt
import pandas as pd
import streamlit as st

from ingestion.db import get_connection


# Streamlit executes the script again when a user changes a widget.
st.set_page_config(
    page_title="Jupiter Offerbook | Onchain Analytics",
    page_icon="📊",
    layout="wide",
)


st.markdown(
    """
    <style>
    .block-container {
        max-width: 1440px;
        padding-top: 2.2rem;
        padding-bottom: 3rem;
    }

    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.85rem;
        margin: 0.35rem 0 1rem 0;
    }

    .kpi-card {
        min-width: 0;
        padding: 0.95rem 1rem 0.9rem;
        border: 1px solid rgba(128, 128, 128, 0.22);
        border-radius: 0.8rem;
        background: rgba(128, 128, 128, 0.045);
    }

    .kpi-label {
        font-size: 0.86rem;
        line-height: 1.25;
        opacity: 0.72;
        margin-bottom: 0.45rem;
        min-height: 2.15em;
    }

    .kpi-value {
        font-size: 1.72rem;
        line-height: 1.1;
        font-weight: 650;
        letter-spacing: -0.02em;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }

    .dashboard-hero {
        margin: 0 0 0.2rem 0;
    }

    .dashboard-hero h1 {
        margin: 0;
        font-size: 2.55rem;
        line-height: 1.08;
        letter-spacing: -0.035em;
    }

    .dashboard-hero p {
        margin: 0.3rem 0 0;
        font-size: 1.05rem;
        opacity: 0.7;
    }

    @media (max-width: 768px) {
        .block-container {
            padding-top: 1rem !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
            padding-bottom: 2rem !important;
        }

        .dashboard-hero h1 {
            font-size: 2rem !important;
            line-height: 1.05 !important;
            white-space: nowrap;
        }

        .dashboard-hero p {
            font-size: 0.95rem;
            margin-top: 0.2rem;
        }

        h2 {
            font-size: 1.48rem !important;
            line-height: 1.2 !important;
            margin-top: 1.35rem !important;
        }

        h3 {
            font-size: 1.22rem !important;
            line-height: 1.25 !important;
        }

        h4 {
            font-size: 1.08rem !important;
            line-height: 1.25 !important;
        }

        .kpi-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.65rem;
            margin-bottom: 0.8rem;
        }

        .kpi-card {
            padding: 0.78rem 0.75rem 0.72rem;
            border-radius: 0.7rem;
        }

        .kpi-label {
            font-size: 0.76rem;
            margin-bottom: 0.35rem;
            min-height: 2.35em;
        }

        .kpi-value {
            font-size: 1.42rem;
        }

        [data-testid="stVerticalBlock"] {
            gap: 0.65rem;
        }

        [data-testid="stVegaLiteChart"] {
            width: 100% !important;
        }

        [data-testid="stCaptionContainer"] {
            font-size: 0.78rem;
            line-height: 1.35;
        }

        [data-testid="stDataFrame"] {
            overflow-x: auto;
        }
    }

    @media (max-width: 380px) {
        .dashboard-hero h1 {
            font-size: 1.78rem !important;
        }

        .kpi-value {
            font-size: 1.28rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
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



@st.cache_data(ttl=600, show_spinner=False)
def load_offer_efficiency(days: int | None) -> pd.DataFrame:
    """Offer-creation cohorts by market and creator role."""
    clause, parameters = date_filter(days)
    return read_motherduck(f"""
        select
            block_date,
            principal_asset_key,
            collateral_asset_key,
            creator_role,
            market_name,
            offers_created,
            offers_with_fill,
            fulfilled_offers,
            partially_filled_offers,
            cancelled_offers,
            expired_offers,
            active_offers,
            stale_7d_offers,
            stale_30d_offers,
            unique_offer_creators,
            loans_created_from_offers,
            avg_principal_fill_ratio,
            avg_seconds_to_first_fill,
            avg_apy_raw,
            avg_duration_raw
        from main.mart_offerbook_offer_efficiency_daily
        {clause}
        order by block_date, offers_created desc
    """, parameters)


@st.cache_data(ttl=600, show_spinner=False)
def load_market_liquidity_snapshot() -> pd.DataFrame:
    """Current open-offer staleness and terms by market and creator role."""
    return read_motherduck("""
        select
            snapshot_at,
            principal_asset_key,
            collateral_asset_key,
            creator_role,
            market_name,
            open_offers,
            active_offer_creators,
            stale_7d_offers,
            stale_30d_offers,
            median_open_offer_age_seconds,
            p90_open_offer_age_seconds,
            median_open_apy_raw,
            median_open_duration_raw,
            stale_7d_offer_pct,
            stale_30d_offer_pct
        from main.mart_offerbook_market_liquidity_snapshot
        order by open_offers desc
    """)


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



def elapsed_text(seconds) -> str:
    """Format elapsed seconds for offer fill/age metrics."""
    if pd.isna(seconds):
        return "N/A"
    seconds = float(seconds)
    if seconds < 3_600:
        return f"{seconds / 60:,.0f} min"
    if seconds < 86_400:
        return f"{seconds / 3_600:,.1f} h"
    return f"{seconds / 86_400:,.1f} days"


def render_kpi_grid(items: list[tuple[str, str]]) -> None:
    """Render responsive KPI cards: four-up desktop, two-up mobile."""
    cards = []
    for label, value in items:
        safe_label = html.escape(str(label))
        safe_value = html.escape(str(value))
        cards.append(
            "<div class=\"kpi-card\">"
            f"<div class=\"kpi-label\">{safe_label}</div>"
            f"<div class=\"kpi-value\">{safe_value}</div>"
            "</div>"
        )

    st.markdown(
        '<div class="kpi-grid">' + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


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


def percent_text(value, decimals: int = 1) -> str:
    """Format percentage-point values consistently."""
    return "N/A" if pd.isna(value) else f"{float(value):,.{decimals}f}%"


def apy_text(raw_apy) -> str:
    """Offerbook APY is stored in basis-point-like raw units: raw / 100 = %."""
    return "N/A" if pd.isna(raw_apy) else f"{float(raw_apy) / 100:,.2f}%"


def compact_number_text(value, decimals: int = 1) -> str:
    """Compact generic numbers for chart labels."""
    if pd.isna(value):
        return "N/A"
    value = float(value)
    absolute = abs(value)
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:,.{decimals}f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:,.{decimals}f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:,.{decimals}f}K"
    if value.is_integer():
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def _chart_axis(value_kind: str, title: str | None = None):
    """Create consistent compact axes for counts, USD, percentages, and raw values."""
    if value_kind == "usd":
        return alt.Axis(
            title=title,
            labelExpr="'$' + format(datum.value, '~s')",
            gridOpacity=0.18,
        )
    if value_kind == "percent":
        return alt.Axis(
            title=title,
            labelExpr="format(datum.value, '.1f') + '%'",
            gridOpacity=0.18,
        )
    if value_kind == "count":
        return alt.Axis(
            title=title,
            format="~s",
            tickMinStep=1,
            gridOpacity=0.18,
        )
    return alt.Axis(title=title, gridOpacity=0.18)


def _tooltip_format(value_kind: str) -> str:
    """Return Vega-Lite numeric tooltip format strings."""
    if value_kind == "usd":
        return "$,.2f"
    if value_kind == "percent":
        return ",.1f"
    if value_kind == "count":
        return ",.0f"
    return ",.2f"


def _display_value(value, value_kind: str) -> str:
    """Return a compact label for values printed directly on bar charts."""
    if value_kind == "usd":
        return usd_text(value)
    if value_kind == "percent":
        return percent_text(value)
    if value_kind == "count":
        return count_text(value)
    return compact_number_text(value)


def time_series_chart(
    df: pd.DataFrame,
    y_columns: list[str],
    *,
    value_kind: str = "count",
    y_title: str | None = None,
    mark: str = "line",
):
    """Render a polished interactive time series with shared-date hover."""
    if df.empty:
        return

    data = date_ready(df)
    for column in y_columns:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    long = data.melt(
        id_vars=["block_date"],
        value_vars=y_columns,
        var_name="Series",
        value_name="Value",
    ).dropna(subset=["Value"])

    if long.empty:
        st.info("No values are available for this chart.")
        return

    nearest = alt.selection_point(
        nearest=True,
        on="pointerover",
        fields=["block_date"],
        empty=False,
        clear="pointerout",
    )

    x_encoding = alt.X(
        "block_date:T",
        title=None,
        axis=alt.Axis(
            format="%b %d",
            labelAngle=0,
            labelPadding=8,
            tickSize=4,
            grid=False,
        ),
    )

    y_encoding = alt.Y(
        "Value:Q",
        title=y_title,
        axis=_chart_axis(value_kind, y_title),
    )

    color_encoding = alt.Color(
        "Series:N",
        title=None,
        legend=(
            alt.Legend(
                orient="top",
                direction="horizontal",
                columns=3,
                symbolSize=70,
                labelLimit=180,
                offset=6,
            )
            if len(y_columns) > 1
            else None
        ),
    )

    base = alt.Chart(long).encode(
        x=x_encoding,
        y=y_encoding,
        color=color_encoding,
    )

    shared_tooltip = [
        alt.Tooltip("block_date:T", title="Date", format="%b %d, %Y"),
    ]
    for column in y_columns:
        shared_tooltip.append(
            alt.Tooltip(
                f"{column}:Q",
                title=column,
                format=_tooltip_format(value_kind),
            )
        )

    selector = (
        alt.Chart(data)
        .mark_point(opacity=0, size=1000)
        .encode(
            x=x_encoding,
            tooltip=shared_tooltip,
        )
        .add_params(nearest)
    )

    rule = (
        alt.Chart(data)
        .mark_rule(strokeWidth=1)
        .encode(
            x=x_encoding,
            opacity=alt.condition(nearest, alt.value(0.5), alt.value(0)),
        )
        .transform_filter(nearest)
    )

    if mark == "bar":
        bars = base.mark_bar(
            cornerRadiusTopLeft=3,
            cornerRadiusTopRight=3,
        ).encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0.78)),
        )
        chart = bars + selector + rule
    else:
        lines = base.mark_line(
            strokeWidth=2.4,
            interpolate="monotone",
        )

        points = base.mark_point(
            filled=True,
            size=72,
            strokeWidth=1.5,
        ).encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
        ).transform_filter(nearest)

        chart = lines + selector + points + rule

    st.altair_chart(
        chart.properties(height=300).configure_view(stroke=None),
        use_container_width=True,
    )


def labeled_bar_chart(
    df: pd.DataFrame,
    category: str,
    value: str,
    *,
    value_kind: str = "count",
    axis_title: str | None = None,
    horizontal: bool = True,
):
    """Render bars with direct labels and polished hover highlighting."""
    if df.empty:
        st.info("No values are available for this chart.")
        return

    data = df.copy()
    data[value] = pd.to_numeric(data[value], errors="coerce")
    data = data.dropna(subset=[category, value])

    if data.empty:
        st.info("No values are available for this chart.")
        return

    data["display_value"] = data[value].map(
        lambda x: _display_value(x, value_kind)
    )

    hover = alt.selection_point(
        fields=[category],
        on="pointerover",
        empty=False,
        clear="pointerout",
    )

    tooltip = [
        alt.Tooltip(f"{category}:N", title="Category"),
        alt.Tooltip(
            f"{value}:Q",
            title=axis_title or value,
            format=_tooltip_format(value_kind),
        ),
    ]

    if horizontal:
        base = alt.Chart(data).encode(
            y=alt.Y(
                f"{category}:N",
                sort="-x",
                title=None,
                axis=alt.Axis(
                    labelLimit=220,
                    labelPadding=8,
                    ticks=False,
                    domain=False,
                ),
            ),
            x=alt.X(
                f"{value}:Q",
                title=axis_title,
                axis=_chart_axis(value_kind, axis_title),
            ),
        )

        bars = base.mark_bar(cornerRadiusEnd=4).encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0.78)),
            tooltip=tooltip,
        ).add_params(hover)

        labels = base.mark_text(
            align="left",
            baseline="middle",
            dx=5,
            fontSize=12,
            fontWeight=500,
        ).encode(
            text=alt.Text("display_value:N"),
            opacity=alt.condition(hover, alt.value(1), alt.value(0.82)),
        )

    else:
        base = alt.Chart(data).encode(
            x=alt.X(
                f"{category}:N",
                sort="-y",
                title=None,
                axis=alt.Axis(
                    labelAngle=0,
                    labelPadding=8,
                    ticks=False,
                    domain=False,
                ),
            ),
            y=alt.Y(
                f"{value}:Q",
                title=axis_title,
                axis=_chart_axis(value_kind, axis_title),
            ),
        )

        bars = base.mark_bar(
            cornerRadiusTopLeft=4,
            cornerRadiusTopRight=4,
        ).encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0.78)),
            tooltip=tooltip,
        ).add_params(hover)

        labels = base.mark_text(
            align="center",
            baseline="bottom",
            dy=-5,
            fontSize=12,
            fontWeight=500,
        ).encode(
            text=alt.Text("display_value:N"),
            opacity=alt.condition(hover, alt.value(1), alt.value(0.82)),
        )

    st.altair_chart(
        (bars + labels).properties(height=320).configure_view(stroke=None),
        use_container_width=True,
    )


def chart_volume(daily: pd.DataFrame, amount_column: str, title: str):
    """Daily USD chart with compact axis and exact-value hover tooltips."""
    volume = date_ready(daily)
    volume[title] = pd.to_numeric(volume[amount_column], errors="coerce")

    if (
        "originated_loans" in volume.columns
        and amount_column == "origination_volume_usd"
    ):
        volume.loc[volume["originated_loans"].eq(0), title] = 0

    time_series_chart(
        volume,
        [title],
        value_kind="usd",
        y_title="USD",
        mark="bar",
    )


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
    render_kpi_grid([
        ("Originated loans", count_text(s["total_loans"])),
        ("Active loans", count_text(s["active_loans"])),
        ("Unique wallets", count_text(s["unique_users"])),
        ("Priced origination USD", usd_text(s["total_origination_volume_usd"])),
    ])

    price_coverage = s["price_coverage_pct"]
    coverage_label = (
        percent_text(price_coverage) if pd.notna(price_coverage) else "N/A"
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

    render_kpi_grid([
        ("Originated loans", count_text(sum_count(daily, "originated_loans"))),
        ("Priced origination USD", usd_text(sum_usd(daily, "origination_volume_usd"))),
        ("Repaid loans", count_text(sum_count(daily, "repaid_loans"))),
        ("Avg wallets / day", count_text(round(active_days_avg)) if pd.notna(active_days_avg) else "N/A"),
    ])

    st.caption(f"Origination price coverage in this period: {coverage_text(priced, unpriced)}. Average daily wallets does not represent period-unique wallets.")
    left, right = st.columns(2)

    with left:
        st.markdown("#### Daily loan events")
        chart = date_ready(daily).rename(columns={
            "originated_loans": "Originated",
            "repaid_loans": "Repaid",
            "defaulted_loans": "Defaulted",
        })
        time_series_chart(
            chart,
            ["Originated", "Repaid", "Defaulted"],
            value_kind="count",
            y_title="Loans",
        )

    with right:
        st.markdown("#### Daily priced origination (USD)")
        chart_volume(daily, "origination_volume_usd", "Priced origination USD")

    st.markdown("#### Current loan position")
    render_kpi_grid([
        ("Repaid loans", count_text(s["repaid_loans"])),
        ("Defaulted loans", count_text(s["defaulted_loans"])),
        ("Past-due open", count_text(s["past_due_open_loans"])),
        ("Open principal", usd_text(s["open_principal_at_origination_usd"])),
    ])
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

    render_kpi_grid([
        ("Funded loans", count_text(sum_count(daily, "loan_count"))),
        ("Priced origination USD", usd_text(sum_usd(daily, "origination_volume_usd"))),
        ("Daily filled offers", count_text(sum_count(daily, "filled_offer_count"))),
        ("Avg wallets / day", count_text(round(avg_wallets)) if pd.notna(avg_wallets) else "N/A"),
    ])

    st.caption(f"Loan-level price coverage: {coverage_text(priced, unpriced)}. Daily distinct-wallet counts are not additive across dates.")
    left, right = st.columns(2)
    with left:
        st.markdown("#### Funded loans per day")
        time_series_chart(
            daily.rename(columns={"loan_count": "Funded loans"}),
            ["Funded loans"],
            value_kind="count",
            y_title="Loans",
        )
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
    time_series_chart(
        participants,
        ["Lenders", "Borrowers", "Distinct wallets"],
        value_kind="count",
        y_title="Wallets",
    )
    st.caption("Each participation count is distinct within one date; lenders and borrowers may overlap.")


def render_lifecycle(snapshot: pd.DataFrame, daily: pd.DataFrame, window: str):
    """Separate current loan states from events occurring during the filter."""
    st.subheader("Current lifecycle state · lifetime snapshot")
    if not snapshot.empty:
        s = snapshot.iloc[0]
        render_kpi_grid([
            ("Active", count_text(s["active_loans"])),
            ("Repaid", count_text(s["repaid_loans"])),
            ("Defaulted", count_text(s["defaulted_loans"])),
            ("Extended", count_text(s["extended_loans"])),
        ])

        breakdown = pd.DataFrame({
            "Loan status": ["Active", "Repaid", "Defaulted"],
            "Loans": [s["active_loans"], s["repaid_loans"], s["defaulted_loans"]],
        })
        labeled_bar_chart(
            breakdown,
            "Loan status",
            "Loans",
            value_kind="count",
            axis_title="Loans",
            horizontal=False,
        )

    st.subheader(f"Lifecycle events · {window}")
    if daily.empty:
        st.info("No lifecycle activity was found in this period.")
        return

    render_kpi_grid([
        ("Originations", count_text(sum_count(daily, "originated_loans"))),
        ("Repayments", count_text(sum_count(daily, "repaid_loans"))),
        ("Defaults", count_text(sum_count(daily, "defaulted_loans"))),
        ("Realized interest", usd_text(sum_usd(daily, "realized_interest_usd"))),
    ])

    activity = date_ready(daily).rename(columns={
        "originated_loans": "Originations",
        "repaid_loans": "Repayments",
        "defaulted_loans": "Defaults",
    })
    time_series_chart(
        activity,
        ["Originations", "Repayments", "Defaults"],
        value_kind="count",
        y_title="Loans",
    )
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

    render_kpi_grid([
        ("Active market pairs", count_text(len(markets))),
        ("Originated loans", count_text(sum_count(markets, "originated_loans"))),
        ("Priced origination USD", usd_text(sum_usd(markets, "origination_volume_usd"))),
        ("Price coverage", coverage_text(int(priced.sum()), int(unpriced.sum()))),
    ])

    left, right = st.columns(2)
    with left:
        st.markdown("#### Top markets by originated loans")
        top = markets.sort_values("originated_loans", ascending=False).head(10)
        labeled_bar_chart(
            top,
            "market_label",
            "originated_loans",
            value_kind="count",
            axis_title="Loans",
        )
    with right:
        st.markdown("#### Top markets by priced USD originations")
        priced_markets = markets[priced.gt(0)].dropna(subset=["origination_volume_usd"])
        top = priced_markets.sort_values("origination_volume_usd", ascending=False).head(10)
        if top.empty:
            st.info("No historical USD prices are available for this period.")
        else:
            labeled_bar_chart(
                top,
                "market_label",
                "origination_volume_usd",
                value_kind="usd",
                axis_title="Priced origination USD",
            )

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
    st.dataframe(
        table,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Originations": st.column_config.NumberColumn(format="%d"),
            "Repayments": st.column_config.NumberColumn(format="%d"),
            "Defaults": st.column_config.NumberColumn(format="%d"),
            "Priced origination USD": st.column_config.NumberColumn(format="$%.2f"),
            "Priced originations (%)": st.column_config.NumberColumn(format="%.1f%%"),
        },
    )
    st.download_button(
        "Download market data (CSV)",
        data=markets.to_csv(index=False).encode("utf-8"),
        file_name="offerbook_market_summary.csv",
        mime="text/csv",
    )
    st.caption("Market totals aggregate additive daily events, not outstanding positions. Distinct daily lender counts are deliberately not summed across dates.")



def render_offer_insights(efficiency: pd.DataFrame, liquidity: pd.DataFrame, window: str):
    """User-facing offer quality plus developer-facing matching friction."""
    st.subheader(f"Offer Insights · {window}")
    st.caption(
        "Offer analytics are token-native and do not require additional Birdeye pricing. "
        "The selected period applies to offer creation cohorts; open-liquidity metrics are current."
    )

    role_label = st.selectbox(
        "Offer creator side",
        ["All creators", "Lender-side offers", "Borrower-side offers"],
        index=0,
    )
    role_map = {
        "All creators": None,
        "Lender-side offers": "lender",
        "Borrower-side offers": "borrower",
    }
    role = role_map[role_label]

    eff = efficiency.copy()
    liq = liquidity.copy()
    if role is not None:
        eff = eff[eff["creator_role"].eq(role)].copy()
        liq = liq[liq["creator_role"].eq(role)].copy()

    st.markdown("### User view")
    if eff.empty:
        st.info("No offers were created for this side in the selected period.")
    else:
        for column in [
            "offers_created", "offers_with_fill", "fulfilled_offers",
            "cancelled_offers", "expired_offers", "loans_created_from_offers",
            "avg_seconds_to_first_fill", "avg_apy_raw", "avg_duration_raw",
        ]:
            eff[column] = pd.to_numeric(eff[column], errors="coerce")

        offers = sum_count(eff, "offers_created")
        with_fill = sum_count(eff, "offers_with_fill")
        fulfilled = sum_count(eff, "fulfilled_offers")
        cancelled = sum_count(eff, "cancelled_offers")
        expired = sum_count(eff, "expired_offers")

        fill_rate = 100 * with_fill / offers if offers else float("nan")
        full_fill_rate = 100 * fulfilled / offers if offers else float("nan")
        cancel_rate = 100 * cancelled / offers if offers else float("nan")
        expiry_rate = 100 * expired / offers if offers else float("nan")

        valid_fill = eff["offers_with_fill"].fillna(0).gt(0) & eff["avg_seconds_to_first_fill"].notna()
        if valid_fill.any():
            avg_fill_seconds = (
                eff.loc[valid_fill, "avg_seconds_to_first_fill"]
                * eff.loc[valid_fill, "offers_with_fill"]
            ).sum() / eff.loc[valid_fill, "offers_with_fill"].sum()
        else:
            avg_fill_seconds = float("nan")

        valid_apy = eff["offers_created"].fillna(0).gt(0) & eff["avg_apy_raw"].notna()
        if valid_apy.any():
            avg_apy_raw = (
                eff.loc[valid_apy, "avg_apy_raw"]
                * eff.loc[valid_apy, "offers_created"]
            ).sum() / eff.loc[valid_apy, "offers_created"].sum()
        else:
            avg_apy_raw = float("nan")

        render_kpi_grid([
            ("Offers created", count_text(offers)),
            ("Reached a fill", percent_text(fill_rate)),
            ("Fully fulfilled", percent_text(full_fill_rate)),
            ("Avg time to first fill", elapsed_text(avg_fill_seconds)),
            ("Average offered APY", apy_text(avg_apy_raw)),
            ("Cancelled", percent_text(cancel_rate)),
            ("Expired", percent_text(expiry_rate)),
        ])

        daily = (
            eff.groupby("block_date", as_index=False)[
                ["offers_created", "offers_with_fill", "fulfilled_offers",
                 "cancelled_offers", "expired_offers"]
            ]
            .sum()
            .sort_values("block_date")
        )
        daily["fill_rate_pct"] = (
            100 * daily["offers_with_fill"]
            / daily["offers_created"].where(daily["offers_created"].ne(0))
        )

        left, right = st.columns(2)
        with left:
            st.markdown("#### Offer outcomes by creation date")
            chart = date_ready(daily).rename(columns={
                "offers_created": "Created",
                "offers_with_fill": "Reached a fill",
                "fulfilled_offers": "Fulfilled",
                "cancelled_offers": "Cancelled",
                "expired_offers": "Expired",
            })
            time_series_chart(
                chart,
                ["Created", "Reached a fill", "Fulfilled", "Cancelled", "Expired"],
                value_kind="count",
                y_title="Offers",
            )

        with right:
            st.markdown("#### Fill rate by creation date")
            time_series_chart(
                daily.rename(columns={"fill_rate_pct": "Fill rate"}),
                ["Fill rate"],
                value_kind="percent",
                y_title="Fill rate",
            )

        market = (
            eff.groupby(
                ["principal_asset_key", "collateral_asset_key", "market_name"],
                as_index=False,
            )
            .agg(
                offers_created=("offers_created", "sum"),
                offers_with_fill=("offers_with_fill", "sum"),
                fulfilled_offers=("fulfilled_offers", "sum"),
                cancelled_offers=("cancelled_offers", "sum"),
                expired_offers=("expired_offers", "sum"),
                loans_created=("loans_created_from_offers", "sum"),
            )
        )
        market["fill_rate_pct"] = (
            100 * market["offers_with_fill"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )
        market["cancel_rate_pct"] = (
            100 * market["cancelled_offers"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )
        market["expiry_rate_pct"] = (
            100 * market["expired_offers"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )

        st.markdown("#### Market fill performance")
        market_table = market.sort_values("offers_created", ascending=False).rename(columns={
            "market_name": "Market",
            "offers_created": "Offers",
            "offers_with_fill": "Reached fill",
            "fulfilled_offers": "Fulfilled",
            "cancelled_offers": "Cancelled",
            "expired_offers": "Expired",
            "loans_created": "Loans created",
            "fill_rate_pct": "Fill rate (%)",
            "cancel_rate_pct": "Cancel rate (%)",
            "expiry_rate_pct": "Expiry rate (%)",
        })
        st.dataframe(
            market_table[[
                "Market", "Offers", "Reached fill", "Fulfilled", "Cancelled",
                "Expired", "Loans created", "Fill rate (%)", "Cancel rate (%)",
                "Expiry rate (%)",
            ]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "Offers": st.column_config.NumberColumn(format="%d"),
                "Reached fill": st.column_config.NumberColumn(format="%d"),
                "Fulfilled": st.column_config.NumberColumn(format="%d"),
                "Cancelled": st.column_config.NumberColumn(format="%d"),
                "Expired": st.column_config.NumberColumn(format="%d"),
                "Loans created": st.column_config.NumberColumn(format="%d"),
                "Fill rate (%)": st.column_config.NumberColumn(format="%.1f%%"),
                "Cancel rate (%)": st.column_config.NumberColumn(format="%.1f%%"),
                "Expiry rate (%)": st.column_config.NumberColumn(format="%.1f%%"),
            },
        )

    st.markdown("### Developer view")
    if liq.empty:
        st.info("No currently open offers were found for this side.")
    else:
        for column in [
            "open_offers", "stale_7d_offers", "stale_30d_offers",
            "median_open_offer_age_seconds", "stale_7d_offer_pct",
            "stale_30d_offer_pct",
        ]:
            liq[column] = pd.to_numeric(liq[column], errors="coerce")

        open_offers = sum_count(liq, "open_offers")
        stale7 = sum_count(liq, "stale_7d_offers")
        stale30 = sum_count(liq, "stale_30d_offers")
        stale7_pct = 100 * stale7 / open_offers if open_offers else float("nan")
        stale30_pct = 100 * stale30 / open_offers if open_offers else float("nan")

        valid_age = liq["open_offers"].fillna(0).gt(0) & liq["median_open_offer_age_seconds"].notna()
        if valid_age.any():
            typical_age = (
                liq.loc[valid_age, "median_open_offer_age_seconds"]
                * liq.loc[valid_age, "open_offers"]
            ).sum() / liq.loc[valid_age, "open_offers"].sum()
        else:
            typical_age = float("nan")

        render_kpi_grid([
            ("Open offers", count_text(open_offers)),
            ("Stale >7d", percent_text(stale7_pct)),
            ("Stale >30d", percent_text(stale30_pct)),
            ("Typical offer age", elapsed_text(typical_age)),
        ])

        market_liq = (
            liq.groupby(
                ["principal_asset_key", "collateral_asset_key", "market_name"],
                as_index=False,
            )
            .agg(
                open_offers=("open_offers", "sum"),
                stale_7d_offers=("stale_7d_offers", "sum"),
                stale_30d_offers=("stale_30d_offers", "sum"),
            )
        )
        market_liq["stale_7d_pct"] = (
            100 * market_liq["stale_7d_offers"]
            / market_liq["open_offers"].where(market_liq["open_offers"].ne(0))
        )
        market_liq["stale_30d_pct"] = (
            100 * market_liq["stale_30d_offers"]
            / market_liq["open_offers"].where(market_liq["open_offers"].ne(0))
        )

        left, right = st.columns(2)
        with left:
            st.markdown("#### Markets with the most open offers")
            labeled_bar_chart(
                market_liq.sort_values("open_offers", ascending=False).head(12),
                "market_name",
                "open_offers",
                value_kind="count",
                axis_title="Open offers",
            )

        with right:
            st.markdown("#### Highest stale-offer share")
            attention = market_liq[market_liq["open_offers"].ge(3)].sort_values(
                ["stale_7d_pct", "open_offers"],
                ascending=[False, False],
            ).head(12)
            if attention.empty:
                st.info("No markets with at least 3 open offers are available.")
            else:
                labeled_bar_chart(
                    attention,
                    "market_name",
                    "stale_7d_pct",
                    value_kind="percent",
                    axis_title="Stale >7d",
                )

        st.markdown("#### Markets that may need matching work")
        attention = market_liq[market_liq["open_offers"].ge(3)].sort_values(
            ["stale_7d_pct", "open_offers"],
            ascending=[False, False],
        )
        attention_table = attention.rename(columns={
            "market_name": "Market",
            "open_offers": "Open offers",
            "stale_7d_offers": "Stale >7d",
            "stale_30d_offers": "Stale >30d",
            "stale_7d_pct": "Stale >7d (%)",
            "stale_30d_pct": "Stale >30d (%)",
        })
        st.dataframe(
            attention_table[[
                "Market", "Open offers", "Stale >7d", "Stale >30d",
                "Stale >7d (%)", "Stale >30d (%)",
            ]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "Open offers": st.column_config.NumberColumn(format="%d"),
                "Stale >7d": st.column_config.NumberColumn(format="%d"),
                "Stale >30d": st.column_config.NumberColumn(format="%d"),
                "Stale >7d (%)": st.column_config.NumberColumn(format="%.1f%%"),
                "Stale >30d (%)": st.column_config.NumberColumn(format="%.1f%%"),
            },
        )

    st.info(
        "Recent offer cohorts are right-censored: newer offers have had less time to fill, "
        "cancel, or expire. A later iteration should add fixed-horizon fill metrics such as "
        "filled within 1h / 24h / 7d."
    )


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
    render_kpi_grid([
        ("Instructions", count_text(total)),
        ("Instruction categories", count_text(activity["instruction_name"].nunique())),
        ("Unclassified", count_text(unknown)),
    ])

    daily = activity.groupby("block_date", as_index=False)["instruction_count"].sum()
    st.markdown("#### Instructions per day")
    time_series_chart(
        daily.rename(columns={"instruction_count": "Instructions"}),
        ["Instructions"],
        value_kind="count",
        y_title="Instructions",
    )

    rankings = (
        activity.groupby("instruction_name", as_index=False)["instruction_count"]
        .sum()
        .sort_values("instruction_count", ascending=False)
    )
    left, right = st.columns(2)
    with left:
        st.markdown("#### Top instruction types")
        labeled_bar_chart(
            rankings.head(12),
            "instruction_name",
            "instruction_count",
            value_kind="count",
            axis_title="Instructions",
        )
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
    st.markdown(
        """
        <div class="dashboard-hero">
            <h1>Jupiter Offerbook</h1>
            <p>Onchain Analytics</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("MotherDuck + dbt marts · UTC data · read-only dashboard")

    st.sidebar.header("Explore Offerbook")
    page = st.sidebar.radio(
        "Section",
        ["Overview", "Lending", "Loan Lifecycle", "Markets", "Offer Insights", "Protocol Activity"],
    )
    label = st.sidebar.selectbox("Time window", list(WINDOWS), index=1)
    days = WINDOWS[label]

    if st.sidebar.button("Refresh MotherDuck data"):
        for loader in (
            load_snapshot, load_daily_lifecycle, load_daily_lending,
            load_markets, load_offer_efficiency, load_market_liquidity_snapshot,
            load_activity,
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
            elif page == "Offer Insights":
                render_offer_insights(
                    load_offer_efficiency(days),
                    load_market_liquidity_snapshot(),
                    label,
                )
            else:
                render_activity(load_activity(days), label)
    except Exception as exc:
        st.error("Unable to load this page. Check your MotherDuck connection and that the dbt marts exist.")
        st.caption(f"Error type: {type(exc).__name__}")
        st.stop()


if __name__ == "__main__":
    main()