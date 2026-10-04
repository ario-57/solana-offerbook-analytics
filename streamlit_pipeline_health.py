
import pandas as pd
import streamlit as st

from ingestion.db import get_connection


# ======================================================
# 1. Streamlit page configuration
# ======================================================

st.set_page_config(
    page_title="Offerbook | Pipeline Health",
    page_icon="📊",
    layout="wide",
)


# ======================================================
# 2. Available time filters
# ======================================================

TIME_WINDOWS = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 90 days": 90,
    "All history": None,
}


# ======================================================
# 3. Fetch monitoring data from MotherDuck
# ======================================================

@st.cache_data(
    ttl=300,
    show_spinner=False,
)
def load_run_history(days: int | None) -> pd.DataFrame:

    query = """
        select
            github_run_id,
            run_attempt,

            started_at,
            finished_at,
            run_date_utc,

            status,
            effective_status,

            elapsed_minutes,

            instructions_processed,
            instruction_issues,
            instruction_issue_pct,

            events_processed,
            event_issues,
            event_issue_pct,

            total_offers,
            missing_collateral_decimals,

            github_run_url

        from main.mart_pipeline_run_health
    """

    parameters = []

    # Apply the selected time window.
    if days is not None:

        query += """
            where started_at >=
                current_timestamp
                - (? * interval '1 day')
        """

        parameters.append(days)

    query += """
        order by started_at desc
    """

    con = get_connection()

    try:

        return con.execute(
            query,
            parameters,
        ).fetchdf()

    finally:

        con.close()


# ======================================================
# 4. Calculate and display KPI metrics
# ======================================================

def render_metrics(df: pd.DataFrame):

    completed = df[
        df["effective_status"].isin(
            ["success", "failure", "cancelled"]
        )
    ]

    successful = completed[
        completed["effective_status"] == "success"
    ]

    # Success rate among completed attempts.
    if len(completed) > 0:

        success_rate = (
            len(successful) / len(completed)
        )

        success_text = f"{success_rate:.1%}"

    else:

        success_text = "N/A"

    # Average duration of successful executions.
    avg_duration = successful[
        "elapsed_minutes"
    ].mean()

    duration_text = (
        f"{avg_duration:.1f} min"
        if pd.notna(avg_duration)
        else "N/A"
    )

    # Latest successfully completed enrichment snapshot.
    if not successful.empty:

        latest_success = successful.iloc[0]

        missing = latest_success[
            "missing_collateral_decimals"
        ]

        missing_text = (
            f"{int(missing):,}"
            if pd.notna(missing)
            else "N/A"
        )

    else:

        missing_text = "N/A"

    # Create four KPI containers.
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Run attempts",
        f"{len(df):,}",
    )

    col2.metric(
        "Completed success rate",
        success_text,
    )

    col3.metric(
        "Average successful runtime",
        duration_text,
    )

    col4.metric(
        "Missing collateral decimals",
        missing_text,
    )

    st.caption(
        "Success rate excludes running and stale attempts. "
        "Missing decimals reflect the latest successful "
        "run within the selected period."
    )


# ======================================================
# 5. Display the execution-duration chart
# ======================================================

def render_duration_chart(df: pd.DataFrame):

    st.subheader("Pipeline execution duration")

    successful = df[
        df["effective_status"] == "success"
    ].copy()

    successful = successful.sort_values(
        "started_at"
    )

    if successful.empty:

        st.info(
            "No successful executions in this period."
        )

        return

    st.line_chart(
        successful,
        x="started_at",
        y="elapsed_minutes",
        x_label="Execution time",
        y_label="Duration (minutes)",
    )


# ======================================================
# 6. Display decoder-quality trends
# ======================================================

def render_decoder_chart(df: pd.DataFrame):

    st.subheader("Decoder issues by day")

    issues = df[
        [
            "run_date_utc",
            "instruction_issues",
            "event_issues",
        ]
    ].copy()

    # Convert nullable database fields to numbers.
    for column in [
        "instruction_issues",
        "event_issues",
    ]:

        issues[column] = pd.to_numeric(
            issues[column],
            errors="coerce",
        ).fillna(0)

    # Combine multiple execution attempts on the same day.
    daily = (
        issues.groupby(
            "run_date_utc",
            as_index=False,
        )[
            [
                "instruction_issues",
                "event_issues",
            ]
        ]
        .sum()
        .sort_values("run_date_utc")
    )

    daily["run_date_utc"] = pd.to_datetime(
        daily["run_date_utc"]
    )

    st.bar_chart(
        daily,
        x="run_date_utc",
        y=[
            "instruction_issues",
            "event_issues",
        ],
        x_label="Date (UTC)",
        y_label="Processed decoding issues",
    )

    st.caption(
        "Counts represent problematic records decoded or "
        "reprocessed during each execution. They are not "
        "necessarily unique, newly discovered incidents."
    )


# ======================================================
# 7. Display individual GitHub execution attempts
# ======================================================

def render_run_history(df: pd.DataFrame):

    st.subheader("Execution history")

    history = df[
        [
            "started_at",
            "github_run_id",
            "run_attempt",
            "effective_status",
            "elapsed_minutes",
            "instructions_processed",
            "events_processed",
            "instruction_issues",
            "event_issues",
            "github_run_url",
        ]
    ].copy()

    history = history.rename(
        columns={
            "started_at": "Started at",
            "github_run_id": "Run ID",
            "run_attempt": "Attempt",
            "effective_status": "Status",
            "elapsed_minutes": "Minutes",
            "instructions_processed": "Instructions",
            "events_processed": "Events",
            "instruction_issues": "Instruction issues",
            "event_issues": "Event issues",
            "github_run_url": "GitHub Run",
        }
    )

    st.dataframe(
        history,
        hide_index=True,
        use_container_width=True,
        column_config={
            "GitHub Run": st.column_config.LinkColumn(
                "GitHub Run",
                display_text="Open run",
            ),
        },
    )


# ======================================================
# 8. Main application
# ======================================================

def main():

    st.title("Offerbook Pipeline Health")

    st.caption(
        "Operational monitoring powered by "
        "MotherDuck, dbt and GitHub Actions."
    )

    # Sidebar filters.
    st.sidebar.header("Dashboard controls")

    selected_window = st.sidebar.selectbox(
        "Select time window",
        options=list(TIME_WINDOWS.keys()),
        index=1,
    )

    days = TIME_WINDOWS[selected_window]

    # Manual cache refresh.
    if st.sidebar.button("Refresh MotherDuck data"):

        load_run_history.clear()

    st.sidebar.caption(
        "Data is automatically refreshed after "
        "five minutes or when Refresh is clicked."
    )

    # Fetch data.
    try:

        with st.spinner(
            "Loading pipeline history..."
        ):

            df = load_run_history(days)

    except Exception as exc:

        st.error(
            "Unable to load MotherDuck data. "
            "Check DB_TARGET, MOTHERDUCK_TOKEN "
            "and the database connection."
        )

        st.caption(
            f"Error type: {type(exc).__name__}"
        )

        st.stop()

    # Handle an empty result.
    if df.empty:

        st.info(
            "No pipeline executions were found "
            "for the selected period."
        )

        st.stop()

    # Display the latest operational state.
    latest = df.iloc[0]

    st.write(
        "Latest attempt status:",
        f"**{latest['effective_status']}**",
    )

    if latest["effective_status"] == "stale":

        st.warning(
            "The latest pipeline attempt appears stale. "
            "Check its GitHub Actions execution."
        )

    # Render the dashboard sections.
    render_metrics(df)

    st.divider()

    left, right = st.columns(2)

    with left:

        render_duration_chart(df)

    with right:

        render_decoder_chart(df)

    st.divider()

    render_run_history(df)


# ======================================================
# Application entry point
# ======================================================

if __name__ == "__main__":
    main()
