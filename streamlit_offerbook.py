The requested file reference is not currently visible. Use files.search or files.list to rediscover the file, then retry with a returned ref_id or file_id.    for column in [
        "originated_loans", "repaid_loans", "defaulted_loans",
        "origination_volume_usd", "origination_collateral_value_usd",
        "realized_interest_usd", "priced_originations", "unpriced_originations",
    ]:
        markets[column] = pd.to_numeric(markets[column], errors="coerce")

    priced = markets["priced_originations"].fillna(0)
    unpriced = markets["unpriced_originations"].fillna(0)
    denom = priced + unpriced

    markets["price_coverage_pct"] = 100 * priced / denom.where(denom.ne(0))
    markets["avg_apy_pct"] = pd.to_numeric(markets["avg_apy_raw"], errors="coerce") / 100
    markets["avg_duration_days"] = pd.to_numeric(markets["avg_duration_raw"], errors="coerce") / 86_400
    markets["collateralization_ratio"] = (
        markets["origination_collateral_value_usd"]
        / markets["origination_volume_usd"].where(markets["origination_volume_usd"].ne(0))
    )
    markets["default_rate_pct"] = (
        100 * markets["defaulted_loans"]
        / markets["originated_loans"].where(markets["originated_loans"].ne(0))
    )

    markets["market_label"] = markets["market_name"].fillna("Unknown market").astype(str)
    repeated = markets["market_label"].duplicated(keep=False)
    markets.loc[repeated, "market_label"] = markets.loc[repeated].apply(
        lambda row: (
            f"{row['market_label']} "
            f"({str(row['principal_asset_key'])[:6]}/{str(row['collateral_asset_key'])[:6]})"
        ),
        axis=1,
    )

    total_volume = markets["origination_volume_usd"].sum(min_count=1)
    top3_volume = (
        markets["origination_volume_usd"].nlargest(3).sum(min_count=1)
        if markets["origination_volume_usd"].notna().any()
        else float("nan")
    )
    top3_share = (
        100 * top3_volume / total_volume
        if pd.notna(total_volume) and total_volume != 0 and pd.notna(top3_volume)
        else float("nan")
    )

    a, b, c, d = st.columns(4)
    a.metric("Market pairs", count_text(len(markets)))
    b.metric("Originated loans", count_text(sum_count(markets, "originated_loans")))
    c.metric("Origination volume", usd_text(total_volume))
    d.metric("Top 3 volume concentration", pct_text(top3_share))

    e, f, g = st.columns(3)
    e.metric("Realized interest", usd_text(sum_usd(markets, "realized_interest_usd")))
    f.metric("Origination price coverage", coverage_text(int(priced.sum()), int(unpriced.sum())))
    g.metric("Defaulted loans", count_text(sum_count(markets, "defaulted_loans")))

    left, right = st.columns(2)
    with left:
        st.markdown("#### Top markets by originated loans")
        top = markets.sort_values("originated_loans", ascending=False).head(10)
        st.bar_chart(top, x="market_label", y="originated_loans")

    with right:
        st.markdown("#### Top markets by priced origination USD")
        top = markets.dropna(subset=["origination_volume_usd"]).sort_values(
            "origination_volume_usd", ascending=False
        ).head(10)
        if top.empty:
            st.info("No priced origination volume is available for this period.")
        else:
            st.bar_chart(top, x="market_label", y="origination_volume_usd")

    left, right = st.columns(2)
    with left:
        st.markdown("#### Average APY by market")
        top = markets.dropna(subset=["avg_apy_pct"]).sort_values(
            "originated_loans", ascending=False
        ).head(12)
        st.bar_chart(top, x="market_label", y="avg_apy_pct")

    with right:
        st.markdown("#### Default rate by market")
        eligible = markets[markets["originated_loans"].fillna(0).ge(3)].dropna(
            subset=["default_rate_pct"]
        ).sort_values("default_rate_pct", ascending=False).head(12)
        if eligible.empty:
            st.info("No markets with at least 3 originated loans are available in this period.")
        else:
            st.bar_chart(eligible, x="market_label", y="default_rate_pct")

    st.markdown("#### Market leaderboard")
    table = markets[[
        "market_name", "originated_loans", "repaid_loans", "defaulted_loans",
        "origination_volume_usd", "realized_interest_usd", "avg_apy_pct",
        "avg_duration_days", "collateralization_ratio", "default_rate_pct",
        "price_coverage_pct", "principal_asset_key", "collateral_asset_key",
    ]].rename(columns={
        "market_name": "Market",
        "originated_loans": "Originations",
        "repaid_loans": "Repayments",
        "defaulted_loans": "Defaults",
        "origination_volume_usd": "Origination USD",
        "realized_interest_usd": "Realized interest USD",
        "avg_apy_pct": "Avg APY (%)",
        "avg_duration_days": "Avg duration (days)",
        "collateralization_ratio": "Collateralization (x)",
        "default_rate_pct": "Defaults / originations (%)",
        "price_coverage_pct": "Priced originations (%)",
        "principal_asset_key": "Principal key",
        "collateral_asset_key": "Collateral key",
    })

    st.dataframe(
        table.sort_values("Originations", ascending=False),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Origination USD": st.column_config.NumberColumn(format="$%.2f"),
            "Realized interest USD": st.column_config.NumberColumn(format="$%.2f"),
            "Avg APY (%)": st.column_config.NumberColumn(format="%.2f%%"),
            "Avg duration (days)": st.column_config.NumberColumn(format="%.1f"),
            "Collateralization (x)": st.column_config.NumberColumn(format="%.2fx"),
            "Defaults / originations (%)": st.column_config.NumberColumn(format="%.1f%%"),
            "Priced originations (%)": st.column_config.NumberColumn(format="%.1f%%"),
        },
    )

    render_download(markets, "Download market data (CSV)", "offerbook_market_summary.csv")
    st.caption(
        "Default rate by market is descriptive: it compares observed default events with originations in the selected window and is not a cohort-matched default probability. "
        "Use the Cohorts page for outcome analysis by origination vintage."
    )
    render_price_methodology()




def render_offer_insights(
    efficiency: pd.DataFrame,
    liquidity: pd.DataFrame,
    window: str,
):
    st.subheader(f"Offer Insights · {window}")
    st.caption(
        "Offer metrics are token-native and do not require additional Birdeye pricing. "
        "Creation cohorts measure what eventually happened to offers created in the selected period."
    )

    if efficiency.empty and liquidity.empty:
        st.info("No offer-level analytics were found.")
        return

    role_label = st.selectbox(
        "Offer creator side",
        ["All creators", "Lender-side offers", "Borrower-side offers"],
        index=0,
        help=(
            "Principal-side offers are treated as lender-created; collateral-side offers "
            "are treated as borrower-created."
        ),
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

    numeric_eff = [
        "offers_created", "offers_with_fill", "fulfilled_offers",
        "partially_filled_offers", "cancelled_offers", "expired_offers",
        "active_offers", "stale_7d_offers", "stale_30d_offers",
        "unique_offer_creators", "loans_created_from_offers",
        "avg_principal_fill_ratio", "avg_seconds_to_first_fill",
        "avg_apy_raw", "avg_duration_raw",
    ]
    for column in numeric_eff:
        if column in eff.columns:
            eff[column] = pd.to_numeric(eff[column], errors="coerce")

    numeric_liq = [
        "open_offers", "active_offer_creators", "stale_7d_offers",
        "stale_30d_offers", "median_open_offer_age_seconds",
        "p90_open_offer_age_seconds", "avg_open_apy_raw",
        "median_open_apy_raw", "avg_open_duration_raw",
        "median_open_duration_raw", "stale_7d_offer_pct",
        "stale_30d_offer_pct",
    ]
    for column in numeric_liq:
        if column in liq.columns:
            liq[column] = pd.to_numeric(liq[column], errors="coerce")

    st.markdown("### User view")
    if eff.empty:
        st.info("No created offers were found for this side in the selected period.")
    else:
        offers = sum_count(eff, "offers_created")
        with_fill = sum_count(eff, "offers_with_fill")
        fulfilled = sum_count(eff, "fulfilled_offers")
        cancelled = sum_count(eff, "cancelled_offers")
        expired = sum_count(eff, "expired_offers")

        fill_rate = 100.0 * with_fill / offers if offers else float("nan")
        full_fill_rate = 100.0 * fulfilled / offers if offers else float("nan")
        cancel_rate = 100.0 * cancelled / offers if offers else float("nan")
        expiry_rate = 100.0 * expired / offers if offers else float("nan")
        avg_fill_seconds = weighted_average(
            eff, "avg_seconds_to_first_fill", "offers_with_fill"
        )
        avg_apy_raw = weighted_average(eff, "avg_apy_raw", "offers_created")

        a, b, c, d = st.columns(4)
        a.metric("Offers created", count_text(offers))
        b.metric("Reached a fill", pct_text(fill_rate))
        c.metric("Fully fulfilled", pct_text(full_fill_rate))
        d.metric(
            "Avg time to first fill",
            elapsed_text(avg_fill_seconds),
        )

        e, f, g = st.columns(3)
        e.metric("Average offered APY", apy_text(avg_apy_raw))
        f.metric("Cancelled", pct_text(cancel_rate))
        g.metric("Expired", pct_text(expiry_rate))

        daily = (
            eff.groupby("block_date", as_index=False)[
                [
                    "offers_created", "offers_with_fill", "fulfilled_offers",
                    "cancelled_offers", "expired_offers"
                ]
            ]
            .sum()
            .sort_values("block_date")
        )
        daily["fill_rate_pct"] = (
            100.0 * daily["offers_with_fill"]
            / daily["offers_created"].where(daily["offers_created"].ne(0))
        )

        left, right = st.columns(2)
        with left:
            st.markdown("#### Offer outcomes by creation date")
            chart = date_ready(daily).rename(columns={
                "offers_created": "Created",
                "offers_with_fill": "Reached a fill",
                "fulfilled_offers": "Fully fulfilled",
                "cancelled_offers": "Cancelled",
                "expired_offers": "Expired",
            })
            st.line_chart(
                chart,
                x="block_date",
                y=["Created", "Reached a fill", "Fully fulfilled", "Cancelled", "Expired"],
            )

        with right:
            st.markdown("#### Fill rate by creation date")
            st.line_chart(
                date_ready(daily),
                x="block_date",
                y="fill_rate_pct",
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
            100.0 * market["offers_with_fill"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )
        market["full_fill_rate_pct"] = (
            100.0 * market["fulfilled_offers"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )
        market["cancel_rate_pct"] = (
            100.0 * market["cancelled_offers"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )
        market["expiry_rate_pct"] = (
            100.0 * market["expired_offers"]
            / market["offers_created"].where(market["offers_created"].ne(0))
        )

        st.markdown("#### Market fill performance")
        st.dataframe(
            market.sort_values(["offers_created", "fill_rate_pct"], ascending=[False, False]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "offers_created": st.column_config.NumberColumn("Offers", format="%d"),
                "offers_with_fill": st.column_config.NumberColumn("Reached a fill", format="%d"),
                "fulfilled_offers": st.column_config.NumberColumn("Fulfilled", format="%d"),
                "loans_created": st.column_config.NumberColumn("Loans created", format="%d"),
                "fill_rate_pct": st.column_config.NumberColumn("Fill rate", format="%.1f%%"),
                "full_fill_rate_pct": st.column_config.NumberColumn("Full-fill rate", format="%.1f%%"),
                "cancel_rate_pct": st.column_config.NumberColumn("Cancelled", format="%.1f%%"),
                "expiry_rate_pct": st.column_config.NumberColumn("Expired", format="%.1f%%"),
            },
        )

    st.markdown("### Developer view")
    if liq.empty:
        st.info("No currently open offers were found for this side.")
    else:
        open_offers = sum_count(liq, "open_offers")
        stale7 = sum_count(liq, "stale_7d_offers")
        stale30 = sum_count(liq, "stale_30d_offers")
        creators = sum_count(liq, "active_offer_creators")

        # active_offer_creators is additive only within a market/role row, so this
        # is intentionally labelled market-level creator presences, not unique wallets.
        stale7_pct = 100.0 * stale7 / open_offers if open_offers else float("nan")
        stale30_pct = 100.0 * stale30 / open_offers if open_offers else float("nan")
        median_age = weighted_average(
            liq, "median_open_offer_age_seconds", "open_offers"
        )

        a, b, c, d = st.columns(4)
        a.metric("Open offers", count_text(open_offers))
        b.metric("Stale >7d", pct_text(stale7_pct))
        c.metric("Stale >30d", pct_text(stale30_pct))
        d.metric(
            "Typical open-offer age",
            elapsed_text(median_age),
        )

        st.caption(
            f"Market-level creator presences: {creators:,}. Do not interpret this as protocol-wide unique wallets because the same wallet can appear in multiple markets."
        )

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
            100.0 * market_liq["stale_7d_offers"]
            / market_liq["open_offers"].where(market_liq["open_offers"].ne(0))
        )
        market_liq["stale_30d_pct"] = (
            100.0 * market_liq["stale_30d_offers"]
            / market_liq["open_offers"].where(market_liq["open_offers"].ne(0))
        )

        left, right = st.columns(2)
        with left:
            st.markdown("#### Markets with the most open offers")
            top = market_liq.sort_values("open_offers", ascending=False).head(12)
            st.bar_chart(top, x="market_name", y="open_offers")

        with right:
            st.markdown("#### Markets with the highest stale share")
            attention = market_liq[market_liq["open_offers"].ge(3)].sort_values(
                ["stale_7d_pct", "open_offers"], ascending=[False, False]
            ).head(12)
            if attention.empty:
                st.info("No markets with at least 3 open offers are available.")
            else:
                st.bar_chart(attention, x="market_name", y="stale_7d_pct")

        st.markdown("#### Markets that may need matching/liquidity work")
        attention = market_liq[market_liq["open_offers"].ge(3)].sort_values(
            ["stale_7d_pct", "open_offers"], ascending=[False, False]
        )
        if attention.empty:
            st.info("No markets with enough open offers for a useful comparison.")
        else:
            st.dataframe(
                attention,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "open_offers": st.column_config.NumberColumn("Open offers", format="%d"),
                    "stale_7d_offers": st.column_config.NumberColumn("Stale >7d", format="%d"),
                    "stale_30d_offers": st.column_config.NumberColumn("Stale >30d", format="%d"),
                    "stale_7d_pct": st.column_config.NumberColumn("Stale >7d share", format="%.1f%%"),
                    "stale_30d_pct": st.column_config.NumberColumn("Stale >30d share", format="%.1f%%"),
                },
            )

    st.info(
        "Recent creation cohorts are right-censored: newer offers have had less time to fill, cancel, or expire. "
        "The next iteration should add fixed-horizon metrics such as filled within 1h / 24h / 7d for fair cohort comparison."
    )

    if not eff.empty:
        render_download(eff, "Download offer efficiency data (CSV)", "offerbook_offer_efficiency.csv")
    if not liq.empty:
        render_download(liq, "Download open-liquidity snapshot (CSV)", "offerbook_market_liquidity_snapshot.csv")


def render_activity(activity: pd.DataFrame, window: str):
    st.subheader(f"Protocol instruction activity · {window}")

    if activity.empty:
        st.info("No instruction activity was found in this period.")
        return

    total_instructions = sum_count(activity, "instruction_count")
    successful_instructions = sum_count(activity, "successful_instructions")
    failed_instructions = sum_count(activity, "failed_instructions")
    unknown = sum_count(
        activity[activity["instruction_name"].eq("unknown")],
        "instruction_count",
    )

    success_rate = (
        100.0 * successful_instructions / total_instructions
        if total_instructions
        else float("nan")
    )

    a, b, c, d = st.columns(4)
    a.metric("Instructions", count_text(total_instructions))
    b.metric("Instruction success rate", pct_text(success_rate))
    c.metric("Instruction categories", count_text(activity["instruction_name"].nunique()))
    d.metric("Unclassified instructions", count_text(unknown))

    daily = (
        activity.groupby("block_date", as_index=False)[
            ["instruction_count", "successful_instructions", "failed_instructions"]
        ]
        .sum()
        .sort_values("block_date")
    )

    st.markdown("#### Instructions per day")
    chart = date_ready(daily).rename(columns={
        "instruction_count": "Total",
        "successful_instructions": "Successful",
        "failed_instructions": "Failed",
    })
    st.line_chart(chart, x="block_date", y=["Total", "Successful", "Failed"])

    rankings = (
        activity.groupby("instruction_name", as_index=False)["instruction_count"]
        .sum()
        .sort_values("instruction_count", ascending=False)
    )

    left, right = st.columns(2)

    with left:
        st.markdown("#### Top instruction types")
        st.bar_chart(
            rankings.head(12),
            x="instruction_name",
            y="instruction_count",
        )

    with right:
        st.markdown("#### Full instruction breakdown")
        st.dataframe(rankings, hide_index=True, use_container_width=True)

    st.caption(
        "Do not sum per-instruction-type transaction counts to estimate unique protocol transactions: "
        "the same Solana signature can contain multiple Offerbook instruction types."
    )

    render_download(
        activity,
        "Download activity data (CSV)",
        "offerbook_daily_activity.csv",
    )


# ==========================================================
# Main application
# ==========================================================
def main():
    st.title("Jupiter Offerbook | Onchain Analytics")
    st.caption(
        "Production MotherDuck + dbt marts · UTC calendar-day filters · daily reference USD pricing"
    )

    st.sidebar.header("Explore Offerbook")

    page = st.sidebar.radio(
        "Section",
        [
            "Overview",
            "Lending",
            "Loan Lifecycle",
            "Cohorts",
            "Markets",
            "Offer Insights",
            "Protocol Activity",
        ],
    )

    label = st.sidebar.selectbox(
        "Time window",
        list(WINDOWS),
        index=1,
    )
    days = WINDOWS[label]

    if st.sidebar.button("Refresh MotherDuck data"):
        for loader in (
            load_snapshot,
            load_daily_lifecycle,
            load_daily_lending,
            load_cohorts,
            load_markets,
            load_offer_efficiency,
            load_market_liquidity_snapshot,
            load_activity,
        ):
            loader.clear()
        st.rerun()

    st.sidebar.caption(
        "Results are cached for 10 minutes. Lifetime snapshot KPIs are not affected by the selected date window."
    )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "USD values use daily historical reference prices. USDC/USDT may use the pipeline's fixed $1 assumption."
    )

    try:
        with st.spinner("Loading Offerbook analytics from MotherDuck..."):
            if page == "Overview":
                render_overview(
                    load_snapshot(),
                    load_daily_lifecycle(days),
                    label,
                )

            elif page == "Lending":
                render_lending(
                    load_daily_lending(days),
                    label,
                )

            elif page == "Loan Lifecycle":
                render_lifecycle(
                    load_snapshot(),
                    load_daily_lifecycle(days),
                    label,
                )

            elif page == "Cohorts":
                render_cohorts(
                    load_cohorts(days),
                    label,
                )

            elif page == "Markets":
                render_markets(
                    load_markets(days),
                    label,
                )

            elif page == "Offer Insights":
                render_offer_insights(
                    load_offer_efficiency(days),
                    load_market_liquidity_snapshot(),
                    label,
                )

            else:
                render_activity(
                    load_activity(days),
                    label,
                )

    except Exception as exc:
        st.error(
            "Unable to load this page. Check the MotherDuck connection and confirm the production dbt marts exist."
        )
        st.caption(f"Error type: {type(exc).__name__}")
        st.exception(exc)
        st.stop()


if __name__ == "__main__":
    main()