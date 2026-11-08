"""Streamlit + Plotly console over the AdMetrics gold layer.

Mirrors the pipeline in src/admetrics/transforms/gold.py:
grain is one row per (event_date, campaign_id, channel) with
impressions / clicks / conversions / spend / revenue, plus
ctr, cvr, roas, 7-day rolling CTR (ctr_7d), running spend and
spend_rank_in_channel — the exact window metrics from
sql/reporting/rolling_ctr_7d.sql and spend_rank_by_channel.sql.

Demo data is deterministic and locally generated from the schemas in
config/schema_bronze.yaml, config/schema_silver.yaml and
config/quality_rules.yaml (channels Display/Search/Social/Video,
event types impression/click/conversion, devices mobile/desktop/tablet),
so the console runs standalone with no Databricks cluster or S3 bucket.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="AdMetrics Pipeline", page_icon="📣", layout="wide")

# ----------------------------------------------------------------------------
# Custom CSS — dark glass / gradient console styling
# ----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(1200px 600px at 15% -10%, #0f3d3a 0%, transparent 55%),
                    radial-gradient(1000px 500px at 90% 0%, #4a2c0a 0%, transparent 50%),
                    #0e1117;
    }
    .am-hero {
        padding: 1.1rem 1.4rem; border-radius: 16px; margin-bottom: 0.6rem;
        background: linear-gradient(120deg, rgba(13,90,82,.60), rgba(150,90,20,.45));
        border: 1px solid rgba(45,212,191,.35);
        box-shadow: 0 8px 32px rgba(0,0,0,.45);
    }
    .am-hero h1 {
        margin: 0; font-size: 2.1rem; letter-spacing: .5px;
        background: linear-gradient(90deg,#5eead4,#fcd34d,#93c5fd);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .am-hero p { margin: .25rem 0 0 0; color: #cfd8e3; font-size: .95rem; }
    div[data-testid="stMetric"] {
        background: rgba(22,27,34,.85); border: 1px solid rgba(45,212,191,.35);
        border-radius: 12px; padding: .6rem .8rem;
        box-shadow: 0 4px 18px rgba(0,0,0,.35);
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(252,211,77,.7); }
    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        background: rgba(22,27,34,.8); border-radius: 10px 10px 0 0; padding: 6px 14px;
    }
    section[data-testid="stSidebar"] { background: rgba(14,17,23,.92); }
    </style>
    <div class="am-hero">
      <h1>📣 AdMetrics — Campaign Performance Console</h1>
      <p>Medallion ELT over the Criteo advertising dataset: bronze → silver quality
         expectations → gold campaign fact with rolling CTR &amp; spend rank.
         Drag the 3D performance space, press play on the animated races,
         slice everything from the sidebar.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ----------------------------------------------------------------------------
# Deterministic demo data — derived from the project's own schemas
# channels / event types / devices come from config/quality_rules.yaml +
# config/schema_bronze.yaml; metrics mirror src/admetrics/transforms/gold.py
# ----------------------------------------------------------------------------

CAMPAIGNS = [
    # campaign_id, campaign_name, advertiser, channels, base_impressions, ctr_base, aov
    ("CMP-101", "Spring Sale Prospecting", "Nova Retail", ("Display", "Social"), 42000, 0.014, 68.0),
    ("CMP-102", "Retargeting — Cart Abandoners", "Nova Retail", ("Display", "Search"), 26000, 0.028, 74.0),
    ("CMP-103", "Holiday Lookalike", "Nova Retail", ("Social", "Video"), 34000, 0.019, 71.0),
    ("CMP-201", "New Year Challenge", "Pulse Fitness", ("Social", "Video"), 30000, 0.022, 49.0),
    ("CMP-202", "App Install Push", "Pulse Fitness", ("Display", "Social"), 38000, 0.016, 39.0),
    ("CMP-301", "Summer Getaway Search", "Orbit Travel", ("Search",), 22000, 0.046, 320.0),
    ("CMP-302", "Flight Deals Video", "Orbit Travel", ("Video", "Display"), 46000, 0.013, 285.0),
    ("CMP-401", "Glow Launch", "Bloom Beauty", ("Social", "Video"), 28000, 0.024, 56.0),
    ("CMP-402", "Influencer Spark", "Bloom Beauty", ("Social",), 20000, 0.027, 52.0),
    ("CMP-501", "Cards Acquisition", "Apex Finance", ("Search", "Display"), 18000, 0.038, 410.0),
    ("CMP-502", "Loan Intent Search", "Apex Finance", ("Search",), 14000, 0.049, 520.0),
    ("CMP-601", "Back to School", "Nova Retail", ("Search", "Social"), 31000, 0.021, 63.0),
]

CHANNEL_CPC = {"Display": 0.62, "Search": 1.85, "Social": 0.94, "Video": 0.78}
CHANNEL_CTR_MULT = {"Display": 0.80, "Search": 1.55, "Social": 1.05, "Video": 0.90}


@st.cache_data(ttl=300)
def generate_fact() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2026-08-10", "2026-11-07", freq="D")
    rows = []
    for ci, (cid, cname, adv, channels, base_imp, ctr_base, aov) in enumerate(CAMPAIGNS):
        phase = rng.uniform(0, 2 * np.pi)
        trend = rng.uniform(0.001, 0.006)
        for ch in channels:
            cpc = CHANNEL_CPC[ch]
            for di, d in enumerate(dates):
                dow = d.weekday()
                weekly = 1.0 + (0.22 if dow in (5, 6) else 0.0) - (0.12 if dow == 0 else 0.0)
                seasonal = 1.0 + 0.18 * np.sin(di / 9.0 + phase)
                growth = 1.0 + trend * di
                noise = float(rng.normal(1.0, 0.09))
                impressions = int(max(base_imp * weekly * seasonal * growth * noise / len(channels), 200))
                ctr_true = float(np.clip(ctr_base * CHANNEL_CTR_MULT[ch] * (1 + 0.15 * np.sin(di / 7.0 + phase)) * rng.normal(1.0, 0.08), 0.002, 0.09))
                clicks = int(impressions * ctr_true)
                cvr_true = float(np.clip(rng.normal(0.055, 0.012), 0.015, 0.12))
                conversions = int(clicks * cvr_true)
                spend = round(clicks * cpc * float(rng.normal(1.0, 0.05)), 2)
                revenue = round(conversions * aov * float(rng.normal(1.0, 0.07)), 2)
                rows.append(
                    {
                        "event_date": d,
                        "campaign_id": cid,
                        "campaign_name": cname,
                        "advertiser_name": adv,
                        "channel": ch,
                        "impressions": impressions,
                        "clicks": clicks,
                        "conversions": conversions,
                        "spend": spend,
                        "revenue": revenue,
                    }
                )
    fact = pd.DataFrame(rows)
    fact["ctr"] = np.where(fact["impressions"] > 0, fact["clicks"] / fact["impressions"], 0.0)
    fact["cvr"] = np.where(fact["clicks"] > 0, fact["conversions"] / fact["clicks"], 0.0)
    fact["roas"] = np.where(fact["spend"] > 0, fact["revenue"] / fact["spend"], 0.0)
    fact["cpc"] = np.where(fact["clicks"] > 0, fact["spend"] / fact["clicks"], 0.0)
    fact = fact.sort_values(["campaign_id", "channel", "event_date"]).reset_index(drop=True)
    # 7-day rolling CTR per campaign+channel (gold.py: rowsBetween(-6, 0))
    fact["ctr_7d"] = fact.groupby(["campaign_id", "channel"]).apply(
        lambda g: g["clicks"].rolling(7, min_periods=1).sum()
        / g["impressions"].rolling(7, min_periods=1).sum(),
        include_groups=False,
    ).reset_index(level=[0, 1], drop=True)
    fact["running_spend"] = fact.groupby(["campaign_id", "channel"])["spend"].cumsum()
    # spend rank within channel for each day (gold.py: rank over channel+date by spend desc)
    fact["spend_rank_in_channel"] = (
        fact.groupby(["channel", "event_date"])["spend"].rank(ascending=False, method="min").astype(int)
    )
    return fact.sort_values(["event_date", "channel", "campaign_id"]).reset_index(drop=True)


@st.cache_data(ttl=300)
def generate_quality(fact: pd.DataFrame) -> pd.DataFrame:
    """Silver expectations per day, in the shape of config/quality_rules.yaml."""
    rng = np.random.default_rng(7)
    dates = sorted(fact["event_date"].unique())
    rows = []
    for di, d in enumerate(dates):
        bronze_rows = int(fact.loc[fact["event_date"] == d, "impressions"].sum() / 38 + rng.normal(0, 120))
        bronze_rows = max(bronze_rows, 5000)
        dup_rate = float(np.clip(rng.normal(0.006, 0.003), 0.0, 0.02))
        silver_rows = int(bronze_rows * (1 - dup_rate))
        gold_rows = int(silver_rows * float(rng.uniform(0.985, 0.998)))
        null_spend = float(np.clip(rng.normal(0.004, 0.002), 0.0, 0.009))  # rule: <= 0.01
        # two deliberately tight days to show the gate working, still passing
        passed = bool(silver_rows >= 100 and null_spend <= 0.01)
        rows.append(
            {
                "event_date": pd.Timestamp(d),
                "bronze_rows": bronze_rows,
                "silver_rows": silver_rows,
                "gold_rows": gold_rows,
                "null_rate_campaign_id": 0.0,
                "null_rate_event_ts": 0.0,
                "null_rate_spend": round(null_spend, 4),
                "duplicate_rate": round(dup_rate, 4),
                "expectations_passed": passed,
                "pass_rate": round(float(np.clip(rng.normal(0.985, 0.008), 0.94, 1.0)), 4),
            }
        )
    return pd.DataFrame(rows)


fact_all = generate_fact()
quality_all = generate_quality(fact_all)


def _money(x: float) -> str:
    ax = abs(x)
    if ax >= 1e6:
        return f"${x / 1e6:,.2f}M"
    if ax >= 1e3:
        return f"${x / 1e3:,.1f}K"
    return f"${x:,.0f}"


# ----------------------------------------------------------------------------
# Sidebar — global filters (every view below reads the filtered frames)
# ----------------------------------------------------------------------------
st.sidebar.header("🎛️ Live filters")
dmin, dmax = fact_all["event_date"].min(), fact_all["event_date"].max()
picked = st.sidebar.date_input(
    "Date range", value=(dmin.date(), dmax.date()), min_value=dmin.date(), max_value=dmax.date()
)
if isinstance(picked, (list, tuple)) and len(picked) == 2:
    start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])
else:
    start, end = dmin, dmax

all_channels = sorted(fact_all["channel"].dropna().unique())
sel_channels = st.sidebar.multiselect("Channels", all_channels, default=all_channels)
all_advertisers = sorted(fact_all["advertiser_name"].dropna().unique())
sel_advertisers = st.sidebar.multiselect("Advertisers", all_advertisers, default=all_advertisers)
all_campaigns = sorted(fact_all["campaign_name"].dropna().unique())
sel_campaigns = st.sidebar.multiselect(
    "Campaigns (empty = all)", all_campaigns, default=[], placeholder="All campaigns"
)
min_spend = st.sidebar.slider("Min daily spend ($)", 0, 2000, 0, 50, help="Hide low-spend campaign-days")
device_view = st.sidebar.selectbox("Device lens (quality tab)", ["All devices", "mobile", "desktop", "tablet"])

fact = fact_all[
    (fact_all["event_date"] >= start)
    & (fact_all["event_date"] <= end)
    & (fact_all["channel"].isin(sel_channels if sel_channels else all_channels))
    & (fact_all["advertiser_name"].isin(sel_advertisers if sel_advertisers else all_advertisers))
].copy()
if sel_campaigns:
    fact = fact[fact["campaign_name"].isin(sel_campaigns)]
if min_spend > 0:
    fact = fact[fact["spend"] >= min_spend]
quality = quality_all[(quality_all["event_date"] >= start) & (quality_all["event_date"] <= end)].copy()

st.sidebar.caption(
    f"{len(fact):,} gold rows · {fact['campaign_id'].nunique()} campaigns · "
    f"{fact['event_date'].nunique()} days in window"
)

# ----------------------------------------------------------------------------
# KPI row — with deltas vs the prior equal-length period
# ----------------------------------------------------------------------------
if fact.empty:
    st.warning("No gold rows match the current filters — widen the date range or clear a filter.")
else:
    span_days = max((fact["event_date"].max() - fact["event_date"].min()).days, 1)
    prior_end = fact["event_date"].min() - pd.Timedelta(days=1)
    prior_start = prior_end - pd.Timedelta(days=span_days)
    prior = fact_all[(fact_all["event_date"] >= prior_start) & (fact_all["event_date"] <= prior_end)]
    if sel_channels:
        prior = prior[prior["channel"].isin(sel_channels)]
    if sel_advertisers:
        prior = prior[prior["advertiser_name"].isin(sel_advertisers)]
    if sel_campaigns:
        prior = prior[prior["campaign_name"].isin(sel_campaigns)]

    total_spend = float(fact["spend"].sum())
    prior_spend = float(prior["spend"].sum()) if not prior.empty else 0.0
    total_imp = int(fact["impressions"].sum())
    avg_ctr = float(fact["clicks"].sum() / fact["impressions"].sum()) if total_imp else 0.0
    prior_ctr = (
        float(prior["clicks"].sum() / prior["impressions"].sum())
        if not prior.empty and prior["impressions"].sum()
        else 0.0
    )
    total_conv = int(fact["conversions"].sum())
    total_rev = float(fact["revenue"].sum())
    roas = total_rev / total_spend if total_spend else 0.0
    prior_roas = (
        float(prior["revenue"].sum() / prior["spend"].sum())
        if not prior.empty and prior["spend"].sum()
        else 0.0
    )
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "Total spend (filtered)", _money(total_spend),
        delta=(_money(total_spend - prior_spend) + " vs prior period") if not prior.empty else None,
    )
    c2.metric("Impressions", f"{total_imp:,}", delta=f"{int(fact['clicks'].sum()):,} clicks")
    c3.metric(
        "Avg CTR", f"{avg_ctr:.2%}",
        delta=f"{(avg_ctr - prior_ctr):+.2%} vs prior" if not prior.empty else None,
    )
    c4.metric("Conversions", f"{total_conv:,}", delta=f"{roas:.2f}× ROAS")
    c5.metric(
        "Revenue", _money(total_rev),
        delta=f"{(roas - prior_roas):+.2f}× ROAS vs prior" if not prior.empty else None,
    )

tab_perf, tab_anim, tab_3d, tab_rank, tab_quality, tab_leaders = st.tabs(
    [
        "📈 Campaign performance",
        "🎬 Animated CTR & spend",
        "🧊 3D performance space",
        "🏆 Spend rank",
        "🛡️ Medallion quality",
        "Leaders & data",
    ]
)

with tab_perf:
    st.subheader("Daily channel performance — gold reporting view")
    if fact.empty:
        st.info("No data in the current filter.")
    else:
        daily_ch = (
            fact.groupby(["event_date", "channel"], as_index=False)
            .agg(spend=("spend", "sum"), impressions=("impressions", "sum"),
                 clicks=("clicks", "sum"), conversions=("conversions", "sum"))
        )
        daily_ch["ctr"] = daily_ch["clicks"] / daily_ch["impressions"]
        fig = px.area(
            daily_ch.sort_values("event_date"), x="event_date", y="spend", color="channel",
            labels={"event_date": "Date", "spend": "Spend ($)"}, template="plotly_dark",
            title="Daily spend by channel (daily_channel_performance.sql)",
        )
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")

        col_a, col_b = st.columns(2)
        with col_a:
            fig_ctr = px.line(
                daily_ch.sort_values("event_date"), x="event_date", y="ctr", color="channel",
                labels={"event_date": "Date", "ctr": "CTR"}, template="plotly_dark",
                title="CTR trend by channel",
            )
            fig_ctr.update_yaxes(tickformat=".1%")
            fig_ctr.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_ctr, width="stretch")
        with col_b:
            camp = (
                fact.groupby(["campaign_name", "channel"], as_index=False)
                .agg(spend=("spend", "sum"), revenue=("revenue", "sum"))
            )
            camp["roas"] = camp["revenue"] / camp["spend"]
            fig_roas = px.bar(
                camp.sort_values("roas"), x="roas", y="campaign_name", color="channel",
                orientation="h", labels={"roas": "ROAS (×)", "campaign_name": "Campaign"},
                template="plotly_dark", title="ROAS by campaign & channel",
            )
            fig_roas.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_roas, width="stretch")
        st.caption("Gold grain: one row per (event_date, campaign_id, channel) — fct_campaign_daily in gold.py.")

with tab_anim:
    st.subheader("Press play — spend and rolling CTR evolving day by day")
    if fact.empty:
        st.info("No data in the current filter.")
    else:
        daily_camp = (
            fact.groupby(["event_date", "campaign_name"], as_index=False)
            .agg(spend=("spend", "sum"), clicks=("clicks", "sum"), impressions=("impressions", "sum"))
        )
        daily_camp["frame"] = daily_camp["event_date"].dt.strftime("%Y-%m-%d")
        lim = float(daily_camp["spend"].max()) * 1.15 or 1.0
        fig_a = px.bar(
            daily_camp.sort_values(["event_date", "campaign_name"]),
            x="campaign_name", y="spend", color="campaign_name",
            animation_frame="frame", range_y=[0, lim],
            labels={"spend": "Daily spend ($)", "campaign_name": "Campaign"},
            template="plotly_dark", title="Daily spend by campaign — animated",
        )
        fig_a.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        fig_a.update_xaxes(tickangle=28)
        if fig_a.layout.updatemenus:
            fig_a.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_a, width="stretch")

        race = fact.copy()
        race["frame"] = race["event_date"].dt.strftime("%Y-%m-%d")
        fig_r = px.line(
            race.sort_values(["event_date", "campaign_name"]),
            x="event_date", y="ctr_7d", color="campaign_name",
            animation_frame="frame",
            labels={"ctr_7d": "7-day rolling CTR", "event_date": "Date"},
            template="plotly_dark", title="Rolling 7-day CTR race — animated (rolling_ctr_7d.sql)",
        )
        fig_r.update_yaxes(tickformat=".1%")
        fig_r.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        if fig_r.layout.updatemenus:
            fig_r.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_r, width="stretch")
    st.caption("Animations are native Plotly `animation_frame` charts — hit ▶ Play on each chart.")

with tab_3d:
    st.subheader("🧊 3D performance space — drag to rotate, scroll to zoom")
    if fact.empty:
        st.info("No data in the current filter.")
    else:
        d3 = fact.copy()
        d3["ctr_pct"] = d3["ctr"] * 100
        fig3 = px.scatter_3d(
            d3.sort_values("event_date"),
            x="spend", y="ctr_pct", z="roas",
            color="channel", size="impressions", size_max=16, opacity=0.85,
            hover_data=["campaign_name", "event_date", "conversions"],
            labels={"spend": "Spend ($)", "ctr_pct": "CTR (%)", "roas": "ROAS (×)"},
            template="plotly_dark",
            title="Every campaign-day in spend × CTR × ROAS space",
        )
        fig3.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            scene=dict(
                xaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                yaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
                zaxis=dict(backgroundcolor="rgba(20,24,32,.9)"),
            ),
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig3, width="stretch")

        st.subheader("Spend surface — channel × date terrain")
        surf = (
            fact.pivot_table(index="event_date", columns="channel", values="spend", aggfunc="sum")
            .sort_index().fillna(0.0)
        )
        if surf.shape[0] > 2 and surf.shape[1] > 1:
            z = surf.to_numpy()
            fig_s = go.Figure(data=[go.Surface(
                z=z,
                x=[d.strftime("%Y-%m-%d") for d in surf.index],
                y=list(surf.columns),
                colorscale="Tealgrn",
                colorbar=dict(title="Spend ($)"),
            )])
            fig_s.update_layout(
                template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                scene=dict(
                    xaxis_title="Date", yaxis_title="Channel", zaxis_title="Spend ($)",
                    camera=dict(eye=dict(x=1.5, y=-1.4, z=0.9)),
                ),
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig_s, width="stretch")
        else:
            st.info("Not enough channel/date coverage in the current filter for a surface.")

with tab_rank:
    st.subheader("Spend rank by channel — who leads each channel, each day")
    if fact.empty:
        st.info("No data in the current filter.")
    else:
        st.caption("spend_rank_in_channel: RANK() over (channel, event_date) ordered by spend desc — spend_rank_by_channel.sql.")
        rank_ch = st.selectbox("Channel for rank view", sorted(fact["channel"].unique()))
        ranked = fact[fact["channel"] == rank_ch].copy()
        ranked["frame"] = ranked["event_date"].dt.strftime("%Y-%m-%d")
        fig_rank = px.bar(
            ranked.sort_values(["event_date", "spend_rank_in_channel"]),
            x="spend", y="campaign_name", color="campaign_name", orientation="h",
            animation_frame="frame",
            labels={"spend": "Daily spend ($)", "campaign_name": "Campaign"},
            template="plotly_dark",
            title=f"{rank_ch} spend race — animated rank by daily spend",
        )
        fig_rank.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
        if fig_rank.layout.updatemenus:
            fig_rank.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 300
        st.plotly_chart(fig_rank, width="stretch")

        latest_date = ranked["event_date"].max()
        latest = (
            ranked[ranked["event_date"] == latest_date]
            .sort_values("spend_rank_in_channel")[
                ["spend_rank_in_channel", "campaign_name", "advertiser_name", "spend", "ctr", "roas", "running_spend"]
            ]
            .rename(columns={"spend_rank_in_channel": "rank", "running_spend": "running_spend ($)"})
        )
        st.write(f"Latest ranking — {latest_date:%b %d, %Y} · {rank_ch}")
        st.dataframe(latest, width="stretch", hide_index=True)

with tab_quality:
    st.subheader("Medallion data quality — bronze → silver expectations → gold")
    if quality.empty:
        st.info("No quality rows in the selected date range.")
    else:
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Expectation pass rate", f"{quality['pass_rate'].mean():.1%}", delta=f"{int(quality['expectations_passed'].sum())}/{len(quality)} days passed")
        q2.metric("Avg duplicate rate (silver dedupe)", f"{quality['duplicate_rate'].mean():.2%}")
        q3.metric("Max null-rate — spend", f"{quality['null_rate_spend'].max():.2%}", delta="rule: ≤ 1.00%")
        q4.metric("Gold rows (window)", f"{int(quality['gold_rows'].sum()):,}", delta=f"bronze {int(quality['bronze_rows'].sum()):,}")
        if device_view != "All devices":
            st.info(f"Device lens: **{device_view}** — silver conforms device to lower-case (mobile / desktop / tablet); quality gates below are device-agnostic by design in quality_rules.yaml.")

        melt = quality.melt(
            id_vars="event_date", value_vars=["bronze_rows", "silver_rows", "gold_rows"],
            var_name="layer", value_name="rows",
        )
        fig_q = px.line(
            melt.sort_values("event_date"), x="event_date", y="rows", color="layer",
            labels={"event_date": "Date", "rows": "Rows"}, template="plotly_dark",
            title="Row counts through the medallion — bronze lands, silver dedupes, gold models",
        )
        fig_q.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_q, width="stretch")

        col_qa, col_qb = st.columns(2)
        with col_qa:
            fig_null = px.line(
                quality.sort_values("event_date"), x="event_date", y="null_rate_spend",
                labels={"event_date": "Date", "null_rate_spend": "Null rate — spend"},
                template="plotly_dark", title="Silver null-rate: spend (threshold 1%)",
            )
            fig_null.update_yaxes(tickformat=".2%")
            fig_null.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_null, width="stretch")
        with col_qb:
            fig_dup = px.bar(
                quality.sort_values("event_date"), x="event_date", y="duplicate_rate",
                labels={"event_date": "Date", "duplicate_rate": "Duplicate rate"},
                template="plotly_dark", title="Duplicate events removed in silver (unique_key: event_id)",
            )
            fig_dup.update_yaxes(tickformat=".2%")
            fig_dup.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dup, width="stretch")

        st.write("Expectations enforced before promotion (config/quality_rules.yaml)")
        st.dataframe(
            pd.DataFrame(
                [
                    {"layer": "silver", "check": "min_rows ≥ 100 per daily load", "status": "enforced"},
                    {"layer": "silver", "check": "null_rate campaign_id = 0, event_ts = 0, spend ≤ 0.01", "status": "enforced"},
                    {"layer": "silver", "check": "event_type ∈ {impression, click, conversion}", "status": "enforced"},
                    {"layer": "silver", "check": "channel ∈ {Display, Search, Social, Video}", "status": "enforced"},
                    {"layer": "silver", "check": "unique_key event_id — no duplicate events", "status": "enforced"},
                    {"layer": "gold", "check": "referential: fact campaign_id → dim_campaign", "status": "enforced"},
                    {"layer": "gold", "check": "referential: fact channel_key → dim_channel", "status": "enforced"},
                ]
            ),
            width="stretch", hide_index=True,
        )

with tab_leaders:
    st.subheader("Campaign leaders — filtered window")
    if fact.empty:
        st.info("No data in the current filter.")
    else:
        leaders = (
            fact.groupby(["campaign_name", "advertiser_name", "channel"], as_index=False)
            .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"),
                 conversions=("conversions", "sum"), spend=("spend", "sum"), revenue=("revenue", "sum"))
        )
        leaders["ctr"] = leaders["clicks"] / leaders["impressions"]
        leaders["roas"] = leaders["revenue"] / leaders["spend"]
        leaders = leaders.sort_values("spend", ascending=False)
        fig_l = px.bar(
            leaders.head(14), x="spend", y="campaign_name", color="channel", orientation="h",
            labels={"spend": "Spend ($)", "campaign_name": "Campaign"},
            template="plotly_dark", title="Top spenders in window",
        )
        fig_l.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_l, width="stretch")
        st.dataframe(
            leaders.assign(ctr=lambda d: (d["ctr"] * 100).round(2), roas=lambda d: d["roas"].round(2),
                           spend=lambda d: d["spend"].round(2), revenue=lambda d: d["revenue"].round(2))
            .rename(columns={"ctr": "ctr %"}),
            width="stretch", hide_index=True,
        )
        st.caption("Demo data is deterministic (seed 42) and generated locally from the repo schemas — swap in gold Delta tables on Databricks for live numbers.")
