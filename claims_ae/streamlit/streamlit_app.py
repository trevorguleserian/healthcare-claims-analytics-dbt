import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(page_title="Claims Analytics Dashboard", layout="wide")
st.title("Claims Analytics Dashboard")
st.caption("Portfolio demo dashboard powered by dbt + Snowflake + Streamlit.")


# =====================================================
# Snowflake Connection (Key Pair Auth)
#
# Local dev (path-based):
#   snowflake_private_key_path = "C:\\path\\to\\rsa_key.p8"
#   snowflake_private_key_passphrase = "..."
#
# Deployment (Streamlit Cloud recommended):
#   snowflake_private_key = """-----BEGIN PRIVATE KEY-----
#   ...
#   -----END PRIVATE KEY-----"""
#   snowflake_private_key_passphrase = "..."
#
# This function supports BOTH. It will:
#   1) use snowflake_private_key if present (Cloud)
#   2) else fall back to snowflake_private_key_path (Local)
# =====================================================

@st.cache_resource
def get_snowflake_connection():
    account = st.secrets["snowflake_account"]
    user = st.secrets["snowflake_user"]

    role = st.secrets.get("snowflake_role")
    warehouse = st.secrets.get("snowflake_warehouse")
    database = st.secrets.get("snowflake_database")
    schema = st.secrets.get("snowflake_schema")

    passphrase = st.secrets.get("snowflake_private_key_passphrase", "")
    password_bytes = passphrase.encode("utf-8") if passphrase else None

    # Prefer key content for deployment
    private_key_pem = st.secrets.get("snowflake_private_key", None)

    if private_key_pem:
        # Streamlit Cloud: key stored directly in secrets (multiline string)
        p_key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"),
            password=password_bytes,
            backend=default_backend(),
        )
    else:
        # Local dev: key stored on disk
        private_key_path = st.secrets["snowflake_private_key_path"]
        with open(private_key_path, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(),
                password=password_bytes,
                backend=default_backend(),
            )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        private_key=pkb,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    return conn


@st.cache_data(ttl=120)
def run_query(sql, params=None):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def build_in_filter(column_name: str, values: list):
    """
    Builds a safe IN (...) filter with Snowflake connector named params.
    Returns (where_clause, params_dict).
    """
    if not values:
        return "", {}

    params = {f"p{i}": v for i, v in enumerate(values)}
    placeholders = ", ".join([f"%({k})s" for k in params.keys()])
    where_clause = f" WHERE {column_name} IN ({placeholders}) "
    return where_clause, params


def as_currency(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return x


def as_int(x):
    try:
        return f"{int(float(x)):,}"
    except Exception:
        return x


def as_pct(x):
    try:
        return f"{float(x):.1%}"
    except Exception:
        return x


def style_financial_df(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    money_cols = [
        "CHARGE_SUM", "ALLOWED_SUM", "PAID_SUM",
        "ALLOWED_PER_MEMBER", "PAID_PER_MEMBER",
        "OP_ALLOWED", "OP_PAID", "AVG_ALLOWED_PER_LINE",
    ]
    pct_cols = ["ALLOWED_TO_CHARGE_RATIO", "PAID_TO_ALLOWED_RATIO"]
    int_cols = ["MEMBERS", "CLAIMS", "OP_CLAIMS", "IP_STAYS"]

    fmt = {}
    for c in money_cols:
        if c in df.columns:
            fmt[c] = as_currency
    for c in pct_cols:
        if c in df.columns:
            fmt[c] = as_pct
    for c in int_cols:
        if c in df.columns:
            fmt[c] = as_int

    return df.style.format(fmt)


def download_button(df: pd.DataFrame, label: str, file_name: str, key: str):
    if df is None or df.empty:
        st.caption("No rows to download for this section.")
        return
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv,
        file_name=file_name,
        mime="text/csv",
        key=key
    )


# =====================================================
# Sidebar Controls
# =====================================================

st.sidebar.header("Filters")

payer_df = run_query("SELECT DISTINCT payer_type FROM PBI_FINANCIAL_KPIS ORDER BY payer_type")
payer_list = payer_df["PAYER_TYPE"].tolist() if not payer_df.empty else []

selected_payers = st.sidebar.multiselect(
    "Payer Type",
    options=payer_list,
    default=payer_list[:3] if len(payer_list) >= 3 else payer_list
)

show_tables = st.sidebar.toggle("Show data tables", value=True)

where_clause, params = build_in_filter("payer_type", selected_payers)

st.sidebar.divider()
st.sidebar.caption("Tip: Select fewer payers for clearer multi-line charts.")


# =====================================================
# Pull core datasets once
# =====================================================

kpi_sql = f"""
SELECT
    payer_type,
    members,
    claims,
    charge_sum,
    allowed_sum,
    paid_sum,
    allowed_to_charge_ratio,
    paid_to_allowed_ratio,
    allowed_per_member,
    paid_per_member
FROM PBI_FINANCIAL_KPIS
{where_clause}
ORDER BY payer_type
"""
kpi = run_query(kpi_sql, params)

monthly_sql = f"""
SELECT
    month,
    payer_type,
    claims,
    allowed_sum,
    paid_sum
FROM PBI_MONTHLY_TRENDS
{where_clause}
ORDER BY month
"""
monthly = run_query(monthly_sql, params)

util_sql = f"""
SELECT
    payer_type,
    op_claims,
    op_allowed,
    op_paid,
    avg_allowed_per_line
FROM PBI_UTILIZATION_BY_PAYER
{where_clause}
ORDER BY payer_type
"""
util = run_query(util_sql, params)

mix_sql = f"""
SELECT
    provider_group,
    specialty,
    payer_type,
    claims,
    allowed_sum
FROM PBI_PROVIDER_SPECIALTY_MIX
{where_clause}
ORDER BY allowed_sum DESC
LIMIT 500
"""
mix = run_query(mix_sql, params)

drg_sql = f"""
SELECT
    payer_type,
    drg_code,
    drg_description,
    ip_stays,
    avg_los,
    allowed_sum,
    paid_sum
FROM PBI_IP_DRG_SUMMARY
{where_clause}
ORDER BY allowed_sum DESC
LIMIT 200
"""
drg = run_query(drg_sql, params)


# =====================================================
# TOP KPI RIBBON (Portfolio-grade)
# =====================================================

st.subheader("Executive Summary")

if kpi.empty:
    st.info("No data returned for the selected filters.")
else:
    total_claims = float(kpi["CLAIMS"].sum())
    total_members = float(kpi["MEMBERS"].sum())
    total_allowed = float(kpi["ALLOWED_SUM"].sum())
    total_paid = float(kpi["PAID_SUM"].sum())

    paid_to_allowed = (total_paid / total_allowed) if total_allowed else 0.0
    allowed_per_claim = (total_allowed / total_claims) if total_claims else 0.0
    paid_per_claim = (total_paid / total_claims) if total_claims else 0.0

    # 7 KPI tiles
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
    k1.metric("Claims", f"{total_claims:,.0f}")
    k2.metric("Members", f"{total_members:,.0f}")
    k3.metric("Allowed", f"${total_allowed:,.2f}")
    k4.metric("Paid", f"${total_paid:,.2f}")
    k5.metric("Paid / Allowed", f"{paid_to_allowed:.1%}")
    k6.metric("Allowed / Claim", f"${allowed_per_claim:,.2f}")
    k7.metric("Paid / Claim", f"${paid_per_claim:,.2f}")

st.divider()


# =====================================================
# FINANCIAL KPI SECTION (table + downloads)
# =====================================================

st.subheader("Financial KPIs")

cA, cB = st.columns([1, 3])
with cA:
    download_button(
        kpi,
        label="⬇️ Download Financial KPIs (CSV)",
        file_name="financial_kpis.csv",
        key="dl_kpi"
    )

if not kpi.empty and show_tables:
    st.dataframe(style_financial_df(kpi), use_container_width=True, height=260)

st.divider()


# =====================================================
# High-impact Charts
# =====================================================

tab1, tab2, tab3 = st.tabs(["💰 Payer Economics", "📈 Trends", "🏥 Clinical / Provider"])


with tab1:
    st.subheader("Payer Economics")

    if kpi.empty:
        st.info("No KPI data to chart.")
    else:
        payer_bar = kpi[["PAYER_TYPE", "ALLOWED_SUM", "PAID_SUM"]].copy()
        payer_bar_melt = payer_bar.melt(
            id_vars="PAYER_TYPE",
            value_vars=["ALLOWED_SUM", "PAID_SUM"],
            var_name="Metric",
            value_name="Amount"
        )
        payer_bar_melt["Metric"] = payer_bar_melt["Metric"].replace(
            {"ALLOWED_SUM": "Allowed", "PAID_SUM": "Paid"}
        )

        fig1 = px.bar(
            payer_bar_melt,
            x="PAYER_TYPE",
            y="Amount",
            color="Metric",
            barmode="group",
            title="Allowed vs Paid by Payer Type"
        )
        fig1.update_layout(xaxis_title="", yaxis_title="Amount ($)")
        st.plotly_chart(fig1, use_container_width=True)

        eff = kpi[["PAYER_TYPE", "PAID_TO_ALLOWED_RATIO"]].copy()
        fig2 = px.bar(
            eff.sort_values("PAID_TO_ALLOWED_RATIO", ascending=False),
            x="PAYER_TYPE",
            y="PAID_TO_ALLOWED_RATIO",
            title="Paid-to-Allowed Ratio by Payer (Reimbursement Efficiency)"
        )
        fig2.update_layout(xaxis_title="", yaxis_title="Paid / Allowed")
        fig2.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

        scatter = kpi[["PAYER_TYPE", "CLAIMS", "ALLOWED_SUM", "MEMBERS"]].copy()
        fig3 = px.scatter(
            scatter,
            x="CLAIMS",
            y="ALLOWED_SUM",
            size="MEMBERS",
            hover_name="PAYER_TYPE",
            title="Cost vs Utilization (Bubble size = Members)"
        )
        fig3.update_layout(xaxis_title="Claims", yaxis_title="Allowed ($)")
        st.plotly_chart(fig3, use_container_width=True)


with tab2:
    st.subheader("Trends")

    cA, cB = st.columns([1, 3])
    with cA:
        download_button(
            monthly,
            label="⬇️ Download Monthly Trends (CSV)",
            file_name="monthly_trends.csv",
            key="dl_monthly"
        )

    if monthly.empty:
        st.info("No monthly data to chart.")
    else:
        monthly = monthly.copy()
        monthly["MONTH"] = pd.to_datetime(monthly["MONTH"])

        monthly_agg = (
            monthly.groupby("MONTH", as_index=False)[["ALLOWED_SUM", "PAID_SUM"]]
            .sum()
            .sort_values("MONTH")
        )

        fig4 = px.line(
            monthly_agg,
            x="MONTH",
            y=["ALLOWED_SUM", "PAID_SUM"],
            title="Allowed vs Paid Over Time (All Selected Payers)"
        )
        fig4.update_layout(xaxis_title="", yaxis_title="Amount ($)")
        st.plotly_chart(fig4, use_container_width=True)

        fig5 = px.line(
            monthly.sort_values("MONTH"),
            x="MONTH",
            y="ALLOWED_SUM",
            color="PAYER_TYPE",
            title="Allowed Trend by Payer Type"
        )
        fig5.update_layout(xaxis_title="", yaxis_title="Allowed ($)")
        st.plotly_chart(fig5, use_container_width=True)

        if show_tables:
            st.dataframe(style_financial_df(monthly), use_container_width=True, height=320)


with tab3:
    st.subheader("Clinical / Provider Drivers")

    cA, cB = st.columns([1, 3])
    with cA:
        download_button(
            drg,
            label="⬇️ Download DRG Summary (CSV)",
            file_name="inpatient_drg_summary.csv",
            key="dl_drg"
        )

    if not drg.empty:
        drg_plot = drg.copy()
        drg_plot["DRG_LABEL"] = drg_plot["DRG_CODE"].astype(str) + " - " + drg_plot["DRG_DESCRIPTION"].astype(str)

        top_n = st.slider("Top DRGs to display", min_value=5, max_value=30, value=15, step=1)

        drg_top = drg_plot.sort_values("ALLOWED_SUM", ascending=False).head(top_n)

        fig6 = px.bar(
            drg_top.sort_values("ALLOWED_SUM", ascending=True),
            x="ALLOWED_SUM",
            y="DRG_LABEL",
            color="PAYER_TYPE",
            orientation="h",
            title="Top Inpatient DRGs by Allowed ($)"
        )
        fig6.update_layout(xaxis_title="Allowed ($)", yaxis_title="")
        st.plotly_chart(fig6, use_container_width=True)

        if show_tables:
            st.dataframe(style_financial_df(drg), use_container_width=True, height=320)
    else:
        st.info("No DRG data returned for current filters.")

    st.divider()

    cA, cB = st.columns([1, 3])
    with cA:
        download_button(
            mix,
            label="⬇️ Download Provider Specialty Mix (CSV)",
            file_name="provider_specialty_mix.csv",
            key="dl_mix"
        )

    if not mix.empty:
        mix_plot = mix.copy()
        mix_plot = mix_plot.sort_values("ALLOWED_SUM", ascending=False).head(200)

        fig7 = px.treemap(
            mix_plot,
            path=["PROVIDER_GROUP", "SPECIALTY"],
            values="ALLOWED_SUM",
            color="ALLOWED_SUM",
            title="Provider Specialty Mix (Treemap by Allowed $)"
        )
        st.plotly_chart(fig7, use_container_width=True)

        if show_tables:
            st.dataframe(style_financial_df(mix), use_container_width=True, height=320)
    else:
        st.info("No Provider Specialty Mix data returned for current filters.")

st.divider()


# =====================================================
# Outpatient Utilization (chart + downloads)
# =====================================================

st.subheader("Outpatient Utilization")

cA, cB = st.columns([1, 3])
with cA:
    download_button(
        util,
        label="⬇️ Download Outpatient Utilization (CSV)",
        file_name="outpatient_utilization.csv",
        key="dl_util"
    )

if util.empty:
    st.info("No outpatient utilization data returned for current filters.")
else:
    fig8 = px.bar(
        util,
        x="PAYER_TYPE",
        y="AVG_ALLOWED_PER_LINE",
        title="Average Allowed per Line (Outpatient) by Payer"
    )
    fig8.update_layout(xaxis_title="", yaxis_title="Avg Allowed per Line ($)")
    st.plotly_chart(fig8, use_container_width=True)

    if show_tables:
        st.dataframe(style_financial_df(util), use_container_width=True, height=280)