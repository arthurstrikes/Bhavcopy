"""
NAV Calculator - IMP Portfolios.  Place at pages/nav_calculator.py

Price source: official NSE bhavcopy (nse_bhavcopy.py at repo root).
Engine:       imp_engine.py at repo root.

Streamlit 1.39.0 API only - use_container_width, never width=.
"""

import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

import imp_engine as engine
import nse_bhavcopy

st.set_page_config(page_title="NAV Calculator - IMP Portfolios",
                   page_icon="chart_with_upwards_trend", layout="wide")

st.markdown("""
<style>
  .title { font-size:1.8rem; font-weight:700; color:#1F3864; }
  .sub   { font-size:1rem; color:#555; margin-bottom:1.2rem; }
  .info  { background:#EBF5FB; border-left:4px solid #2E86C1; padding:.8rem 1rem;
           border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
  .warn  { background:#FEF9E7; border-left:4px solid #F39C12; padding:.8rem 1rem;
           border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
  .ok    { background:#EAFAF1; border-left:4px solid #27AE60; padding:.8rem 1rem;
           border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
  .err   { background:#FDEDEC; border-left:4px solid #E74C3C; padding:.8rem 1rem;
           border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
  .metric-card { background:#F8F9FA; border-radius:8px; padding:1rem;
                 border:1px solid #DEE2E6; text-align:center; }
  .metric-val { font-size:1.5rem; font-weight:700; }
  .metric-lbl { font-size:.75rem; color:#666; margin-top:.2rem; }
  .gain { color:#1A8F4F; } .loss { color:#C0392B; } .neutral { color:#555; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="title">NAV Calculator - IMP Portfolios</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Daily rebased NAV from inception &middot; target-weight '
            'rebalancing with a 1pp deviation gate &middot; EOD prices via NSE bhavcopy</div>',
            unsafe_allow_html=True)

CAPITAL_OPTIONS = {
    "Rs 1,00,000": 100000, "Rs 1,50,000": 150000, "Rs 2,50,000": 250000,
    "Rs 5,00,000": 500000, "Rs 10,00,000": 1000000, "Rs 25,00,000": 2500000,
    "Rs 50,00,000": 5000000,
}
DEFAULT_CAPITAL = "Rs 25,00,000"
BENCH_MAP = {"Nifty 50": "^NSEI", "Nifty 500": "^CRSLDX", "Sensex": "^BSESN"}


def fmt_inr(v):
    if abs(v) >= 1e7:
        return f"Rs {v/1e7:.2f} Cr"
    if abs(v) >= 1e5:
        return f"Rs {v/1e5:.2f} L"
    return f"Rs {v:,.2f}"


def fmt_pct(v):
    return f"{'+' if v >= 0 else ''}{v:.2f}%"


# -- price fetch --------------------------------------------------------------

@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def fetch_closes(start: date, end: date):
    """
    {date: {SYMBOL: close}} for every NSE session in range.

    Every calendar day is probed. Weekends are NOT pre-filtered: NSE runs
    special weekend sessions (Sunday 01-Feb-2026 traded). A 404 from the archive
    is the authoritative non-trading-day signal.
    """
    days = engine.calendar_days(start, end)
    out, errors = {}, {}

    def one(d):
        try:
            m, traded = nse_bhavcopy.fetch_day(d)
            return d, (m if traded else None), None
        except nse_bhavcopy.BhavcopyFetchError as e:
            return d, None, str(e)[:160]

    with ThreadPoolExecutor(max_workers=nse_bhavcopy.MAX_WORKERS) as ex:
        for d, m, err in ex.map(one, days):
            if err:
                errors[d] = err
            elif m:
                out[d] = m
    return out, errors


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_benchmark(name: str, start: date, end: date):
    """Nifty 50/500 from NSE index bhavcopy; Sensex (BSE) from yfinance."""
    prices = {}
    if name in ("Nifty 50", "Nifty 500"):
        try:
            res, _, _, _ = nse_bhavcopy.fetch_closes(
                [name], engine.calendar_days(start, end), fill_holidays=False)
            prices = {d: v for d, v in res.get(name, {}).items() if v is not None}
        except Exception:
            prices = {}
    if not prices:
        try:
            df = yf.download(BENCH_MAP[name], start=start.strftime("%Y-%m-%d"),
                             end=(end + timedelta(days=2)).strftime("%Y-%m-%d"),
                             interval="1d", progress=False, auto_adjust=False, actions=False)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                cc = next((c for c in df.columns if "close" in c.lower()), None)
                if cc:
                    for idx, row in df.iterrows():
                        try:
                            v = float(row[cc])
                            if not np.isnan(v):
                                prices[idx.date()] = v
                        except Exception:
                            pass
        except Exception:
            pass
    return prices


def to_excel(nav_df, holdings_df, trades_df, alerts_df, recon_df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        nav_df.to_excel(w, sheet_name="Daily NAV", index=False)
        holdings_df.to_excel(w, sheet_name="Final Holdings", index=False)
        trades_df.to_excel(w, sheet_name="Trades", index=False)
        alerts_df.to_excel(w, sheet_name="Alerts", index=False)
        recon_df.to_excel(w, sheet_name="Reconciliation", index=False)
    return buf.getvalue()


# -- sidebar ------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Settings")
    capital_label = st.selectbox("Starting capital", list(CAPITAL_OPTIONS),
                                 index=list(CAPITAL_OPTIONS).index(DEFAULT_CAPITAL))
    capital = CAPITAL_OPTIONS[capital_label]

    start_override = st.date_input("Inception (advice before this date executes here)",
                                   value=date(2025, 10, 20),
                                   min_value=date(2015, 1, 1), max_value=date.today())
    to_date = st.date_input("Calculate through", value=date.today(),
                            min_value=date(2015, 1, 1), max_value=date.today())

    gate_pp = st.number_input("Rebalance gate (percentage points)", 0.0, 10.0, 1.0, 0.25,
                              help="Trade only when |client weight - model weight| exceeds this.")
    force_retarget = st.checkbox(
        "Re-target all holdings on event dates", value=True,
        help="On. Every holding is checked against its model weight on any date "
             "carrying a log row. Off = only symbols named in the log that date can trade.")

    benchmark = st.selectbox("Benchmark overlay", ["None", "Nifty 50", "Nifty 500", "Sensex"])

    st.markdown("---")
    st.markdown("### Upload log")
    uploaded = st.file_uploader("Historical advice log", type=["csv", "xlsx", "xls"],
                                help="Pipe-delimited CSV download or the edited XLSX.")
    run_btn = st.button("Calculate NAV", type="primary", use_container_width=True,
                        disabled=uploaded is None)
    st.caption("IMP NAV Calculator v2.0 - target-weight engine")

# -- landing ------------------------------------------------------------------

if uploaded is None:
    st.markdown('<div class="info"><strong>Upload an advice log to begin.</strong><br>'
                'Accepts the pipe-delimited CSV download or the edited XLSX. Column '
                'structures are auto-detected.</div>', unsafe_allow_html=True)
    with st.expander("How it works"):
        st.markdown("""
**Model, not replay.** Each log row states the model's target weight for a symbol.
A trade happens only when the client portfolio has drifted more than the gate away
from that target - it is not triggered by the row existing. Roughly a third of the
rows in a typical log are weight-drift entries the dashboard writes as prices move;
the gate filters them out by construction.

**Sequence on any date carrying log rows**
1. Same-date rows per symbol are chain-resolved along the `old -> new` weight trail
   (never by `No.`), and collapse to the last link. Its price is the execution
   price - `Entry Price` when the old weight is 0, otherwise `Modified Price`.
2. Model target weights update.
3. Every holding is measured against its target using the **previous** EOD NAV and
   previous EOD closes.
4. Where the deviation exceeds the gate, quantity is floored to the target weight.
   Sells execute before buys.
5. Day-end NAV = holdings at today's EOD closes + cash.

**Cash** is idle allocation plus any LIQUIDCASE / liquid-ETF weight, held as one
pool earning nothing. Negative cash is permitted and always alerted.

**Not handled in this build:** corporate actions. A demerger or split shows up as a
price collapse and will understate NAV until the adjustment layer lands.
        """)
    st.stop()

# -- parse --------------------------------------------------------------------

log_df, parse_err = engine.load_log(uploaded)
if parse_err:
    st.markdown(f'<div class="err">{parse_err}</div>', unsafe_allow_html=True)
    st.stop()

eq_log = log_df[~log_df["is_liquid"]]
st.markdown(f"""
<div class="ok">
Log parsed - <strong>{len(log_df)}</strong> rows
({len(eq_log)} equity, {len(log_df) - len(eq_log)} liquid-as-cash) &nbsp;|&nbsp;
<strong>{eq_log['symbol'].nunique()}</strong> symbols &nbsp;|&nbsp;
<strong>{eq_log['mod_dt'].nunique()}</strong> event dates &nbsp;|&nbsp;
<strong>{eq_log['mod_dt'].min():%d %b %Y}</strong> to
<strong>{eq_log['mod_dt'].max():%d %b %Y}</strong>
</div>""", unsafe_allow_html=True)

params = (uploaded.name, capital, start_override, to_date, gate_pp,
          force_retarget, benchmark)
if run_btn:
    st.session_state["nav_run"] = True
if st.session_state.get("nav_params") != params:
    for k in ("nav_results", "nav_excel", "nav_csv"):
        st.session_state.pop(k, None)
    st.session_state["nav_params"] = params

if not st.session_state.get("nav_run"):
    st.info("Set the options in the sidebar, then click **Calculate NAV**.")
    st.stop()

# -- compute (cached across reruns so the download button cannot wipe it) ------

if "nav_results" not in st.session_state:
    px_start = min(eq_log["mod_dt"].min(), start_override) - timedelta(days=7)
    with st.spinner("Fetching NSE bhavcopy..."):
        closes, fetch_errors = fetch_closes(px_start, to_date)
    if not closes:
        st.markdown('<div class="err">No NSE sessions retrieved for this range. '
                    'Check the date window and the archive host.</div>',
                    unsafe_allow_html=True)
        st.stop()
    with st.spinner("Running the NAV engine..."):
        try:
            nav_rows, trades, alerts = engine.run_nav(
                log_df, closes, capital, start_override, to_date,
                gate_pp=gate_pp, force_retarget=force_retarget)
        except ValueError as e:
            st.markdown(f'<div class="err">{e}</div>', unsafe_allow_html=True)
            st.stop()
    for d, err in sorted(fetch_errors.items()):
        alerts.append(dict(Date=d, Symbol="-", Type="BHAVCOPY FETCH ERROR", Detail=err))
    bench = fetch_benchmark(benchmark, start_override, to_date) if benchmark != "None" else {}
    st.session_state["nav_results"] = (nav_rows, trades, alerts, bench)

nav_rows, trades, alerts, bench = st.session_state["nav_results"]

nav_df = pd.DataFrame([{k: v for k, v in r.items() if k != "_holdings"} for r in nav_rows])
trades_df = pd.DataFrame(trades) if trades else pd.DataFrame(
    columns=["Date", "Symbol", "Side", "Qty", "Price", "Value"])
alerts_df = pd.DataFrame(alerts) if alerts else pd.DataFrame(
    columns=["Date", "Symbol", "Type", "Detail"])
recon_df = engine.reconciliation(nav_rows)

last, first = nav_rows[-1], nav_rows[0]
total_return = (last["NAV"] - capital) / capital * 100
abs_pl = last["NAV"] - capital

# -- alerts (first, never buried) ---------------------------------------------

if len(alerts_df):
    counts = alerts_df["Type"].value_counts()
    summary = " &nbsp;|&nbsp; ".join(f"<strong>{n}</strong> {t}" for t, n in counts.items())
    st.markdown(f'<div class="warn">{len(alerts_df)} alert(s): {summary}</div>',
                unsafe_allow_html=True)
    with st.expander(f"Alert detail ({len(alerts_df)})", expanded=False):
        pick = st.multiselect("Filter by type", sorted(counts.index),
                              default=sorted(counts.index), key="alert_filter")
        shown = alerts_df[alerts_df["Type"].isin(pick)] if pick else alerts_df
        st.dataframe(shown, use_container_width=True, hide_index=True, height=320)
else:
    st.markdown('<div class="ok">No alerts - every trade priced from the log, '
                'cash never negative, all symbols resolved.</div>', unsafe_allow_html=True)

# -- KPIs ---------------------------------------------------------------------

st.markdown("---")
cols = st.columns(6)


def kpi(col, label, value, cls="neutral"):
    col.markdown(f'<div class="metric-card"><div class="metric-val {cls}">{value}</div>'
                 f'<div class="metric-lbl">{label}</div></div>', unsafe_allow_html=True)


kpi(cols[0], "Inception", first["Date"].strftime("%d %b %Y"))
kpi(cols[1], "Portfolio NAV", fmt_inr(last["NAV"]))
kpi(cols[2], "Rebased NAV", f"{last['Rebased']:.2f}",
    "gain" if last["Rebased"] >= 100 else "loss")
kpi(cols[3], "Absolute return", fmt_pct(total_return),
    "gain" if total_return >= 0 else "loss")
kpi(cols[4], "Absolute P&L", fmt_inr(abs_pl), "gain" if abs_pl >= 0 else "loss")
kpi(cols[5], "Sessions", f"{len(nav_rows)}")

st.markdown("<br>", unsafe_allow_html=True)

# -- chart --------------------------------------------------------------------

dates_plot = [r["Date"] for r in nav_rows]
rebased = [r["Rebased"] for r in nav_rows]

fig = go.Figure()
fig.add_trace(go.Scatter(x=dates_plot, y=rebased, name="Portfolio NAV",
                         line=dict(color="#1F3864", width=2),
                         hovertemplate="<b>%{x}</b><br>NAV: %{y:.4f}"
                                       "<br>Return: %{customdata:.2f}%<extra></extra>",
                         customdata=[v - 100 for v in rebased]))
tr_dates = [r["Date"] for r in nav_rows if r["Type"] == "TRADE"]
tr_vals = [r["Rebased"] for r in nav_rows if r["Type"] == "TRADE"]
fig.add_trace(go.Scatter(x=tr_dates, y=tr_vals, mode="markers", name="Event day",
                         marker=dict(color="#E67E22", size=5),
                         hovertemplate="<b>%{x}</b> - event day<extra></extra>"))
if bench:
    bd = sorted(d for d in bench if first["Date"] <= d <= last["Date"])
    if bd:
        base = bench[bd[0]]
        fig.add_trace(go.Scatter(x=bd, y=[bench[d] / base * 100 for d in bd],
                                 name=benchmark,
                                 line=dict(color="#E74C3C", width=1.5, dash="dot"),
                                 hovertemplate=f"<b>%{{x}}</b><br>{benchmark}: "
                                               f"%{{y:.2f}}<extra></extra>"))
fig.add_hline(y=100, line_dash="dash", line_color="grey", line_width=1, opacity=.5)
fig.update_layout(
    title=dict(text=f"Rebased NAV - {uploaded.name.rsplit('.', 1)[0]} "
                    f"(base 100 on {first['Date']:%d %b %Y}, capital {capital_label})",
               font=dict(size=14, color="#1F3864")),
    height=420, hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(title="", showgrid=False, tickformat="%b %Y"),
    yaxis=dict(title="Rebased NAV", gridcolor="#F0F0F0"),
    plot_bgcolor="white", paper_bgcolor="white",
    margin=dict(l=50, r=20, t=60, b=40))
st.plotly_chart(fig, use_container_width=True)

# -- holdings + summary -------------------------------------------------------

col_h, col_s = st.columns([3, 2])
holdings_export = pd.DataFrame([
    {"Symbol": s, "Qty": h["qty"], "Close": round(h["price"], 2),
     "Value": round(h["value"], 2),
     "Achieved Wt %": round(h["value"] / last["NAV"] * 100, 2) if last["NAV"] else 0.0,
     "Model Wt %": round(h["model_wt"], 2),
     "Drift pp": round((h["value"] / last["NAV"] * 100 if last["NAV"] else 0.0)
                       - h["model_wt"], 2)}
    for s, h in sorted(last["_holdings"].items(), key=lambda x: -x[1]["value"])
])

with col_h:
    st.markdown(f"#### Holdings as of {last['Date']:%d %b %Y}")
    if len(holdings_export):
        st.dataframe(holdings_export, use_container_width=True, hide_index=True)
        gap = holdings_export["Drift pp"].abs().max()
        st.caption(f"Largest weight drift vs model: {gap:.2f}pp. "
                   f"Drift below the {gate_pp:.2f}pp gate is expected and untraded.")
    else:
        st.info("No open positions on this date.")

with col_s:
    st.markdown("#### Summary")
    cash_pct = (last["Cash"] / last["NAV"] * 100) if last["NAV"] else 0
    buys = int((trades_df["Side"] == "BUY").sum()) if len(trades_df) else 0
    sells = int((trades_df["Side"] == "SELL").sum()) if len(trades_df) else 0
    turnover = float(trades_df["Value"].sum()) if len(trades_df) else 0.0
    summary = {
        "Market value": fmt_inr(last["MarketValue"]),
        "Cash": fmt_inr(last["Cash"]),
        "Cash weight": f"{cash_pct:.2f}%",
        "Total NAV": fmt_inr(last["NAV"]),
        "Absolute return": fmt_pct(total_return),
        "Absolute P&L": fmt_inr(abs_pl),
        "Starting capital": fmt_inr(capital),
        "Sessions": str(len(nav_rows)),
        "Event dates": str(sum(1 for r in nav_rows if r["Type"] == "TRADE")),
        "Trades (buy / sell)": f"{buys} / {sells}",
        "Two-sided turnover": fmt_inr(turnover),
        "Turnover / capital": f"{turnover / capital * 100:.0f}%",
        "Open positions": str(len(last["_holdings"])),
        "Min cash over period": fmt_inr(nav_df["Cash"].min()),
    }
    st.dataframe(pd.DataFrame(list(summary.items()), columns=["Metric", "Value"]),
                 use_container_width=True, hide_index=True, height=520)

# -- tables -------------------------------------------------------------------

st.markdown("---")
tab_nav, tab_trades, tab_recon = st.tabs(["Daily NAV", "Trades", "Reconciliation"])

with tab_nav:
    flt = st.selectbox("Rows", ["All", "Event days only", "MTM days only"], key="navflt")
    d = nav_df.copy()
    if flt == "Event days only":
        d = d[d["Type"] == "TRADE"]
    elif flt == "MTM days only":
        d = d[d["Type"] == "MTM"]
    st.dataframe(d.iloc[::-1].reset_index(drop=True), use_container_width=True,
                 hide_index=True, height=420,
                 column_config={
                     "Rebased": st.column_config.NumberColumn(format="%.4f"),
                     "DayReturnPct": st.column_config.NumberColumn(format="%.3f"),
                     "MarketValue": st.column_config.NumberColumn(format="%.2f"),
                     "Cash": st.column_config.NumberColumn(format="%.2f"),
                     "NAV": st.column_config.NumberColumn(format="%.2f"),
                     "DayPL": st.column_config.NumberColumn(format="%.2f"),
                 })

with tab_trades:
    if len(trades_df):
        st.dataframe(trades_df.iloc[::-1].reset_index(drop=True),
                     use_container_width=True, hide_index=True, height=420)
        st.caption("PriceSource: log = from the advice log | eod-forced = re-target "
                   "with no log row that date | eod-zerofill = log price was 0.")
    else:
        st.info("No trades generated.")

with tab_recon:
    st.dataframe(recon_df.iloc[::-1].reset_index(drop=True),
                 use_container_width=True, hide_index=True, height=420)
    st.caption("Model weight vs achieved weight per holding per day. "
               "DriftPP within the gate is expected.")

# -- export -------------------------------------------------------------------

st.markdown("---")
if "nav_excel" not in st.session_state:
    st.session_state["nav_excel"] = to_excel(nav_df, holdings_export, trades_df,
                                             alerts_df, recon_df)
    st.session_state["nav_csv"] = nav_df.to_csv(index=False).encode()

stem = uploaded.name.rsplit(".", 1)[0]
e1, e2 = st.columns(2)
with e1:
    st.download_button("Download Excel (NAV, holdings, trades, alerts, reconciliation)",
                       data=st.session_state["nav_excel"],
                       file_name=f"NAV_{stem}_{to_date:%d%b%Y}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument."
                            "spreadsheetml.sheet",
                       use_container_width=True, type="primary")
with e2:
    st.download_button("Download CSV (NAV series only)",
                       data=st.session_state["nav_csv"],
                       file_name=f"NAV_{stem}_{to_date:%d%b%Y}.csv",
                       mime="text/csv", use_container_width=True)
