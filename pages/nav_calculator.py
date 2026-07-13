import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta, date
import time
import re
from collections import defaultdict

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NAV Calculator — IMP Portfolios",
    page_icon="📈",
    layout="wide"
)

st.markdown("""
<style>
    .title  { font-size:1.8rem; font-weight:700; color:#1F3864; }
    .sub    { font-size:1rem; color:#555; margin-bottom:1.2rem; }
    .info   { background:#EBF5FB; border-left:4px solid #2E86C1;
              padding:.8rem 1rem; border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
    .warn   { background:#FEF9E7; border-left:4px solid #F39C12;
              padding:.8rem 1rem; border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
    .ok     { background:#EAFAF1; border-left:4px solid #27AE60;
              padding:.8rem 1rem; border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
    .err    { background:#FDEDEC; border-left:4px solid #E74C3C;
              padding:.8rem 1rem; border-radius:4px; font-size:.9rem; margin-bottom:.8rem; }
    .metric-card { background:#F8F9FA; border-radius:8px; padding:1rem;
                   border:1px solid #DEE2E6; text-align:center; }
    .metric-val  { font-size:1.6rem; font-weight:700; }
    .metric-lbl  { font-size:.78rem; color:#666; margin-top:.2rem; }
    .gain { color:#1A8F4F; }
    .loss { color:#C0392B; }
    .neutral { color:#555; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="title">📈 NAV Calculator — IMP Portfolios</div>', unsafe_allow_html=True)
st.markdown('<div class="sub">Daily rebased NAV from inception · EOD prices via Yahoo Finance · Version A (log trade prices + EOD MTM)</div>', unsafe_allow_html=True)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
CAPITAL_OPTIONS = {
    "₹1,00,000":  100000,
    "₹1,50,000":  150000,
    "₹2,50,000":  250000,
    "₹5,00,000":  500000,
    "₹10,00,000": 1000000,
    "₹25,00,000": 2500000,
    "₹50,00,000": 5000000,
}
DEFAULT_CAPITAL = "₹2,50,000"
LIQUID_KEYWORDS = ["LIQUIDCASE", "LIQUIDBEES", "LIQUIDETF", "LIQUID"]
YF_DELAY        = 0.3
YF_RETRIES      = 3

# ── HELPERS ───────────────────────────────────────────────────────────────────
def to_yf(symbol: str) -> str:
    sym = symbol.strip().upper()
    # Handle known quirks
    quirks = {"J&KBANK": "JKBANK.NS", "IDFCFIRSTB": "IDFCFIRSTBK.NS",
               "ATHERENERG": "ATHERENG.NS", "ENRIN": "SIEMENSENR.NS"}
    if sym in quirks:
        return quirks[sym]
    if sym.endswith(".NS") or sym.endswith(".BO"):
        return sym
    return sym + ".NS"

def clean_price(val) -> float:
    """Strip commas, cast to float, return 0 on failure."""
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return 0.0

def parse_date(val) -> date | None:
    if pd.isna(val) if isinstance(val, float) else not val:
        return None
    s = str(val).strip()
    for fmt in ["%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(s, dayfirst=True).date()
    except:
        return None

def is_liquid(symbol: str) -> bool:
    return any(kw in symbol.upper() for kw in LIQUID_KEYWORDS)

def fmt_inr(val: float) -> str:
    """Format as Indian number system."""
    if abs(val) >= 1e7:
        return f"₹{val/1e7:.2f}Cr"
    if abs(val) >= 1e5:
        return f"₹{val/1e5:.2f}L"
    return f"₹{val:,.2f}"

def fmt_pct(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"

# ── FILE PARSER ───────────────────────────────────────────────────────────────
def parse_log(uploaded_file) -> tuple[pd.DataFrame | None, str]:
    """
    Parse historical log — handles both CSV and XLSX/XLS.
    Returns (df, error_message). df=None on failure.

    CSV columns (default download):
      No. | Symbol | Company Name | Sector | Issued Allocation | Remaining Allocation |
      Entry Price | Entry Time | Exit Price | Exit Time | P&L | Returns | Freeze Price |
      ISIN-Exchange-Series | Rationale

    XLSX columns (edited version):
      No. | Symbol | Company Name | Sector | New Weight | Old Weight |
      Entry Price | Entry Date | Modified Price | Modified Date | ISIN
    """
    fname = uploaded_file.name.lower()
    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(uploaded_file, sep="|", dtype=str, encoding="utf-8")
        elif fname.endswith((".xlsx", ".xls")):
            df = pd.read_excel(uploaded_file, dtype=str)
        else:
            return None, f"Unsupported file type: {fname}. Upload CSV, XLSX, or XLS."
    except Exception as e:
        return None, f"Could not read file: {e}"

    df.columns = [c.strip() for c in df.columns]

    # ── Detect format and normalise to standard column names ──────────────
    # Standard internal names: sym, new_wt, old_wt, entry_px, entry_date, exit_px, exit_date
    col_map = {}

    def find_col(candidates):
        for c in candidates:
            for col in df.columns:
                if col.strip().lower() == c.lower():
                    return col
        return None

    sym_col       = find_col(["Symbol"])
    new_wt_col    = find_col(["Issued Allocation", "New Weight"])
    old_wt_col    = find_col(["Remaining Allocation", "Old Weight"])
    entry_px_col  = find_col(["Entry Price"])
    entry_dt_col  = find_col(["Entry Time", "Entry Date"])
    exit_px_col   = find_col(["Exit Price", "Modified Price"])
    exit_dt_col   = find_col(["Exit Time", "Modified Date"])
    no_col        = find_col(["No.", "No"])

    missing = [name for name, col in [
        ("Symbol", sym_col), ("New Weight / Issued Allocation", new_wt_col),
        ("Old Weight / Remaining Allocation", old_wt_col),
        ("Entry Price", entry_px_col), ("Entry Time / Date", entry_dt_col),
        ("Exit Price / Modified Price", exit_px_col), ("Exit Time / Modified Date", exit_dt_col),
    ] if col is None]

    if missing:
        return None, f"Could not find required columns: {', '.join(missing)}.\nFound: {list(df.columns)}"

    out = pd.DataFrame()
    out["no"]         = pd.to_numeric(df[no_col].str.strip(), errors="coerce") if no_col else range(len(df))
    out["symbol"]     = df[sym_col].str.strip().str.replace("&amp;", "&", regex=False)
    out["new_wt"]     = df[new_wt_col].apply(clean_price)
    out["old_wt"]     = df[old_wt_col].apply(clean_price)
    out["entry_px"]   = df[entry_px_col].apply(clean_price)
    out["entry_date"] = df[entry_dt_col].apply(parse_date)
    out["exit_px"]    = df[exit_px_col].apply(clean_price)
    out["exit_date"]  = df[exit_dt_col].apply(parse_date)

    # Drop rows with no symbol or no exit_date (can't place in timeline)
    out = out.dropna(subset=["symbol", "exit_date"])
    out = out[out["symbol"].str.strip() != ""]

    # Mark liquid ETF rows
    out["is_liquid"] = out["symbol"].apply(is_liquid)

    # Exclude liquid rows from NAV calc (treated as cash)
    out = out[~out["is_liquid"]].copy()
    out = out.reset_index(drop=True)

    if out.empty:
        return None, "No valid non-LIQUIDCASE entries found after parsing."

    return out, ""


# ── YFINANCE PRICE FETCHER ────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=3600)
def fetch_eod_prices(symbols_tuple: tuple, start_str: str, end_str: str) -> dict:
    """
    Fetch EOD close prices for multiple symbols over a date range.
    Returns {symbol: {date: price}} dict.
    Cached for 1 hour.
    """
    symbols = list(symbols_tuple)
    start   = datetime.strptime(start_str, "%Y-%m-%d").date()
    end     = datetime.strptime(end_str,   "%Y-%m-%d").date()
    result  = {}

    for sym in symbols:
        ticker = to_yf(sym)
        for attempt in range(1, YF_RETRIES + 1):
            try:
                df = yf.download(
                    ticker,
                    start=start - timedelta(days=5),   # buffer for weekends
                    end=end   + timedelta(days=2),
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    actions=False,
                )
                if df.empty:
                    if attempt < YF_RETRIES:
                        time.sleep(YF_DELAY * 2)
                    continue
                # Flatten MultiIndex if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                close_col = next((c for c in df.columns if "close" in c.lower()), None)
                if close_col is None:
                    break
                prices = {}
                for idx, row in df.iterrows():
                    d = idx.date() if hasattr(idx, "date") else idx
                    v = float(row[close_col])
                    if not np.isnan(v):
                        prices[d] = v
                result[sym] = prices
                break
            except Exception:
                if attempt < YF_RETRIES:
                    time.sleep(YF_DELAY * 2)
        if sym not in result:
            result[sym] = {}
        time.sleep(YF_DELAY)

    return result


# ── CHAIN RESOLVER ────────────────────────────────────────────────────────────
def resolve_chain(trades: list[dict], current_wt: float) -> list[dict]:
    """
    Order same-day same-symbol trades by following old_wt → new_wt trail
    starting from current_wt in portfolio. Handles BIOCON-type exits buried
    out of sequence in the log.
    """
    if len(trades) == 1:
        return trades
    rem = trades.copy()
    ordered = []
    wt = current_wt
    for _ in range(len(trades) + 1):
        found = next((t for t in rem if abs(t["old_wt"] - wt) < 0.02), None)
        if not found:
            break
        ordered.append(found)
        wt = found["new_wt"]
        rem.remove(found)
    # Append any unresolved (shouldn't happen in clean data)
    ordered.extend(sorted(rem, key=lambda x: x["no"]))
    return ordered


# ── TRADING CALENDAR ──────────────────────────────────────────────────────────
def build_calendar(start: date, end: date, action_dates: set, price_dates: set) -> list[date]:
    """
    Union of: weekdays in [start, end] filtered to dates that have
    either an action or a price available. This avoids NSE holidays.
    Falls back to all weekdays if price data is sparse.
    """
    all_weekdays = set()
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            all_weekdays.add(cur)
        cur += timedelta(days=1)

    # If we have price data, restrict to dates with prices or actions
    has_prices = len(price_dates) > 10
    if has_prices:
        trading = (price_dates | action_dates) & all_weekdays
    else:
        trading = all_weekdays | action_dates

    return sorted(d for d in trading if start <= d <= end)


# ── CORE NAV ENGINE ───────────────────────────────────────────────────────────
def run_nav(
    log: pd.DataFrame,
    eod_prices: dict,           # {sym: {date: price}}
    capital: float,
    to_date: date,
) -> tuple[list[dict], list[dict]]:
    """
    Returns: (nav_series, glitch_log)
    nav_series: list of dicts — one per trading day
    glitch_log: list of zero-price warnings
    """
    glitch_log = []

    # ── Group log by exit_date ───────────────────────────────────────────
    by_date = defaultdict(list)
    for _, row in log.iterrows():
        if row["exit_date"] <= to_date:
            by_date[row["exit_date"]].append({
                "no":       row["no"],
                "symbol":   row["symbol"],
                "new_wt":   row["new_wt"],
                "old_wt":   row["old_wt"],
                "entry_px": row["entry_px"],
                "exit_px":  row["exit_px"],
                "exit_date":row["exit_date"],
            })

    action_dates = set(by_date.keys())

    # ── Collect all price dates ──────────────────────────────────────────
    price_dates = set()
    for sym_prices in eod_prices.values():
        price_dates.update(sym_prices.keys())

    start_date = log["exit_date"].min()
    calendar   = build_calendar(start_date, to_date, action_dates, price_dates)

    # ── Portfolio state ──────────────────────────────────────────────────
    portfolio   = {}   # {sym: {"qty": int, "avg_px": float, "weight": float}}
    price_cache = {}   # {sym: latest_known_eod_price}
    cash        = capital
    nav_series  = []

    def live_nav():
        mv = sum(h["qty"] * price_cache.get(s, h["avg_px"]) for s, h in portfolio.items())
        return mv + cash

    def get_trade_price(sym, px_field, fallback_date):
        """
        Return trade price. If zero (glitch), pull EOD price for that date.
        Log a warning.
        """
        if px_field > 0:
            return px_field, False
        # Glitch — use EOD
        eod = eod_prices.get(sym, {}).get(fallback_date)
        if not eod:
            # Walk back up to 3 days for a price
            for d in range(1, 4):
                eod = eod_prices.get(sym, {}).get(fallback_date - timedelta(days=d))
                if eod:
                    break
        if eod:
            return eod, True
        return 0.0, True

    for day in calendar:
        is_action = day in by_date

        # ── Process trades ───────────────────────────────────────────────
        if is_action:
            by_sym = defaultdict(list)
            for t in by_date[day]:
                by_sym[t["symbol"]].append(t)

            for sym, trades in by_sym.items():
                current_wt = portfolio.get(sym, {}).get("weight", 0.0)
                ordered    = resolve_chain(trades, current_wt)

                for t in ordered:
                    cur_nav = live_nav()

                    # ── NEW BUY ──────────────────────────────────────────
                    if t["old_wt"] == 0 and t["new_wt"] > 0:
                        px, glitch = get_trade_price(sym, t["entry_px"], day)
                        if glitch:
                            glitch_log.append({
                                "Date": day, "Symbol": sym,
                                "Action": "BUY", "Log Price": t["entry_px"],
                                "EOD Used": px,
                                "Note": "Entry price was 0 — used EOD price"
                            })
                        if px <= 0:
                            st.warning(f"⚠ {day} {sym}: no buy price available — skipped")
                            continue
                        qty = int(cur_nav * (t["new_wt"] / 100) / px)
                        if qty <= 0:
                            continue
                        portfolio[sym] = {"qty": qty, "avg_px": px, "weight": t["new_wt"]}
                        cash -= qty * px
                        price_cache[sym] = px

                    # ── FULL EXIT ────────────────────────────────────────
                    elif t["new_wt"] == 0 and t["old_wt"] > 0:
                        if sym not in portfolio:
                            continue
                        qty = portfolio[sym]["qty"]
                        px, glitch = get_trade_price(sym, t["exit_px"], day)
                        if glitch:
                            glitch_log.append({
                                "Date": day, "Symbol": sym,
                                "Action": "SELL", "Log Price": t["exit_px"],
                                "EOD Used": px,
                                "Note": "Exit price was 0 — used EOD price"
                            })
                        if px <= 0:
                            st.warning(f"⚠ {day} {sym}: no exit price — using avg cost")
                            px = portfolio[sym]["avg_px"]
                        cash += qty * px
                        del portfolio[sym]
                        price_cache.pop(sym, None)

                    # ── REBALANCE ────────────────────────────────────────
                    elif t["old_wt"] > 0 and t["new_wt"] > 0:
                        px, glitch = get_trade_price(sym, t["exit_px"], day)
                        if glitch:
                            glitch_log.append({
                                "Date": day, "Symbol": sym,
                                "Action": "REBALANCE", "Log Price": t["exit_px"],
                                "EOD Used": px,
                                "Note": "Rebalance price was 0 — used EOD price"
                            })
                        if px <= 0:
                            px = price_cache.get(sym, portfolio.get(sym, {}).get("avg_px", 0))
                        if px <= 0:
                            continue
                        new_qty = int(cur_nav * (t["new_wt"] / 100) / px)
                        old_qty = portfolio.get(sym, {}).get("qty", 0)
                        delta   = new_qty - old_qty
                        if delta > 0:
                            cash -= delta * px
                        elif delta < 0:
                            cash += abs(delta) * px
                        portfolio[sym] = {
                            "qty": new_qty,
                            "avg_px": portfolio.get(sym, {}).get("avg_px", px),
                            "weight": t["new_wt"]
                        }
                        price_cache[sym] = px

        # ── Update price cache from EOD ──────────────────────────────────
        for sym in list(portfolio.keys()):
            p = eod_prices.get(sym, {}).get(day)
            if p:
                price_cache[sym] = p

        # ── Snap day-end NAV ─────────────────────────────────────────────
        mkt_val   = 0.0
        holdings  = {}
        for sym, h in portfolio.items():
            px  = price_cache.get(sym, h["avg_px"])
            val = h["qty"] * px
            mkt_val += val
            holdings[sym] = {"qty": h["qty"], "price": px, "value": val, "weight": h["weight"]}

        nav       = mkt_val + cash
        prev_nav  = nav_series[-1]["nav"] if nav_series else capital
        day_pl    = nav - prev_nav
        day_ret   = (day_pl / prev_nav * 100) if prev_nav else 0.0
        rebased   = (nav / capital) * 100

        nav_series.append({
            "date":      day,
            "mkt_val":   mkt_val,
            "cash":      cash,
            "nav":       nav,
            "rebased":   rebased,
            "day_pl":    day_pl,
            "day_ret":   day_ret,
            "is_action": is_action,
            "holdings":  holdings,
        })

    return nav_series, glitch_log


# ── EXCEL EXPORT ──────────────────────────────────────────────────────────────
def to_excel(nav_df: pd.DataFrame, holdings_df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        nav_df.to_excel(writer, sheet_name="Daily NAV", index=False)
        holdings_df.to_excel(writer, sheet_name="Final Holdings", index=False)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════════════
# UI
# ══════════════════════════════════════════════════════════════════════════════

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Settings")

    capital_label = st.selectbox(
        "Starting Capital",
        options=list(CAPITAL_OPTIONS.keys()),
        index=list(CAPITAL_OPTIONS.keys()).index(DEFAULT_CAPITAL),
    )
    capital = CAPITAL_OPTIONS[capital_label]

    to_date_input = st.date_input(
        "Calculate through",
        value=date.today(),
        min_value=date(2020, 1, 1),
        max_value=date.today(),
    )

    benchmark = st.selectbox(
        "Benchmark (for chart overlay)",
        ["None", "Nifty 50", "Nifty 500", "Sensex"],
    )
    BENCH_MAP = {
        "Nifty 50":  "^NSEI",
        "Nifty 500": "^CRSLDX",
        "Sensex":    "^BSESN",
    }

    st.markdown("---")
    st.markdown("### 📂 Upload Log")
    uploaded = st.file_uploader(
        "Historical log file",
        type=["csv", "xlsx", "xls"],
        help="CSV (pipe-delimited default download) or XLSX/XLS",
    )
    run_btn = st.button("▶ Calculate NAV", type="primary", use_container_width=True,
                        disabled=uploaded is None)

    st.markdown("---")
    st.caption("IMP NAV Calculator v1.0 · Version A · Built for Motilal Oswal Research")

# ── MAIN PANEL ────────────────────────────────────────────────────────────────
if uploaded is None:
    st.markdown("""
    <div class="info">
    <strong>Upload your portfolio log to begin.</strong><br>
    Accepts the default pipe-delimited CSV download or the edited XLSX format.
    Both column structures are auto-detected — no manual editing required.
    </div>
    """, unsafe_allow_html=True)

    with st.expander("📖 How it works"):
        st.markdown("""
**What this tool does:**
1. Parses the historical log to reconstruct every trade since inception
2. Fetches EOD closing prices from Yahoo Finance for all active holdings
3. Calculates daily NAV = Market Value of holdings + Unutilised Cash
4. Rebases NAV to 100 on inception date

**Price logic (Version A):**
- Trade quantity uses the **log price** (RA execution price) at each buy/rebalance/exit
- Daily MTM uses **Yahoo Finance EOD closing prices**
- LIQUIDCASE and any liquid ETF = treated as cash (excluded from holdings)
- Zero price in log = system glitch → EOD price substituted automatically with a warning

**Chain resolution:**
Multi-step weight changes on the same day (e.g. 25%→20%→14%→0%) are
executed in the correct order by following the old_wt→new_wt trail, not
by row number.
        """)
    st.stop()

# ── PARSE LOG ─────────────────────────────────────────────────────────────────
log_df, parse_err = parse_log(uploaded)
if parse_err:
    st.markdown(f'<div class="err">❌ {parse_err}</div>', unsafe_allow_html=True)
    st.stop()

inception_date = log_df["exit_date"].min()
last_log_date  = log_df["exit_date"].max()
unique_syms    = sorted(log_df["symbol"].unique())
action_dates   = sorted(log_df["exit_date"].unique())

st.markdown(f"""
<div class="ok">
✅ Log parsed — <strong>{len(log_df)}</strong> entries &nbsp;|&nbsp;
<strong>{len(unique_syms)}</strong> unique symbols &nbsp;|&nbsp;
<strong>{len(action_dates)}</strong> action dates &nbsp;|&nbsp;
<strong>{inception_date.strftime('%d %b %Y')}</strong> → <strong>{last_log_date.strftime('%d %b %Y')}</strong>
</div>
""", unsafe_allow_html=True)

if not run_btn:
    st.info("Configure settings in the sidebar and click **Calculate NAV** to run.")
    st.stop()

# ── FETCH PRICES ──────────────────────────────────────────────────────────────
to_date = to_date_input

# Active-only: only fetch symbols that were active at any point up to to_date
active_syms = sorted(log_df[log_df["exit_date"] <= to_date]["symbol"].unique().tolist())

fetch_start = (inception_date - timedelta(days=5)).strftime("%Y-%m-%d")
fetch_end   = (to_date + timedelta(days=2)).strftime("%Y-%m-%d")

with st.spinner(f"Fetching EOD prices for {len(active_syms)} symbols from Yahoo Finance..."):
    progress_bar = st.progress(0, text="Starting price fetch...")
    eod_prices   = {}

    for i, sym in enumerate(active_syms):
        progress_bar.progress(
            (i + 1) / len(active_syms),
            text=f"Fetching {sym} ({i+1}/{len(active_syms)})..."
        )
        ticker = to_yf(sym)
        try:
            df = yf.download(
                ticker,
                start=fetch_start,
                end=fetch_end,
                interval="1d",
                progress=False,
                auto_adjust=False,
                actions=False,
            )
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                close_col = next((c for c in df.columns if "close" in c.lower()), None)
                if close_col:
                    prices = {}
                    for idx, row in df.iterrows():
                        d = idx.date() if hasattr(idx, "date") else idx
                        v = row[close_col]
                        try:
                            fv = float(v)
                            if not np.isnan(fv):
                                prices[d] = fv
                        except:
                            pass
                    eod_prices[sym] = prices
                else:
                    eod_prices[sym] = {}
            else:
                eod_prices[sym] = {}
        except Exception as e:
            eod_prices[sym] = {}
        time.sleep(YF_DELAY)

    progress_bar.empty()

# Report fetch results
fetch_ok   = [s for s in active_syms if eod_prices.get(s)]
fetch_fail = [s for s in active_syms if not eod_prices.get(s)]

if fetch_fail:
    st.markdown(f"""
    <div class="warn">
    ⚠️ Price fetch failed for <strong>{len(fetch_fail)}</strong> symbol(s):
    {", ".join(fetch_fail)}<br>
    These will use log prices / carry-forward on non-trade days.
    </div>
    """, unsafe_allow_html=True)

# ── FETCH BENCHMARK ───────────────────────────────────────────────────────────
bench_prices = {}
if benchmark != "None":
    bench_ticker = BENCH_MAP[benchmark]
    try:
        df = yf.download(bench_ticker, start=fetch_start, end=fetch_end,
                         interval="1d", progress=False, auto_adjust=False, actions=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            close_col = next((c for c in df.columns if "close" in c.lower()), None)
            if close_col:
                for idx, row in df.iterrows():
                    d = idx.date() if hasattr(idx, "date") else idx
                    try:
                        v = float(row[close_col])
                        if not np.isnan(v):
                            bench_prices[d] = v
                    except:
                        pass
    except:
        pass

# ── RUN NAV ENGINE ────────────────────────────────────────────────────────────
with st.spinner("Computing daily NAV..."):
    nav_series, glitch_log = run_nav(log_df, eod_prices, capital, to_date)

if not nav_series:
    st.error("NAV calculation returned no data. Check your log file.")
    st.stop()

# ── GLITCH WARNINGS ───────────────────────────────────────────────────────────
if glitch_log:
    with st.expander(f"⚠️ {len(glitch_log)} zero-price glitch(es) detected — EOD prices substituted"):
        st.dataframe(pd.DataFrame(glitch_log), use_container_width=True, hide_index=True)

# ── BUILD DATAFRAMES ──────────────────────────────────────────────────────────
nav_df = pd.DataFrame([{
    "Date":          r["date"].strftime("%d-%b-%Y"),
    "Market Value":  round(r["mkt_val"], 2),
    "Cash":          round(r["cash"], 2),
    "NAV":           round(r["nav"], 2),
    "Rebased NAV":   round(r["rebased"], 4),
    "Day P&L":       round(r["day_pl"], 2),
    "Day Return %":  round(r["day_ret"], 3),
    "Type":          "TRADE" if r["is_action"] else "MTM",
} for r in nav_series])

last_row     = nav_series[-1]
first_row    = nav_series[0]
total_return = (last_row["nav"] - capital) / capital * 100
abs_pl       = last_row["nav"] - capital

# ── KPI CARDS ─────────────────────────────────────────────────────────────────
st.markdown("---")
k1, k2, k3, k4, k5, k6 = st.columns(6)

def kpi(col, label, value, color_class="neutral"):
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-val {color_class}">{value}</div>
        <div class="metric-lbl">{label}</div>
    </div>
    """, unsafe_allow_html=True)

kpi(k1, "Inception Date",   inception_date.strftime("%d %b %Y"))
kpi(k2, "Portfolio NAV",    fmt_inr(last_row["nav"]))
kpi(k3, "Rebased NAV",      f"{last_row['rebased']:.2f}", "gain" if last_row["rebased"] >= 100 else "loss")
kpi(k4, "Absolute Return",  fmt_pct(total_return), "gain" if total_return >= 0 else "loss")
kpi(k5, "Absolute P&L",     fmt_inr(abs_pl), "gain" if abs_pl >= 0 else "loss")
kpi(k6, "Trading Days",     f"{len(nav_series)} days")

st.markdown("<br>", unsafe_allow_html=True)

# ── CHART ─────────────────────────────────────────────────────────────────────
dates_plot   = [r["date"] for r in nav_series]
rebased_plot = [r["rebased"] for r in nav_series]

fig = go.Figure()

# Portfolio NAV line
fig.add_trace(go.Scatter(
    x=dates_plot, y=rebased_plot,
    name="Portfolio NAV",
    line=dict(color="#1F3864", width=2),
    hovertemplate="<b>%{x}</b><br>NAV: %{y:.4f}<br>Return: %{customdata:.2f}%<extra></extra>",
    customdata=[r - 100 for r in rebased_plot],
))

# Trade markers
trade_dates = [r["date"] for r in nav_series if r["is_action"]]
trade_vals  = [r["rebased"] for r in nav_series if r["is_action"]]
fig.add_trace(go.Scatter(
    x=trade_dates, y=trade_vals,
    mode="markers",
    name="Trade Day",
    marker=dict(color="#E67E22", size=5, symbol="circle"),
    hovertemplate="<b>%{x}</b> — Trade Day<extra></extra>",
))

# Benchmark overlay
if bench_prices:
    bench_dates = sorted(bench_prices.keys())
    # Find first bench date >= inception
    first_bench_date = next((d for d in bench_dates if d >= inception_date), None)
    if first_bench_date:
        base_bench = bench_prices[first_bench_date]
        bench_rebased = []
        bench_dates_plot = []
        for d in bench_dates:
            if inception_date <= d <= to_date:
                bench_rebased.append(bench_prices[d] / base_bench * 100)
                bench_dates_plot.append(d)
        fig.add_trace(go.Scatter(
            x=bench_dates_plot, y=bench_rebased,
            name=benchmark,
            line=dict(color="#E74C3C", width=1.5, dash="dot"),
            hovertemplate=f"<b>%{{x}}</b><br>{benchmark}: %{{y:.2f}}<extra></extra>",
        ))

fig.add_hline(y=100, line_dash="dash", line_color="grey", line_width=1, opacity=0.5)

fig.update_layout(
    title=dict(text=f"Rebased NAV — {uploaded.name.split('.')[0]} (Base 100 on {inception_date.strftime('%d %b %Y')})",
               font=dict(size=14, color="#1F3864")),
    height=420,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(title="", showgrid=False, tickformat="%b %Y"),
    yaxis=dict(title="Rebased NAV", gridcolor="#F0F0F0"),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(l=50, r=20, t=60, b=40),
)
st.plotly_chart(fig, use_container_width=True)

# ── HOLDINGS + SUMMARY ────────────────────────────────────────────────────────
col_h, col_s = st.columns([3, 2])

with col_h:
    st.markdown(f"#### Holdings as of {last_row['date'].strftime('%d %b %Y')}")
    holdings_data = []
    for sym, h in sorted(last_row["holdings"].items(), key=lambda x: -x[1]["value"]):
        actual_wt = (h["value"] / last_row["nav"] * 100) if last_row["nav"] else 0
        holdings_data.append({
            "Symbol": sym,
            "Qty":    h["qty"],
            "Close (₹)": round(h["price"], 2),
            "Value (₹)": round(h["value"], 2),
            "Wt %":  round(actual_wt, 2),
        })
    if holdings_data:
        st.dataframe(pd.DataFrame(holdings_data), use_container_width=True, hide_index=True)
    else:
        st.info("No open positions on this date.")

with col_s:
    st.markdown("#### Portfolio Summary")
    cash_pct = (last_row["cash"] / last_row["nav"] * 100) if last_row["nav"] else 0
    trade_days = sum(1 for r in nav_series if r["is_action"])
    summary = {
        "Market Value":    fmt_inr(last_row["mkt_val"]),
        "Unutilised Cash": fmt_inr(last_row["cash"]),
        "Cash Weight":     f"{cash_pct:.1f}%",
        "Total NAV":       fmt_inr(last_row["nav"]),
        "Rebased NAV":     f"{last_row['rebased']:.4f}",
        "Absolute Return": fmt_pct(total_return),
        "Absolute P&L":    fmt_inr(abs_pl),
        "Starting Capital":fmt_inr(capital),
        "Total Days":      str(len(nav_series)),
        "Trade Days":      str(trade_days),
        "MTM Days":        str(len(nav_series) - trade_days),
        "Active Stocks":   str(len(last_row["holdings"])),
    }
    sum_df = pd.DataFrame(list(summary.items()), columns=["Metric", "Value"])
    st.dataframe(sum_df, use_container_width=True, hide_index=True, height=440)

# ── DAILY NAV TABLE ───────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Daily NAV Series")

col_f1, col_f2 = st.columns([1, 3])
with col_f1:
    show_type = st.selectbox("Filter rows", ["All", "Trade days only", "MTM days only"])
with col_f2:
    st.write("")  # spacer

disp_df = nav_df.copy()
if show_type == "Trade days only":
    disp_df = disp_df[disp_df["Type"] == "TRADE"]
elif show_type == "MTM days only":
    disp_df = disp_df[disp_df["Type"] == "MTM"]

# Show latest first
disp_df = disp_df.iloc[::-1].reset_index(drop=True)

st.dataframe(
    disp_df,
    use_container_width=True,
    hide_index=True,
    height=420,
    column_config={
        "Rebased NAV":  st.column_config.NumberColumn(format="%.4f"),
        "Day Return %": st.column_config.NumberColumn(format="%.3f"),
        "Market Value": st.column_config.NumberColumn(format="₹%.2f"),
        "Cash":         st.column_config.NumberColumn(format="₹%.2f"),
        "NAV":          st.column_config.NumberColumn(format="₹%.2f"),
        "Day P&L":      st.column_config.NumberColumn(format="₹%.2f"),
    }
)

# ── EXPORT ────────────────────────────────────────────────────────────────────
st.markdown("---")
ex1, ex2 = st.columns(2)

holdings_export = pd.DataFrame([
    {"Symbol": sym, "Qty": h["qty"], "Close": round(h["price"],2),
     "Value": round(h["value"],2), "Weight %": round(h["value"]/last_row["nav"]*100,2)}
    for sym, h in last_row["holdings"].items()
]) if last_row["holdings"] else pd.DataFrame()

with ex1:
    excel_bytes = to_excel(nav_df, holdings_export)
    st.download_button(
        label="⬇️ Download Excel (NAV + Holdings)",
        data=excel_bytes,
        file_name=f"NAV_{uploaded.name.split('.')[0]}_{to_date.strftime('%d%b%Y')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
    )

with ex2:
    csv_bytes = nav_df.to_csv(index=False).encode()
    st.download_button(
        label="⬇️ Download CSV (NAV series only)",
        data=csv_bytes,
        file_name=f"NAV_{uploaded.name.split('.')[0]}_{to_date.strftime('%d%b%Y')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

