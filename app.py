import streamlit as st
import pandas as pd
import yfinance as yf
from io import BytesIO
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="Bhavcopy – NSE Close Prices",
    page_icon="📋",
    layout="wide"
)

st.markdown("""
<style>
    .title { font-size: 2rem; font-weight: 700; color: #1F3864; }
    .subtitle { font-size: 1rem; color: #555; margin-bottom: 1.5rem; }
    .info-box { background: #EBF5FB; border-left: 4px solid #2E86C1;
                padding: 0.8rem 1rem; border-radius: 4px; font-size: 0.9rem; margin-bottom: 1rem; }
    .success-box { background: #EAFAF1; border-left: 4px solid #27AE60;
                   padding: 0.8rem 1rem; border-radius: 4px; font-size: 0.9rem; }
    .warn-box { background: #FEF9E7; border-left: 4px solid #F39C12;
                padding: 0.8rem 1rem; border-radius: 4px; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="title">📋 Bhavcopy — NSE/BSE Closing Prices</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Paste your stock × date matrix from Excel. Get NSE stock and index (NSE + BSE) closing prices back instantly.</div>', unsafe_allow_html=True)

# ── HOW TO USE ───────────────────────────────────────────────
with st.expander("📖 How to use", expanded=False):
    st.markdown("""
**Step 1 — Prepare your table in Excel:**
```
Symbol      29-Sep-25   02-Nov-25   17-Apr-26
TVSMOTOR
HCLTECH
TATASTEEL
NIFTY50
SENSEX
BANKNIFTY
```
- First column = NSE ticker symbols **or index names** (see supported indexes below)
- First row = dates in any format (29-Sep-25, 29/09/2025, 2025-09-29 — all work)
- Leave all price cells empty — the app fills them

**Step 2 — Select all in Excel (Ctrl+A) → Copy (Ctrl+C)**

**Step 3 — Paste below → click Fetch Prices**

**Step 4 — Download filled matrix as Excel**

**Supported index names (type exactly as shown):**

| Category | Names you can type |
|---|---|
| NSE Broad | `NIFTY50`, `NIFTY100`, `NIFTY200`, `NIFTY500`, `NIFTYNEXT50` |
| NSE Mid/Small | `NIFTYMIDCAP100`, `NIFTYMID100`, `NIFTYSMALLCAP100`, `NIFTYSC100` |
| NSE Sectoral | `BANKNIFTY`, `NIFTYIT`, `NIFTYAUTO`, `NIFTYPHARMA`, `NIFTYFMCG`, `NIFTYMETAL`, `NIFTYREALTY`, `NIFTYENERGY`, `NIFTYPSUBANK`, `FINNIFTY` |
| BSE Broad | `SENSEX`, `BSE100`, `BSE200`, `BSE500` |
| BSE Mid/Small | `BSEMIDCAP`, `BSESMALLCAP` |
| BSE Sectoral | `BSEBANK`, `BSEIT`, `BSEAUTO`, `BSEPHARMA`, `BSEFMCG`, `BSEMETAL`, `BSEREALTY`, `BSEENERGY` |
| Volatility | `INDIAVIX`, `VIX` |

**Notes:**
- Market holidays and weekends will show as blank
- Stocks: use exact NSE ticker symbols (M&M, BAJAJ-AUTO, L&T etc.)
- Large requests (50+ rows × many dates) will take 2–4 minutes — be patient
    """)

# ── INPUT ────────────────────────────────────────────────────
st.markdown("### Paste your table here")
st.markdown('<div class="info-box">Copy your Symbol × Date table from Excel and paste below. First column = NSE stock symbols <strong>or index names</strong> (NIFTY50, SENSEX, BANKNIFTY…), first row = dates, rest left blank.</div>', unsafe_allow_html=True)

raw_input = st.text_area(
    label="Paste here",
    height=220,
    placeholder="Symbol\t29-Sep-25\t02-Nov-25\t17-Apr-26\nTVSMOTOR\t\t\t\nHCLTECH\t\t\t\nTATASTEEL\t\t\t",
    label_visibility="collapsed"
)

# ── PARSE DATE ───────────────────────────────────────────────
def parse_date_flexible(date_str):
    date_str = str(date_str).strip()
    if not date_str or date_str.lower() in ('nan', 'none', ''):
        return None
    formats = [
        "%d-%b-%y", "%d-%b-%Y",
        "%d/%m/%Y", "%d/%m/%y",
        "%Y-%m-%d",
        "%d-%m-%Y", "%d-%m-%y",
        "%b %d %Y", "%b %d, %Y",
        "%d %b %Y", "%d %b %y",
        "%m/%d/%Y",
        "%Y%m%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(date_str, dayfirst=True).date()
    except:
        return None

# ── PARSE INPUT ──────────────────────────────────────────────
def parse_input(raw):
    lines = raw.strip().split('\n')
    if len(lines) < 2:
        return None, None, "Need at least a header row and one stock row."

    header      = lines[0].split('\t')
    date_strings = [h.strip() for h in header[1:] if h.strip()]

    dates, unparsed = [], []
    for ds in date_strings:
        d = parse_date_flexible(ds)
        if d:
            dates.append((ds, d))
        else:
            unparsed.append(ds)

    if not dates:
        return None, None, "Could not parse any dates from the header row. Check date format."

    symbols = []
    for line in lines[1:]:
        parts = line.split('\t')
        sym = parts[0].strip().upper()
        if sym:
            symbols.append(sym)

    if not symbols:
        return None, None, "No stock symbols found in the first column."

    return symbols, dates, unparsed

# ── TICKER ALIAS MAP ─────────────────────────────────────────
# Maps common shorthand / alternate names → correct Yahoo Finance NSE ticker
TICKER_ALIASES = {
    "GMDC":         "GMDCLTD",
    "LT":           "LT",
    "M&M":          "M&M",
    "MM":           "M&M",
    "BAJAJ AUTO":   "BAJAJ-AUTO",
    "BAJAJ_AUTO":   "BAJAJ-AUTO",
    "NMDCSTEEL":    "NSLNISP",
    "MOTHERSUMI":   "MOTHERSON",
    "LTIM":         "LTIM",
    "LTIMINDTREE":  "LTIM",
    "HINDZINC":     "HINDZINC",
}

# ── INDEX TICKER MAP ──────────────────────────────────────────
# Maps common index names (what analysts type) → Yahoo Finance ticker
# Indexes have no .NS / .BO suffix — they use ^ or .BO format directly
INDEX_TICKERS = {
    # ── NSE Broad Market ─────────────────────────────────────
    "NIFTY50":          "^NSEI",
    "NIFTY 50":         "^NSEI",
    "NIFTY":            "^NSEI",
    "NIFTY100":         "^CNX100",
    "NIFTY 100":        "^CNX100",
    "NIFTY200":         "^CNX200",
    "NIFTY 200":        "^CNX200",
    "NIFTY500":         "^CNX500",
    "NIFTY 500":        "^CNX500",
    "NIFTYNEXT50":      "^NSMIDCP50",   # Nifty Next 50
    "NIFTY NEXT 50":    "^NSMIDCP50",
    "NIFTYJR":          "^NSMIDCP50",

    # ── NSE Midcap / Smallcap ────────────────────────────────
    "NIFTYMIDCAP50":    "^NSEMDCP50",
    "NIFTYMID50":       "^NSEMDCP50",
    "NIFTYMIDCAP100":   "^CNXMIDCAP",
    "NIFTYMID100":      "^CNXMIDCAP",
    "NIFTYMIDCAP150":   "NIFTYMIDCAP150.NS",
    "NIFTYMID150":      "NIFTYMIDCAP150.NS",
    "NIFTYSMALLCAP50":  "^CNXSC",
    "NIFTYSC50":        "^CNXSC",
    "NIFTYSMALLCAP100": "^CNXSC",
    "NIFTYSC100":       "^CNXSC",
    "NIFTYSMALLCAP250": "NIFTYSMALLCAP250.NS",
    "NIFTYSC250":       "NIFTYSMALLCAP250.NS",
    "NIFTYMICROCAP250": "NIFTYMICROCAP250.NS",

    # ── NSE Sectoral ─────────────────────────────────────────
    "BANKNIFTY":        "^NSEBANK",
    "NIFTYBANK":        "^NSEBANK",
    "NIFTY BANK":       "^NSEBANK",
    "NIFTYIT":          "^CNXIT",
    "NIFTY IT":         "^CNXIT",
    "NIFTYAUTO":        "^CNXAUTO",
    "NIFTY AUTO":       "^CNXAUTO",
    "NIFTYPHARMA":      "^CNXPHARMA",
    "NIFTY PHARMA":     "^CNXPHARMA",
    "NIFTYFMCG":        "^CNXFMCG",
    "NIFTY FMCG":       "^CNXFMCG",
    "NIFTYMETAL":       "^CNXMETAL",
    "NIFTY METAL":      "^CNXMETAL",
    "NIFTYREALTY":      "^CNXREALTY",
    "NIFTY REALTY":     "^CNXREALTY",
    "NIFTYENERGY":      "^CNXENERGY",
    "NIFTY ENERGY":     "^CNXENERGY",
    "NIFTYINFRA":       "^CNXINFRA",
    "NIFTY INFRA":      "^CNXINFRA",
    "NIFTYMEDIA":       "^CNXMEDIA",
    "NIFTY MEDIA":      "^CNXMEDIA",
    "NIFTYPSUBANK":     "^CNXPSUBANK",
    "NIFTY PSU BANK":   "^CNXPSUBANK",
    "NIFTYPSU":         "^CNXPSE",
    "NIFTY PSU":        "^CNXPSE",
    "NIFTYPSE":         "^CNXPSE",
    "NIFTYFINSERVICE":  "NIFTYFINSERVICE.NS",
    "NIFTY FIN SERVICE":"NIFTYFINSERVICE.NS",
    "FINNIFTY":         "NIFTYFINSERVICE.NS",
    "NIFTYHEALTHCARE":  "^CNXPHARMA",   # closest proxy
    "NIFTYCONSUMER":    "NIFTYCONSUMPTION.NS",
    "NIFTYOILGAS":      "NIFTYOILGAS.NS",
    "NIFTYMFG":         "NIFTYMFG.NS",
    "NIFTYDEFENCE":     "NIFTYDEFENCE.NS",

    # ── NSE Strategy / Other ────────────────────────────────
    "NIFTYALPHA50":     "NIFTYALPHA50.NS",
    "NIFTYDIVIDEND":    "^CNXDIVID",
    "NIFTYGROWTH":      "^CNXGRW25",
    "NIFTYCPSE":        "^CNXCPSE",
    "INDIA VIX":        "^INDIAVIX",
    "INDIAVIX":         "^INDIAVIX",
    "VIX":              "^INDIAVIX",

    # ── BSE Broad Market ────────────────────────────────────
    "SENSEX":           "^BSESN",
    "BSE SENSEX":       "^BSESN",
    "BSE30":            "^BSESN",
    "BSE100":           "BSE-100.BO",
    "BSE 100":          "BSE-100.BO",
    "BSE200":           "BSE-200.BO",
    "BSE 200":          "BSE-200.BO",
    "BSE500":           "BSE-500.BO",
    "BSE 500":          "BSE-500.BO",

    # ── BSE Midcap / Smallcap ───────────────────────────────
    "BSEMIDCAP":        "BSE-MIDCAP.BO",
    "BSE MIDCAP":       "BSE-MIDCAP.BO",
    "BSESMALLCAP":      "BSE-SMLCAP.BO",
    "BSE SMALLCAP":     "BSE-SMLCAP.BO",
    "BSE SMLCAP":       "BSE-SMLCAP.BO",
    "BSEMICROCAP":      "BSE-MICROCAP.BO",
    "BSE MICROCAP":     "BSE-MICROCAP.BO",
    "BSELARGECAP":      "BSE-LARGECAP.BO",
    "BSE LARGECAP":     "BSE-LARGECAP.BO",

    # ── BSE Sectoral ────────────────────────────────────────
    "BSEBANK":          "BSE-BANK.BO",
    "BSE BANK":         "BSE-BANK.BO",
    "BSEIT":            "BSE-IT.BO",
    "BSE IT":           "BSE-IT.BO",
    "BSEAUTO":          "BSE-AUTO.BO",
    "BSE AUTO":         "BSE-AUTO.BO",
    "BSEPHARMA":        "BSE-HEALTHCARE.BO",
    "BSE PHARMA":       "BSE-HEALTHCARE.BO",
    "BSEHEALTHCARE":    "BSE-HEALTHCARE.BO",
    "BSE HEALTHCARE":   "BSE-HEALTHCARE.BO",
    "BSEFMCG":          "BSE-FMCG.BO",
    "BSE FMCG":         "BSE-FMCG.BO",
    "BSEMETAL":         "BSE-METAL.BO",
    "BSE METAL":        "BSE-METAL.BO",
    "BSEREALTY":        "BSE-REALTY.BO",
    "BSE REALTY":       "BSE-REALTY.BO",
    "BSEENERGY":        "BSE-ENERGY.BO",
    "BSE ENERGY":       "BSE-ENERGY.BO",
    "BSEOILGAS":        "BSE-OIL&GAS.BO",
    "BSE OIL GAS":      "BSE-OIL&GAS.BO",
    "BSECAPGOODS":      "BSE-CARGDS.BO",
    "BSE CAP GOODS":    "BSE-CARGDS.BO",
    "BSEPSU":           "BSE-PSU.BO",
    "BSE PSU":          "BSE-PSU.BO",
    "BSEFINANCE":       "BSE-FIN.BO",
    "BSE FINANCE":      "BSE-FIN.BO",
    "BSEPOWER":         "BSE-POWER.BO",
    "BSE POWER":        "BSE-POWER.BO",
    "BSETECK":          "BSE-TECK.BO",
    "BSE TECK":         "BSE-TECK.BO",
    "BSECONSUMERDURABLES": "BSE-CONSDUR.BO",
    "BSE CONSUMER DURABLES": "BSE-CONSDUR.BO",
    "BSEINDUSTRIALS":   "BSE-INDUS.BO",
    "BSE INDUSTRIALS":  "BSE-INDUS.BO",
    "BSETELECOMMUNICATION": "BSE-TELECOM.BO",
    "BSE TELECOM":      "BSE-TELECOM.BO",
    "BSEUTILS":         "BSE-UTILS.BO",
    "BSE UTILITIES":    "BSE-UTILS.BO",
}

def is_index(symbol):
    """Returns True if symbol is a known index name."""
    return symbol.strip().upper() in INDEX_TICKERS

def to_yf_ticker(symbol):
    symbol = symbol.strip().upper()
    # Check index map first — indexes have their own Yahoo tickers, no .NS suffix
    if symbol in INDEX_TICKERS:
        return INDEX_TICKERS[symbol]
    # Apply stock alias if exists, then append .NS
    symbol = TICKER_ALIASES.get(symbol, symbol)
    return symbol + ".NS"

# ── FETCH PRICES ─────────────────────────────────────────────
MAX_RETRIES   = 3      # retry each symbol up to 3 times on failure
RETRY_DELAY   = 4      # seconds to wait between retries
REQUEST_DELAY = 0.5    # seconds between each symbol (avoids rate-limiting)

def fetch_single(ticker, fetch_start, fetch_end):
    """Download data for one ticker with retries. Returns DataFrame or raises."""
    for attempt in range(1, MAX_RETRIES + 1):
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
                return df
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise e
    return pd.DataFrame()   # all retries exhausted


def fetch_prices(symbols, date_objects):
    min_date = min(date_objects)
    max_date = max(date_objects)

    fetch_start = (min_date - timedelta(days=7)).strftime("%Y-%m-%d")
    fetch_end   = (max_date + timedelta(days=8)).strftime("%Y-%m-%d")

    results       = {}
    failed        = []
    failed_errors = {}   # symbol → reason, shown in the warning box

    progress = st.progress(0, text="Starting…")
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        progress.progress(int(i / total * 100), text=f"Fetching {symbol}  ({i+1} of {total})")
        ticker = to_yf_ticker(symbol)

        try:
            df = fetch_single(ticker, fetch_start, fetch_end)

            if df.empty:
                failed.append(symbol)
                if is_index(symbol):
                    failed_errors[symbol] = f"No data for index {ticker} — may not be available on Yahoo Finance for this date range"
                else:
                    failed_errors[symbol] = f"No data returned for {ticker} — verify NSE ticker"
                results[symbol] = {}
                time.sleep(REQUEST_DELAY)
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df.index = pd.to_datetime(df.index).date
            close_map = {}

            for dt in date_objects:
                if dt in df.index:
                    val = df.loc[dt, "Close"]
                    close_map[dt] = round(float(val), 2) if pd.notna(val) else None
                else:
                    close_map[dt] = None   # holiday / weekend → blank

            results[symbol] = close_map

        except Exception as e:
            failed.append(symbol)
            failed_errors[symbol] = str(e)[:120]
            results[symbol] = {}

        time.sleep(REQUEST_DELAY)

    progress.progress(100, text="Done!")
    return results, failed, failed_errors

# ── BUILD OUTPUT ─────────────────────────────────────────────
def build_output(symbols, dates_with_labels, price_data):
    rows = []
    for sym in symbols:
        row = {"Symbol": sym}
        for orig, dt in dates_with_labels:
            row[orig] = price_data.get(sym, {}).get(dt, None)
        rows.append(row)
    return pd.DataFrame(rows)

# ── EXCEL EXPORT ─────────────────────────────────────────────
def to_excel(df):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Bhavcopy")

        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter

        ws  = writer.sheets["Bhavcopy"]
        navy = PatternFill("solid", fgColor="1F3864")
        thin = Side(style="thin", color="D0D0D0")
        bdr  = Border(left=thin, right=thin, top=thin, bottom=thin)

        # Header row
        for col in range(1, len(df.columns) + 1):
            c = ws.cell(row=1, column=col)
            c.fill = navy
            c.font = Font(name="Arial", color="FFFFFF", bold=True, size=10)
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border = bdr

        # Data rows
        for row in range(2, len(df) + 2):
            # Symbol column
            c = ws.cell(row=row, column=1)
            c.font = Font(name="Arial", bold=True, size=10)
            c.border = bdr
            # Price columns
            for col in range(2, len(df.columns) + 1):
                c = ws.cell(row=row, column=col)
                c.font = Font(name="Arial", size=10)
                c.number_format = "#,##0.00"
                c.alignment = Alignment(horizontal="right")
                c.border = bdr

        # Column widths
        ws.column_dimensions["A"].width = 16
        for col in range(2, len(df.columns) + 1):
            ws.column_dimensions[get_column_letter(col)].width = 13

        ws.freeze_panes = "B2"

    buf.seek(0)
    return buf

# ── EXAMPLE FORMAT (always visible) ──────────────────────────
with st.expander("📊 See example input format", expanded=False):
    st.dataframe(
        pd.DataFrame({
            "Symbol":    ["TVSMOTOR","HCLTECH","TATASTEEL","NIFTY50","SENSEX","BANKNIFTY"],
            "29-Sep-25": [""]*6,
            "02-Nov-25": [""]*6,
            "28-Nov-25": [""]*6,
            "17-Apr-26": [""]*6,
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Mix stocks and index names freely — the app auto-detects which is which.")

# ── FETCH BUTTON (always visible) ────────────────────────────
fetch_clicked = st.button("🔄 Fetch Closing Prices", type="primary", use_container_width=True)

# ── MAIN FLOW ────────────────────────────────────────────────
if fetch_clicked:
    if not raw_input.strip():
        st.error("⚠️ Please paste your Symbol × Date table above before fetching.")
    else:
        symbols, dates_with_labels, unparsed = parse_input(raw_input)

        if symbols is None:
            st.error(dates_with_labels)   # error string in second slot
        else:
            date_objects = [d for _, d in dates_with_labels]

            index_syms = [s for s in symbols if is_index(s)]
            stock_syms = [s for s in symbols if not is_index(s)]
            breakdown = ""
            if index_syms:
                breakdown = f"&nbsp;|&nbsp; <strong>{len(stock_syms)}</strong> stocks + <strong>{len(index_syms)}</strong> indexes"

            st.markdown(f"""
            <div class="success-box">
            ✅ Parsed — <strong>{len(symbols)} rows</strong> × <strong>{len(dates_with_labels)} dates</strong>
            {breakdown}
            &nbsp;|&nbsp; Range: <strong>{min(date_objects).strftime('%d-%b-%Y')}</strong>
            to <strong>{max(date_objects).strftime('%d-%b-%Y')}</strong>
            </div>
            """, unsafe_allow_html=True)

            if unparsed:
                st.markdown(f'<div class="warn-box">⚠️ Skipped unrecognised date values: {", ".join(unparsed)}</div>', unsafe_allow_html=True)

            # Preview parsed inputs
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Symbols detected:**")
                st.write(", ".join(symbols))
            with c2:
                st.markdown("**Dates detected:**")
                preview_dates = [d.strftime('%d-%b-%Y') for _, d in dates_with_labels[:12]]
                suffix = f" … +{len(dates_with_labels)-12} more" if len(dates_with_labels) > 12 else ""
                st.write(", ".join(preview_dates) + suffix)

            st.markdown("---")

            price_data, failed, failed_errors = fetch_prices(symbols, date_objects)
            output_df = build_output(symbols, dates_with_labels, price_data)

            total_cells  = len(symbols) * len(dates_with_labels)
            filled_cells = output_df.iloc[:, 1:].notna().sum().sum()

            st.markdown(f"""
            <div class="success-box">
            ✅ <strong>Complete</strong> &nbsp;|&nbsp;
            <strong>{filled_cells:,}</strong> prices fetched &nbsp;|&nbsp;
            <strong>{total_cells - filled_cells:,}</strong> blanks (holidays / weekends) &nbsp;|&nbsp;
            <strong>{len(failed)}</strong> symbols failed
            </div>
            """, unsafe_allow_html=True)

            if failed:
                failed_lines = "<br>".join(
                    f"<strong>{s}</strong>: {failed_errors.get(s, 'unknown error')}" for s in failed
                )
                st.markdown(f'<div class="warn-box">⚠️ Failed symbols — check NSE ticker or try again:<br>{failed_lines}</div>', unsafe_allow_html=True)

            # Preview — cap at 15 date columns for display
            st.markdown("### Preview")
            preview_cols = ["Symbol"] + [orig for orig, _ in dates_with_labels[:15]]
            st.dataframe(
                output_df[preview_cols].style.format(
                    {col: "{:,.2f}" for col in preview_cols[1:]},
                    na_rep="—"
                ),
                use_container_width=True,
                height=min(400, (len(symbols) + 1) * 38),
            )
            if len(dates_with_labels) > 15:
                st.caption(f"Showing 15 of {len(dates_with_labels)} date columns. All {len(dates_with_labels)} columns included in the Excel download.")

            # Download
            st.download_button(
                label=f"⬇️ Download Full Matrix — {len(symbols)} stocks × {len(dates_with_labels)} dates (Excel)",
                data=to_excel(output_df),
                file_name=f"Bhavcopy_{datetime.today().strftime('%d%b%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )

elif raw_input.strip():
    # Input pasted but not yet fetched — show live parse preview
    symbols, dates_with_labels, unparsed = parse_input(raw_input)
    if symbols is None:
        st.error(dates_with_labels)
    else:
        date_objects = [d for _, d in dates_with_labels]
        index_syms = [s for s in symbols if is_index(s)]
        stock_syms = [s for s in symbols if not is_index(s)]
        breakdown = ""
        if index_syms:
            breakdown = f"&nbsp;|&nbsp; <strong>{len(stock_syms)}</strong> stocks + <strong>{len(index_syms)}</strong> indexes"

        st.markdown(f"""
        <div class="success-box">
        ✅ Parsed — <strong>{len(symbols)} rows</strong> × <strong>{len(dates_with_labels)} dates</strong>
        {breakdown}
        &nbsp;|&nbsp; Range: <strong>{min(date_objects).strftime('%d-%b-%Y')}</strong>
        to <strong>{max(date_objects).strftime('%d-%b-%Y')}</strong>
        &nbsp;|&nbsp; Ready — click <strong>Fetch Closing Prices</strong> above.
        </div>
        """, unsafe_allow_html=True)
        if unparsed:
            st.markdown(f'<div class="warn-box">⚠️ Skipped unrecognised date values: {", ".join(unparsed)}</div>', unsafe_allow_html=True)

# ── FOOTER ───────────────────────────────────────────────────
st.markdown("---")
st.caption("Bhavcopy v1.1  |  NSE stocks + NSE/BSE indexes via Yahoo Finance (unadjusted)  |  Blanks = market holiday or weekend  |  Built for Motilal Oswal Research")
