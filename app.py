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

st.markdown('<div class="title">📋 Bhavcopy — NSE Closing Prices</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Paste your stock × date matrix from Excel. Get NSE closing prices back instantly.</div>', unsafe_allow_html=True)

# ── HOW TO USE ───────────────────────────────────────────────
with st.expander("📖 How to use", expanded=False):
    st.markdown("""
**Step 1 — Prepare your table in Excel:**
```
Symbol      29-Sep-25   02-Nov-25   17-Apr-26
TVSMOTOR
HCLTECH
TATASTEEL
```
- First column = NSE ticker symbols (e.g. HCLTECH, M&M, TATASTEEL)
- First row = dates in any format (29-Sep-25, 29/09/2025, 2025-09-29 — all work)
- Leave all price cells empty — the app fills them

**Step 2 — Select all in Excel (Ctrl+A) → Copy (Ctrl+C)**

**Step 3 — Paste below → click Fetch Prices**

**Step 4 — Download filled matrix as Excel**

**Notes:**
- Market holidays and weekends will show as blank
- Use exact NSE ticker symbols
- M&M, BAJAJ-AUTO, L&T etc. — use the NSE code exactly as listed
- Large requests (50+ stocks × many dates) will take 2–4 minutes — be patient
    """)

# ── INPUT ────────────────────────────────────────────────────
st.markdown("### Paste your table here")
st.markdown('<div class="info-box">Copy your Symbol × Date table from Excel and paste below. First column = symbols, first row = dates, rest left blank.</div>', unsafe_allow_html=True)

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

# ── YAHOO FINANCE TICKER ─────────────────────────────────────
def to_yf_ticker(symbol):
    return symbol.strip().upper() + ".NS"

# ── FETCH PRICES ─────────────────────────────────────────────
MAX_RETRIES   = 3          # retry each symbol up to 3 times
RETRY_DELAY   = 4          # seconds to wait between retries
REQUEST_DELAY = 0.5        # seconds between each symbol fetch (avoids rate-limiting)

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
                return df          # success
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise e
    return pd.DataFrame()          # all retries exhausted → empty


def fetch_prices(symbols, date_objects):
    min_date = min(date_objects)
    max_date = max(date_objects)

    fetch_start = (min_date - timedelta(days=7)).strftime("%Y-%m-%d")
    fetch_end   = (max_date + timedelta(days=8)).strftime("%Y-%m-%d")

    results      = {}
    failed       = []
    failed_errors = {}   # symbol → error reason for display

    progress = st.progress(0, text="Starting…")
    total = len(symbols)

    for i, symbol in enumerate(symbols):
        progress.progress(int(i / total * 100), text=f"Fetching {symbol}  ({i+1} of {total})")
        ticker = to_yf_ticker(symbol)

        try:
            df = fetch_single(ticker, fetch_start, fetch_end)

            if df.empty:
                failed.append(symbol)
                failed_errors[symbol] = "No data returned (check NSE ticker or date range)"
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

        time.sleep(REQUEST_DELAY)   # polite delay between each symbol

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

# ── MAIN FLOW ────────────────────────────────────────────────
if raw_input.strip():
    symbols, dates_with_labels, unparsed = parse_input(raw_input)

    if symbols is None:
        st.error(dates_with_labels)   # error string in second slot
    else:
        date_objects = [d for _, d in dates_with_labels]

        st.markdown(f"""
        <div class="success-box">
        ✅ Parsed — <strong>{len(symbols)} stocks</strong> × <strong>{len(dates_with_labels)} dates</strong>
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

        if st.button("🔄 Fetch Closing Prices", type="primary", use_container_width=True):
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
                failed_details = " &nbsp;|&nbsp; ".join(
                    f"<strong>{s}</strong>: {failed_errors.get(s, 'unknown error')}" for s in failed
                )
                st.markdown(f'<div class="warn-box">⚠️ Failed symbols — {failed_details}</div>', unsafe_allow_html=True)

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

else:
    # Show example when nothing pasted yet
    st.markdown("### Example input format")
    st.dataframe(
        pd.DataFrame({
            "Symbol":    ["TVSMOTOR","HCLTECH","TATASTEEL","CANBK","KIRLOSENG"],
            "29-Sep-25": [""]*5,
            "02-Nov-25": [""]*5,
            "28-Nov-25": [""]*5,
            "17-Apr-26": [""]*5,
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.caption("Replicate this structure in Excel — symbols in rows, dates in columns, prices blank. Copy → paste above.")

# ── FOOTER ───────────────────────────────────────────────────
st.markdown("---")
st.caption("Bhavcopy v1.0  |  NSE closing prices via Yahoo Finance (unadjusted)  |  Blanks = market holiday or weekend  |  Built for Motilal Oswal Research")
