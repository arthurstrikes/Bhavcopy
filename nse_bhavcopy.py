"""
NSE Bhavcopy price source.

Fetches official NSE end-of-day prices directly from NSE's static archive host
(nsearchives.nseindia.com). This is NOT the nseindia.com website — the archive
host serves plain static files with no session tokens, no cookies and no bot
detection, so it does not have the blocking problem the main NSE site has.

Why this beats yfinance for this use case:
  - No rate limiting. It is a static file host, not a query API.
  - One HTTP request per DATE returns every NSE symbol (~3,100 cash scrips),
    instead of one request per SYMBOL. Fetching 76 symbols for 3 dates costs
    3 requests here vs 76 with yfinance.
  - Official exchange close prices (authoritative source of record), not a
    third-party mirror.
  - Free, no API key, no account, no static-IP registration.

Trade-off to be aware of: cost scales with number of DATES, not symbols. For a
long daily NAV series (hundreds of trading days) the per-date model means many
requests, so fetches are threaded and cached.

Endpoints (both verified live):
  Equities : /content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip
  Indices  : /content/indices/ind_close_all_{DDMMYYYY}.csv

Non-trading days (weekends/holidays) return HTTP 404 — this is the normal,
expected signal for "no session on this date", not an error.
"""

from __future__ import annotations

import io
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import pandas as pd
import requests
import streamlit as st

CM_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d}_F_0000.csv.zip"
IDX_URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_{d}.csv"

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 20
MAX_WORKERS = 8

# Cash-market series. TRIVENI trades in 'BE', not 'EQ' — filtering to EQ only
# would silently drop symbols, so all cash series are included.
CASH_SERIES = ("EQ", "BE", "SM", "ST")

# NSE index bhavcopy uses display names. Map the tickers used in this app.
INDEX_ALIASES = {
    "NIFTY": "Nifty 50",
    "NIFTY50": "Nifty 50",
    "NIFTY 50": "Nifty 50",
    "NIFTY500": "Nifty 500",
    "NIFTY 500": "Nifty 500",
    "NIFTYNEXT50": "Nifty Next 50",
    "NIFTYBANK": "Nifty Bank",
    "BANKNIFTY": "Nifty Bank",
    "NIFTYMIDCAP150": "Nifty Midcap 150",
    "NIFTYSMLCAP250": "Nifty Smallcap 250",
    "NIFTY100": "Nifty 100",
    "NIFTY200": "Nifty 200",
}


class NoSessionOnDate(Exception):
    """Non-trading day (weekend / exchange holiday). Expected, not a failure."""


class BhavcopyFetchError(Exception):
    """Genuine fetch failure — network error or unexpected HTTP status."""


# ── single-date fetchers ──────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def fetch_equity_bhavcopy(d: date) -> dict:
    """
    {TckrSymb: close_price} for one trading date, cash segment only.

    Cached for 7 days: a past date's bhavcopy is immutable once published, so
    repeated Streamlit reruns never re-hit the network.

    Raises NoSessionOnDate on 404 (weekend/holiday).
    """
    url = CM_URL.format(d=d.strftime("%Y%m%d"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except Exception as e:
        raise BhavcopyFetchError(f"Network error for {d}: {e}") from e

    if r.status_code == 404:
        raise NoSessionOnDate(str(d))
    if r.status_code != 200:
        raise BhavcopyFetchError(f"HTTP {r.status_code} for {d}")

    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        name = zf.namelist()[0]
        df = pd.read_csv(zf.open(name))
    except Exception as e:
        raise BhavcopyFetchError(f"Could not parse bhavcopy for {d}: {e}") from e

    df.columns = [c.strip() for c in df.columns]
    if "TckrSymb" not in df.columns or "ClsPric" not in df.columns:
        raise BhavcopyFetchError(f"Unexpected bhavcopy layout for {d}")

    cash = df[df["SctySrs"].astype(str).str.strip().isin(CASH_SERIES)]
    cash = cash[["TckrSymb", "ClsPric"]].dropna()

    out = {}
    for sym, px in zip(cash["TckrSymb"], cash["ClsPric"]):
        try:
            px = float(px)
        except (TypeError, ValueError):
            continue
        if px > 0:
            out[str(sym).strip().upper()] = px
    return out


@st.cache_data(show_spinner=False, ttl=7 * 24 * 3600)
def fetch_index_bhavcopy(d: date) -> dict:
    """{Index Name (upper): closing value} for one trading date."""
    url = IDX_URL.format(d=d.strftime("%d%m%Y"))
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except Exception as e:
        raise BhavcopyFetchError(f"Network error (index) for {d}: {e}") from e

    if r.status_code == 404:
        raise NoSessionOnDate(str(d))
    if r.status_code != 200:
        raise BhavcopyFetchError(f"HTTP {r.status_code} (index) for {d}")

    try:
        df = pd.read_csv(io.BytesIO(r.content))
    except Exception as e:
        raise BhavcopyFetchError(f"Could not parse index file for {d}: {e}") from e

    df.columns = [c.strip() for c in df.columns]
    name_col = "Index Name"
    close_col = "Closing Index Value"
    if name_col not in df.columns or close_col not in df.columns:
        raise BhavcopyFetchError(f"Unexpected index layout for {d}")

    out = {}
    for nm, px in zip(df[name_col], df[close_col]):
        try:
            px = float(px)
        except (TypeError, ValueError):
            continue
        if px > 0:
            out[str(nm).strip().upper()] = px
    return out


def resolve_index_name(symbol: str) -> str | None:
    """Map an app-side index ticker to the NSE index bhavcopy display name."""
    s = str(symbol).strip().upper()
    if s in INDEX_ALIASES:
        return INDEX_ALIASES[s].upper()
    compact = s.replace(" ", "")
    for k, v in INDEX_ALIASES.items():
        if k.replace(" ", "") == compact:
            return v.upper()
    return None


# ── combined day snapshot ─────────────────────────────────────────────────────

def fetch_day(d: date) -> tuple[dict, bool]:
    """
    ({SYMBOL: close} incl. indices, traded_flag) for one date.

    traded_flag is False for weekends/holidays. Index failures are tolerated
    (equities still return); an equity failure propagates.
    """
    try:
        eq = fetch_equity_bhavcopy(d)
    except NoSessionOnDate:
        return {}, False

    combined = dict(eq)
    try:
        combined.update(fetch_index_bhavcopy(d))
    except (NoSessionOnDate, BhavcopyFetchError):
        pass  # indices unavailable for this date; equities are still valid
    return combined, True


# ── public API ────────────────────────────────────────────────────────────────

def fetch_closes(
    symbols,
    dates,
    fill_holidays: bool = False,
    max_lookback_days: int = 7,
    progress_cb=None,
):
    """
    Fetch official NSE closes.

    Returns (results, failed, failed_errors, holiday_fills) — the same contract
    the yfinance path used, so callers need no other changes.

      results       {symbol: {date: close|None}}
      failed        [symbols with no price on any requested date]
      failed_errors {symbol: reason}
      holiday_fills {dates where a prior-session close was carried forward}

    One request per date (all symbols come from the same file), threaded and
    cached. Cost is driven by the number of dates, not the number of symbols.
    """
    symbols = [str(s).strip().upper() for s in symbols]
    dates = sorted({d for d in dates})

    # Widen the window so holiday fill can look back before the first date.
    needed = set(dates)
    if fill_holidays:
        for d in dates:
            for k in range(1, max_lookback_days + 1):
                needed.add(d - timedelta(days=k))

    day_maps: dict[date, dict] = {}
    traded: dict[date, bool] = {}
    fetch_errors: dict[date, str] = {}

    def _one(d):
        try:
            m, ok = fetch_day(d)
            return d, m, ok, None
        except BhavcopyFetchError as e:
            return d, {}, False, str(e)[:160]

    ordered = sorted(needed)
    done = 0
    total = len(ordered)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for d, m, ok, err in ex.map(_one, ordered):
            day_maps[d] = m
            traded[d] = ok
            if err:
                fetch_errors[d] = err
            done += 1
            if progress_cb:
                progress_cb(done, total, d)

    results, failed, failed_errors = {}, [], {}
    holiday_fills = set()

    for sym in symbols:
        idx_name = resolve_index_name(sym)
        key = idx_name if idx_name else sym

        close_map = {}
        for d in dates:
            px = day_maps.get(d, {}).get(key)
            if px is not None:
                close_map[d] = round(float(px), 2)
                continue

            if traded.get(d) and key in day_maps.get(d, {}):
                close_map[d] = round(float(day_maps[d][key]), 2)
                continue

            if fill_holidays:
                filled = False
                for k in range(1, max_lookback_days + 1):
                    prior = d - timedelta(days=k)
                    ppx = day_maps.get(prior, {}).get(key)
                    if ppx is not None:
                        close_map[d] = round(float(ppx), 2)
                        holiday_fills.add(d)
                        filled = True
                        break
                if filled:
                    continue

            close_map[d] = None  # holiday/weekend, or symbol absent that day

        results[sym] = close_map

        if all(v is None for v in close_map.values()):
            failed.append(sym)
            if fetch_errors:
                sample = list(fetch_errors.values())[0]
                failed_errors[sym] = f"NSE archive fetch problem: {sample}"
            elif not any(traded.get(d) for d in dates):
                failed_errors[sym] = (
                    "No NSE trading session on any requested date "
                    "(weekend/holiday) — pick trading days or enable holiday fill"
                )
            elif idx_name:
                failed_errors[sym] = (
                    f"Index '{sym}' not found in NSE index bhavcopy "
                    f"(looked for '{idx_name}')"
                )
            else:
                failed_errors[sym] = (
                    f"'{sym}' not in NSE cash bhavcopy "
                    f"({'/'.join(CASH_SERIES)} series) — verify the NSE symbol"
                )

    return results, failed, failed_errors, holiday_fills


def available_symbols(d: date) -> list:
    """Every symbol NSE published on a date — useful for validating tickers."""
    m, ok = fetch_day(d)
    return sorted(m.keys()) if ok else []
