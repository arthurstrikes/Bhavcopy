"""
IMP NAV engine — target-weight state machine.

Models a dummy client who onboards at inception with a chosen capital and whose
portfolio is driven by the RA advice log. This is NOT a trade-log replayer: the
log states the MODEL's target weight for a symbol at a point in time, and a trade
is a consequence of the deviation gate, not of the row's existence.

Locked rules:
  - Model target weight per symbol = last chain-resolved `Issued Allocation`,
    carried forward until changed.
  - Chain resolution follows the old_wt -> new_wt trail, NEVER the `No.` column.
    `No.` ordering produces phantom holdings: HEROMOTOCO 12-Feb-2026 has row 139
    (10->0) before row 140 (0->10). By row number the position ends open at 10%;
    by chain from 0% it is buy-then-exit and correctly ends flat.
  - Same-date rows for one symbol collapse to the LAST link in the chain. Its
    price is the execution price: `Entry Price` when old_wt == 0, otherwise
    `Modified Price` (a.k.a. `Exit Price` — the price at which the model weight
    was modified).
  - Event date = any date carrying >= 1 log row. On an event date EVERY holding
    is re-targeted, not just the symbols named in the log.
  - Gate: trade only when |client actual wt - model target wt| > 1.00 percentage
    points (absolute, not relative).
  - Sizing base is the PREVIOUS EOD NAV. Client actual weights are measured at
    previous EOD closes. This keeps the rebalance non-circular.
  - Quantities floor. Sells execute before buys.
  - LIQUIDCASE / liquid ETFs are cash: excluded from holdings, zero return.
  - Negative cash is permitted but always alerted.
  - Corporate actions are NOT handled in this build (deferred by design).

Every failure path lands in the alert table with a specific reason. Nothing is
skipped silently and nothing is log-only.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd

LIQUID_KEYWORDS = ("LIQUIDCASE", "LIQUIDBEES", "LIQUIDETF")
GATE_PP = 1.0
CHAIN_TOL = 0.02

ALERT_CHAIN = "CHAIN UNRESOLVED"
ALERT_FORCED = "FORCED RETARGET (no log row)"
ALERT_ZERO_PX = "LOG PRICE ZERO"
ALERT_NO_PX = "NO PRICE - TRADE SKIPPED"
ALERT_NEG_CASH = "NEGATIVE CASH"
ALERT_NO_CLOSE = "NO EOD CLOSE"


def is_liquid(symbol: str) -> bool:
    return any(k in str(symbol).upper() for k in LIQUID_KEYWORDS)


def _num(v) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


_DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%d-%b-%y",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%d-%m-%Y",
)


def parse_date(val):
    """ISO-first. dayfirst is never used - it silently transposes ISO strings."""
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val.date()
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    try:
        return pd.to_datetime(s, dayfirst=False).date()
    except Exception:
        return None


# -- log parsing --------------------------------------------------------------

def load_log(uploaded):
    """
    Parse the historical advice log (CSV pipe-delimited or XLSX/XLS).
    Returns (df, error_message); df is None on failure.

    Dates use an explicit ISO-first format list. The previous implementation fell
    through to pd.to_datetime(dayfirst=True), which read "2025-12-01 00:00:00" as
    12-Jan-2025 and silently transposed every date whose day-of-month was <= 12
    (35.7% of rows in the reference log).
    """
    name = getattr(uploaded, "name", str(uploaded)).lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded, sep="|", dtype=str, encoding="utf-8")
        elif name.endswith((".xlsx", ".xls")):
            df = pd.read_excel(uploaded, dtype=str)
        else:
            return None, f"Unsupported file type: {name}. Upload CSV, XLSX or XLS."
    except Exception as e:
        return None, f"Could not read file: {e}"

    df.columns = [c.strip() for c in df.columns]

    def col(*names):
        for n in names:
            for c in df.columns:
                if c.strip().lower() == n.lower():
                    return c
        return None

    c_sym = col("Symbol")
    c_new = col("Issued Allocation", "New Weight")
    c_old = col("Remaining Allocation", "Old Weight")
    c_ep = col("Entry Price")
    c_mp = col("Exit Price", "Modified Price")
    c_md = col("Exit Time", "Modified Date", "Exit Date")
    c_no = col("No.", "No")

    missing = [lbl for lbl, c in [
        ("Symbol", c_sym),
        ("Issued Allocation / New Weight", c_new),
        ("Remaining Allocation / Old Weight", c_old),
        ("Entry Price", c_ep),
        ("Exit Price / Modified Price", c_mp),
        ("Exit Time / Modified Date", c_md),
    ] if c is None]
    if missing:
        return None, ("Missing required columns: " + ", ".join(missing)
                      + f".\nFound: {list(df.columns)}")

    out = pd.DataFrame()
    out["no"] = pd.to_numeric(df[c_no], errors="coerce") if c_no else range(len(df))
    out["symbol"] = (df[c_sym].astype(str).str.strip()
                     .str.replace("&amp;", "&", regex=False).str.upper())
    out["new_wt"] = df[c_new].apply(_num)
    out["old_wt"] = df[c_old].apply(_num)
    out["entry_px"] = df[c_ep].apply(_num)
    out["mod_px"] = df[c_mp].apply(_num)
    out["mod_dt"] = df[c_md].apply(parse_date)

    out = out.dropna(subset=["symbol", "mod_dt"])
    out = out[out["symbol"].str.strip() != ""]
    if out.empty:
        return None, "No parseable rows found in the log."

    out["is_liquid"] = out["symbol"].apply(is_liquid)
    return out.reset_index(drop=True), ""


# -- chain resolution ---------------------------------------------------------

def resolve_chain(rows, current_wt):
    """Order same-day rows for one symbol by the old_wt -> new_wt trail."""
    remaining, ordered, wt = list(rows), [], current_wt
    for _ in range(len(rows) + 1):
        nxt = next((r for r in remaining if abs(r["old_wt"] - wt) < CHAIN_TOL), None)
        if nxt is None:
            break
        ordered.append(nxt)
        wt = nxt["new_wt"]
        remaining.remove(nxt)
    return ordered, remaining


def build_events(log):
    """{date: {symbol: (target_wt, exec_price)}}, collapsed to the last chain link."""
    events = defaultdict(dict)
    alerts = []
    state = {}

    for d in sorted(log["mod_dt"].unique()):
        day = log[log["mod_dt"] == d]
        for symbol, grp in day.groupby("symbol"):
            current = state.get(symbol, 0.0)
            ordered, leftover = resolve_chain(grp.to_dict("records"), current)
            if leftover:
                alerts.append(dict(
                    Date=d, Symbol=symbol, Type=ALERT_CHAIN,
                    Detail=(f"{len(leftover)} row(s) do not sit on the old-to-new trail "
                            f"from {current:.2f}%; appended by No. as a fallback"),
                ))
                ordered = ordered + sorted(leftover, key=lambda r: r["no"])
            if not ordered:
                continue
            last = ordered[-1]
            px = last["entry_px"] if last["old_wt"] == 0 else last["mod_px"]
            events[d][symbol] = (float(last["new_wt"]), float(px))
            state[symbol] = float(last["new_wt"])

    return dict(events), alerts


# -- engine -------------------------------------------------------------------

def run_nav(log, closes, capital, start, to_date,
            gate_pp=GATE_PP, force_retarget=True):
    """
    closes : {date: {SYMBOL: close}} - trading days only (a 404 from the NSE
             archive is the authoritative "no session" signal).

    Returns (nav_rows, trades, alerts) as lists of dicts.
    """
    equity_log = log[~log["is_liquid"]]
    events, alerts = build_events(equity_log)

    # Advice dated before the chosen start is executed on the start date.
    clamped = defaultdict(dict)
    for d, syms in sorted(events.items()):
        clamped[start if d < start else d].update(syms)
    events = dict(clamped)

    calendar = [d for d in sorted(closes) if start <= d <= to_date]
    if not calendar:
        raise ValueError("No NSE trading sessions in the selected range.")

    model, portfolio, last_px, seen_missing = {}, {}, {}, set()
    cash = float(capital)
    base_nav = float(capital)
    nav_rows, trades = [], []

    for day in calendar:
        today = closes[day]

        if day in events:
            for symbol, (wt, _) in events[day].items():
                if wt == 0:
                    model.pop(symbol, None)
                else:
                    model[symbol] = wt

            universe = set(model) | set(portfolio)
            if not force_retarget:
                universe &= set(events[day])

            plan = []
            for symbol in sorted(universe):
                target = model.get(symbol, 0.0)
                held = portfolio.get(symbol, 0)
                mark = last_px.get(symbol, 0.0)
                actual = (held * mark / base_nav * 100.0) if base_nav else 0.0
                if abs(actual - target) <= gate_pp:
                    continue

                if symbol in events[day]:
                    price, source = events[day][symbol][1], "log"
                else:
                    price, source = today.get(symbol, 0.0), "eod-forced"
                    alerts.append(dict(
                        Date=day, Symbol=symbol, Type=ALERT_FORCED,
                        Detail=(f"model {target:.2f}% vs client {actual:.2f}% "
                                f"(dev {actual - target:+.2f}pp); no log row today, "
                                f"filled at EOD close {price}"),
                    ))

                if price <= 0:
                    fallback = today.get(symbol, 0.0)
                    alerts.append(dict(
                        Date=day, Symbol=symbol, Type=ALERT_ZERO_PX,
                        Detail=(f"log price is 0 - substituted EOD close {fallback}"
                                if fallback > 0 else "log price is 0 and no EOD close"),
                    ))
                    price, source = fallback, "eod-zerofill"

                if price <= 0:
                    alerts.append(dict(
                        Date=day, Symbol=symbol, Type=ALERT_NO_PX,
                        Detail=f"target {target:.2f}%, client {actual:.2f}% - no usable price",
                    ))
                    continue

                want = 0 if target == 0 else math.floor(base_nav * target / 100.0 / price)
                if want != held:
                    plan.append((symbol, want - held, price, target, actual, source))

            # sells first, then buys
            for symbol, delta, price, target, actual, source in sorted(
                    plan, key=lambda p: (p[1] >= 0, p[0])):
                cash -= delta * price
                portfolio[symbol] = portfolio.get(symbol, 0) + delta
                if portfolio[symbol] <= 0:
                    portfolio.pop(symbol, None)
                    last_px.pop(symbol, None)
                else:
                    last_px[symbol] = price
                trades.append(dict(
                    Date=day, Symbol=symbol, Side="BUY" if delta > 0 else "SELL",
                    Qty=abs(delta), Price=round(price, 2),
                    Value=round(abs(delta) * price, 2),
                    ModelWt=round(target, 2), ClientWtBefore=round(actual, 2),
                    DeviationPP=round(actual - target, 2), PriceSource=source,
                ))

            if cash < 0:
                alerts.append(dict(
                    Date=day, Symbol="-", Type=ALERT_NEG_CASH,
                    Detail=f"cash {cash:,.2f} after rebalance - capital insufficient "
                           f"for the model at this ticket size",
                ))

        # mark to market
        for symbol in list(portfolio):
            if symbol in today:
                last_px[symbol] = today[symbol]
            elif symbol not in seen_missing:
                seen_missing.add(symbol)
                alerts.append(dict(
                    Date=day, Symbol=symbol, Type=ALERT_NO_CLOSE,
                    Detail="absent from NSE cash bhavcopy (EQ/BE/SM/ST) - "
                           "carried at last known price; verify the NSE symbol",
                ))

        mkt_val = sum(q * last_px.get(s, 0.0) for s, q in portfolio.items())
        nav = mkt_val + cash
        prev = nav_rows[-1]["NAV"] if nav_rows else float(capital)
        holdings = {s: dict(qty=q, price=last_px.get(s, 0.0),
                            value=q * last_px.get(s, 0.0),
                            model_wt=model.get(s, 0.0))
                    for s, q in portfolio.items()}

        nav_rows.append(dict(
            Date=day, MarketValue=round(mkt_val, 2), Cash=round(cash, 2),
            NAV=round(nav, 2), Rebased=round(nav / capital * 100.0, 4),
            DayPL=round(nav - prev, 2),
            DayReturnPct=round((nav - prev) / prev * 100.0, 4) if prev else 0.0,
            Positions=len(portfolio),
            Type="TRADE" if day in events else "MTM",
            _holdings=holdings,
        ))
        base_nav = nav

    return nav_rows, trades, alerts


def reconciliation(nav_rows):
    """Per-day, per-holding model weight vs achieved weight - the audit artefact."""
    out = []
    for row in nav_rows:
        nav = row["NAV"]
        for symbol, h in sorted(row["_holdings"].items()):
            achieved = (h["value"] / nav * 100.0) if nav else 0.0
            out.append(dict(
                Date=row["Date"], Symbol=symbol, Qty=h["qty"],
                Close=round(h["price"], 2), Value=round(h["value"], 2),
                ModelWt=round(h["model_wt"], 2), AchievedWt=round(achieved, 2),
                DriftPP=round(achieved - h["model_wt"], 2),
            ))
    return pd.DataFrame(out)


def calendar_days(start, end):
    """
    Every calendar day in range - do NOT pre-filter weekends.

    NSE runs special weekend sessions (Budget day, muhurat, DR drills). The
    reference range contains one: Sunday 01-Feb-2026 traded. A weekday-only
    calendar drops it silently. The archive's 404 is the authoritative
    non-trading-day signal.
    """
    days, cur = [], start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days
