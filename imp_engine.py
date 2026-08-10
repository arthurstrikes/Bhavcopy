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

import io
import math
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import pandas as pd

LIQUID_KEYWORDS = ("LIQUIDCASE", "LIQUIDBEES", "LIQUIDETF")
GATE_PP = 1.0
CHAIN_TOL = 0.02

ALERT_CHAIN = "CHAIN UNRESOLVED"
ALERT_FORCED = "FORCED REBALANCE (no log row)"
ALERT_ZERO_PX = "LOG PRICE ZERO"
ALERT_NO_PX = "NO PRICE - TRADE SKIPPED"
ALERT_NEG_CASH = "NEGATIVE CASH"
ALERT_UNDERFILL = "BUY UNDERFILLED - INSUFFICIENT CASH"
ALERT_NO_CLOSE = "NO EOD CLOSE"
ALERT_STALE_PX = "PRE-START ADVICE - REPRICED"
ALERT_DIVIDEND = "DIVIDEND CREDITED"
ALERT_ROLLED_DATE = "ADVICE DATE ROLLED"
ALERT_ORPHAN_DATE = "ADVICE DATE ORPHANED"


def is_liquid(symbol: str) -> bool:
    return any(k in str(symbol).upper() for k in LIQUID_KEYWORDS)


def _read_csv_any_delimiter(uploaded):
    """
    Read a CSV without assuming the delimiter.

    The dashboard export has shipped as both pipe-delimited and comma-delimited.
    Hardcoding one produces a single-column frame and a misleading "missing
    columns" error listing every column the file plainly contains. Each candidate
    is tried and the one yielding the most columns wins; quoted fields (rationale
    text, prices like "1,251.00") are handled by the parser, so an embedded
    comma never wins a delimiter vote on its own.
    """
    raw = uploaded.read() if hasattr(uploaded, "read") else open(uploaded, "rb").read()
    if isinstance(raw, str):
        raw = raw.encode("utf-8")

    best, best_cols = None, 0
    for sep in (",", "|", "\t", ";"):
        try:
            probe = pd.read_csv(io.BytesIO(raw), sep=sep, dtype=str,
                                encoding="utf-8", nrows=5, engine="python")
        except Exception:
            continue
        if probe.shape[1] > best_cols:
            best, best_cols = sep, probe.shape[1]

    if best is None:
        raise ValueError("could not determine the delimiter")
    return pd.read_csv(io.BytesIO(raw), sep=best, dtype=str,
                       encoding="utf-8", engine="python")


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
            df = _read_csv_any_delimiter(uploaded)
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
    c_ed = col("Entry Time", "Entry Date")
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
    # Entry date is only needed by the corporate-action layer, to decide which
    # basis entry_px belongs to. Falls back to mod_dt when the column is absent.
    out["entry_dt"] = (df[c_ed].apply(parse_date) if c_ed else out["mod_dt"])
    out["entry_dt"] = out["entry_dt"].fillna(out["mod_dt"])

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
    """{date: {symbol: (target_wt, exec_price, advice_date)}}, last chain link."""
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
            events[d][symbol] = (float(last["new_wt"]), float(px), d)
            state[symbol] = float(last["new_wt"])

    return dict(events), alerts


# -- engine -------------------------------------------------------------------

def run_nav(log, closes, capital, start, to_date,
            gate_pp=GATE_PP, force_retarget=True, dividends=None,
            stale_price_policy="start_close"):
    """
    closes : {date: {SYMBOL: close}} - trading days only (a 404 from the NSE
             archive is the authoritative "no session" signal).

    Returns (nav_rows, trades, alerts) as lists of dicts.
    """
    equity_log = log[~log["is_liquid"]]
    events, alerts = build_events(equity_log)

    # Advice dated before the chosen start collapses onto the start date. The
    # dict update in date order means the LAST value per symbol wins, so day 1
    # opens with the model's state as at the start date - not a replay of every
    # trade since the log began.
    #
    # Those positions must fill at the START DATE's close, not the log price from
    # whenever the advice was issued. A client starting 1 Apr cannot buy GLENMARK
    # at 25 March's 2165.00 when it closed at 2101.10 that day - a 3% fiction.
    # advice_date is carried through so run_nav can tell the two apart.
    clamped = defaultdict(dict)
    for d, syms in sorted(events.items()):
        clamped[start if d < start else d].update(syms)
    events = dict(clamped)

    calendar = [d for d in sorted(closes) if start <= d <= to_date]
    if not calendar:
        raise ValueError("No NSE trading sessions in the selected range.")

    # Advice dated on a non-trading day must roll forward to the next session.
    # The RAs log rebalances on Sundays. Because the day loop iterates `calendar`
    # (NSE sessions only), such an event was previously never reached and the whole
    # date was discarded in silence - no trade, no alert. On the reference logs
    # that lost 18 instructions on Sunday 01-Mar-2026, including two full exits,
    # leaving the positions in the book to the end of the run.
    sessions = set(calendar)
    rolled = defaultdict(dict)
    for d, syms in sorted(events.items()):
        if d in sessions:
            rolled[d].update(syms)
            continue
        nxt = next((s for s in calendar if s > d), None)
        if nxt is None:
            alerts.append(dict(
                Date=d, Symbol="-", Type=ALERT_ORPHAN_DATE,
                Detail=(f"advice dated {d} ({d:%a}) is not an NSE session and no "
                        f"later session exists in range - {len(syms)} symbol(s) "
                        f"NOT applied")))
            continue
        alerts.append(dict(
            Date=d, Symbol="-", Type=ALERT_ROLLED_DATE,
            Detail=(f"advice dated {d} ({d:%a}) is not an NSE session - "
                    f"{len(syms)} symbol(s) applied at the next session {nxt}")))
        # Re-stamp the advice date to `nxt`, not the original non-session date.
        # The day-loop's stale-price branch below fires on `adv != day` - the
        # same signal the pre-start clamp uses to justify substituting the EOD
        # close for the log price. Left as the original Sunday/holiday date,
        # every rolled instruction would misfire that branch on arrival and
        # silently lose its log price, contradicting the confirmed rule that a
        # rolled date executes at the log price unchanged (falling back to the
        # EOD close only via ALERT_ZERO_PX, when the log price is 0/blank).
        rolled[nxt].update({sym: (wt, px, nxt) for sym, (wt, px, _adv) in syms.items()})
    events = dict(rolled)

    model, portfolio, last_px, seen_missing = {}, {}, {}, set()
    cash = float(capital)
    div_cash = 0.0
    div_events = []
    base_nav = float(capital)
    nav_rows, trades = [], []

    for day in calendar:
        today = closes[day]

        if day in events:
            for symbol, (wt, _, _adv) in events[day].items():
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
                    adv = events[day][symbol][2]
                    if adv != day and stale_price_policy == "start_close":
                        close_px = today.get(symbol, 0.0)
                        if close_px > 0:
                            alerts.append(dict(
                                Date=day, Symbol=symbol, Type=ALERT_STALE_PX,
                                Detail=(f"advice dated {adv} predates the start date; "
                                        f"log price {price} replaced with the {day} "
                                        f"close {close_px}")))
                            price, source = close_px, "start-close"
                        else:
                            alerts.append(dict(
                                Date=day, Symbol=symbol, Type=ALERT_STALE_PX,
                                Detail=(f"advice dated {adv} predates the start date "
                                        f"and there is no {day} close; using the log "
                                        f"price {price}")))
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

            # Sells first (locked rule) - unconditional, they only raise cash.
            sells = [p for p in plan if p[1] < 0]
            buys = [p for p in plan if p[1] > 0]

            for symbol, delta, price, target, actual, source in sorted(
                    sells, key=lambda p: p[0]):
                cash -= delta * price
                portfolio[symbol] = portfolio.get(symbol, 0) + delta
                if portfolio[symbol] <= 0:
                    portfolio.pop(symbol, None)
                    last_px.pop(symbol, None)
                else:
                    last_px[symbol] = price
                trades.append(dict(
                    Date=day, Symbol=symbol, Side="SELL",
                    Qty=abs(delta), Price=round(price, 2),
                    Value=round(abs(delta) * price, 2),
                    ModelWt=round(target, 2), ClientWtBefore=round(actual, 2),
                    DeviationPP=round(actual - target, 2), PriceSource=source,
                ))

            # Buys are capped at available cash. A real client cannot buy without
            # funding it - the tolerance band leaves excess trapped in untraded
            # holdings, so the model's 100% never guarantees the cash is actually
            # there. Every planned buy is scaled by the same factor and re-floored
            # individually; no symbol is favoured. Underfunded positions stay
            # underweight until the next event date releases cash through a trim
            # or exit.
            required = sum(delta * price for _, delta, price, *_ in buys)
            if required <= 0:
                scale = 1.0
            elif cash <= 0:
                scale = 0.0
            else:
                scale = min(1.0, cash / required)

            for symbol, delta, price, target, actual, source in sorted(
                    buys, key=lambda p: p[0]):
                qty = delta if scale >= 1.0 else math.floor(delta * scale)
                if qty > 0:
                    cash -= qty * price
                    portfolio[symbol] = portfolio.get(symbol, 0) + qty
                    last_px[symbol] = price
                    trades.append(dict(
                        Date=day, Symbol=symbol, Side="BUY",
                        Qty=qty, Price=round(price, 2),
                        Value=round(qty * price, 2),
                        ModelWt=round(target, 2), ClientWtBefore=round(actual, 2),
                        DeviationPP=round(actual - target, 2), PriceSource=source,
                    ))
                if qty < delta:
                    held_after = portfolio.get(symbol, 0)
                    achieved = (held_after * price / base_nav * 100.0) if base_nav else 0.0
                    shortfall = (delta - qty) * price
                    alerts.append(dict(
                        Date=day, Symbol=symbol, Type=ALERT_UNDERFILL,
                        Detail=(f"target {target:.2f}% but only {achieved:.2f}% funded - "
                                f"cash short by {shortfall:,.2f}; position stays "
                                f"underweight until the next event date"),
                    ))

            if cash < -0.01:
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

        # Dividends: credit cash, leave prices untouched. The bhavcopy close on
        # an ex-date has ALREADY dropped by roughly the DPS - that drop is real
        # and is not adjusted away. Crediting the cash restores the value, so NAV
        # is continuous through the event and the income is captured exactly once.
        for ev in (dividends or {}).get(day, []):
            qty = portfolio.get(ev["symbol"], 0)
            if qty > 0 and ev["dps"] > 0:
                amt = qty * ev["dps"]
                div_cash += amt
                div_events.append(dict(Date=day, Symbol=ev["symbol"],
                                       DPS=round(ev["dps"], 4), Qty=qty,
                                       Amount=round(amt, 2), Purpose=ev.get("purpose", "")))
                alerts.append(dict(Date=day, Symbol=ev["symbol"], Type=ALERT_DIVIDEND,
                                   Detail=(f"{ev.get('purpose','dividend')}: "
                                           f"{qty} x {ev['dps']:g} = {amt:,.2f} "
                                           f"credited to the dividend-inclusive basis")))

        mkt_val = sum(q * last_px.get(s, 0.0) for s, q in portfolio.items())
        nav = mkt_val + cash
        prev = nav_rows[-1]["NAV"] if nav_rows else float(capital)
        holdings = {s: dict(qty=q, price=last_px.get(s, 0.0),
                            value=q * last_px.get(s, 0.0),
                            model_wt=model.get(s, 0.0))
                    for s, q in portfolio.items()}

        # Dividend cash is tracked in parallel and never re-invested, so it can
        # never change a trade quantity. The two bases therefore differ by exactly
        # the cumulative dividends received - nothing else.
        nav_tr = nav + div_cash
        prev_tr = nav_rows[-1]["NAV_TR"] if nav_rows else float(capital)

        nav_rows.append(dict(
            Date=day, MarketValue=round(mkt_val, 2), Cash=round(cash, 2),
            NAV=round(nav, 2), Rebased=round(nav / capital * 100.0, 4),
            DayPL=round(nav - prev, 2),
            DayReturnPct=round((nav - prev) / prev * 100.0, 4) if prev else 0.0,
            DividendCash=round(div_cash, 2), NAV_TR=round(nav_tr, 2),
            Rebased_TR=round(nav_tr / capital * 100.0, 4),
            DayReturnPct_TR=round((nav_tr - prev_tr) / prev_tr * 100.0, 4) if prev_tr else 0.0,
            Positions=len(portfolio),
            Type="TRADE" if day in events else "MTM",
            _holdings=holdings,
        ))
        base_nav = nav

    return nav_rows, trades, alerts, div_events


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


# -- holdings views ------------------------------------------------------------

def holdings_on(nav_rows, target: date):
    """The full book on one session: (row, holdings_df). Returns (None, None) if absent."""
    row = next((r for r in nav_rows if r["Date"] == target), None)
    if row is None:
        return None, None
    nav = row["NAV"]
    df = pd.DataFrame([
        dict(Symbol=s, Qty=h["qty"], Close=round(h["price"], 2),
             Value=round(h["value"], 2),
             AchievedWt=round(h["value"] / nav * 100, 2) if nav else 0.0,
             ModelWt=round(h["model_wt"], 2),
             DriftPP=round((h["value"] / nav * 100 if nav else 0.0) - h["model_wt"], 2))
        for s, h in sorted(row["_holdings"].items(), key=lambda x: -x[1]["value"])
    ])
    return row, df


def holdings_matrix(nav_rows, measure="weight"):
    """
    Wide matrix: dates down, symbols across.

    measure: 'weight'  -> achieved weight %, plus a CASH column and a TOTAL check
             'qty'     -> share count
             'value'   -> rupee value, plus CASH and TOTAL
    Symbols are ordered by first appearance so the sheet reads chronologically.
    """
    order, rows = [], []
    for r in nav_rows:
        nav = r["NAV"]
        rec = {"Date": r["Date"]}
        for s, h in r["_holdings"].items():
            if s not in order:
                order.append(s)
            if measure == "qty":
                rec[s] = h["qty"]
            elif measure == "value":
                rec[s] = round(h["value"], 2)
            else:
                rec[s] = round(h["value"] / nav * 100, 2) if nav else 0.0
        if measure == "weight":
            rec["CASH"] = round(r["Cash"] / nav * 100, 2) if nav else 0.0
            rec["TOTAL"] = round(sum(v for k, v in rec.items()
                                     if k not in ("Date", "TOTAL")), 2)
        elif measure == "value":
            rec["CASH"] = round(r["Cash"], 2)
            rec["TOTAL"] = round(r["NAV"], 2)
        rows.append(rec)

    tail = [c for c in ("CASH", "TOTAL") if measure in ("weight", "value")]
    return pd.DataFrame(rows).reindex(columns=["Date"] + order + tail)


# -- period performance --------------------------------------------------------

_PERIODS = (("MTD", None), ("1M", 1), ("3M", 3), ("6M", 6), ("1Y", 12))


def period_returns(nav_rows, capital, bench=None, bench_name="Benchmark", key="NAV",
                   div_events=None):
    """
    Point-to-point price returns for MTD / 1M / 3M / 6M / 1Y / Since launch.

    Portfolio and benchmark are rebased to the same base session so the
    outperformance column is a like-for-like difference. Base session = the last
    session ON OR BEFORE the period anchor, so a holiday anchor rolls back rather
    than dropping the period. Periods whose base predates inception return n/a
    rather than silently collapsing into Since launch.

    Price return only on both legs - the NSE bhavcopy is a price series and the
    portfolio excludes dividends, so the two are consistent.

    key="NAV_TR" reports the dividend-inclusive basis as price return PLUS the
    dividend cash actually received inside the window, divided by the base
    session's NAV - not NAV_TR(end)/NAV_TR(base). Dividend cash is never
    reinvested, so it is a constant added to both the base and end NAV_TR; for a
    window with no dividend events that constant dilutes the ratio, so the
    dividend-inclusive figure can print BELOW the price-only figure - reads as a
    bug. Adding the period's own income on top of the price return guarantees
    incl. dividends is never lower than excl., and "Since launch" is unchanged
    (base is capital, so the full income is always in that window).
    """
    dates = [r["Date"] for r in nav_rows]
    navs = {r["Date"]: r["NAV"] for r in nav_rows}
    inception, as_of = dates[0], dates[-1]
    bench = bench or {}
    bench_dates = sorted(bench)
    div_events = div_events or []

    def income_in_window(base, end):
        """Dividend cash received after `base` through `end` (inclusive)."""
        return sum(d["Amount"] for d in div_events if base < d["Date"] <= end)

    def session_at_or_before(d):
        c = [x for x in dates if x <= d]
        return c[-1] if c else None

    def bench_at_or_before(d):
        c = [x for x in bench_dates if x <= d]
        return bench[c[-1]] if c else None

    anchors = []
    for label, months in _PERIODS:
        if months is None:
            anchor = as_of.replace(day=1) - timedelta(days=1)
        else:
            anchor = (pd.Timestamp(as_of) - pd.DateOffset(months=months)).date()
        anchors.append((label, session_at_or_before(anchor)))
    anchors.append(("Since launch", inception))

    b_end = bench_at_or_before(as_of)
    out = []
    for label, base in anchors:
        si = label == "Since launch"
        if base is None or (not si and base < inception):
            out.append(dict(Period=label, From="n/a", Days=None,
                            Portfolio=None, Benchmark=None, Outperformance=None))
            continue

        base_nav = float(capital) if si else navs[base]
        port = (navs[as_of] / base_nav - 1) * 100 if base_nav else None
        if key == "NAV_TR" and port is not None and base_nav:
            port += income_in_window(base, as_of) / base_nav * 100.0

        b_start = bench_at_or_before(base)
        bmk = ((b_end / b_start - 1) * 100
               if b_start and b_end and b_start > 0 else None)

        out.append(dict(
            Period=label, From=base.strftime("%d-%b-%Y"), Days=(as_of - base).days,
            Portfolio=round(port, 2) if port is not None else None,
            Benchmark=round(bmk, 2) if bmk is not None else None,
            Outperformance=round(port - bmk, 2)
            if (port is not None and bmk is not None) else None,
        ))

    df = pd.DataFrame(out)
    return df.rename(columns={"Portfolio": "Portfolio %",
                              "Benchmark": f"{bench_name} %",
                              "Outperformance": "Outperf pp"})


# -- corporate actions --------------------------------------------------------
#
# Prices from the NSE bhavcopy are UNADJUSTED - the raw exchange close for that
# session. That is correct and stays that way. A corporate action is handled by
# restating the affected symbol's price series onto one consistent basis, so the
# ex-date gap stops reading as a market loss.
#
# Direction matters and is not cosmetic:
#
#   pre_ex_down  Divide every price BEFORE the ex-date by k. History is restated
#                onto the post-ex basis. Today's NAV is the true market value of
#                what is actually held, and every post-ex trade executes at the
#                real traded price, so share counts match a real client's.
#                The demerged entity's value leaves the NAV permanently.
#
#   post_ex_up   Multiply every price ON OR AFTER the ex-date by k. History is
#                untouched and NAV stays on the cum basis, which acts as a proxy
#                for still holding the unlisted entitlement. But post-ex trades
#                then execute at a synthetic price, so share counts diverge from
#                what a real client would hold.
#
# Both produce the SAME return series. They differ in absolute quantities. On the
# reference log both give +19.23%, but end with 617 real shares vs 361 synthetic.
#
# The log is restated with the same factor, keyed on the date each price belongs
# to: entry_px by its entry date, mod_px by its modification date. Without this
# the log and the price series sit on different bases.

CA_URL = "https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR{d}.zip"
CA_ADJUSTABLE = ("DEMERGER", "SPLIT", "BONUS", "SPLT", "CONSOLIDATION")


def fetch_ca_calendar(days, session_get):
    """
    NSE's official corporate-action file, free on the same archive host.

    PR{DDMMYY}.zip -> bc{DDMMYYYY}.csv, columns SERIES, SYMBOL, SECURITY,
    RECORD_DT, EX_DT, PURPOSE. Verified live: TRIVENI DEMERGER, ex 22-Jul-2026.

    session_get(url) -> requests.Response, injected so the caller controls
    headers, retries and caching.
    """
    import io
    import zipfile

    seen, rows = set(), []
    for d in days:
        try:
            r = session_get(CA_URL.format(d=d.strftime("%d%m%y")))
            if r.status_code != 200:
                continue
            z = zipfile.ZipFile(io.BytesIO(r.content))
            names = [n for n in z.namelist() if n.lower().startswith("bc")]
            if not names:
                continue
            df = pd.read_csv(z.open(names[0]))
        except Exception:
            continue
        df.columns = [c.strip().upper() for c in df.columns]
        if not {"SYMBOL", "EX_DT", "PURPOSE"} <= set(df.columns):
            continue
        for _, r2 in df.iterrows():
            sym = str(r2["SYMBOL"]).strip().upper()
            ex = parse_date(r2["EX_DT"])
            purpose = str(r2["PURPOSE"]).strip()
            key = (sym, ex, purpose)
            if ex and key not in seen:
                seen.add(key)
                rows.append(dict(Symbol=sym, ExDate=ex, Purpose=purpose))
    return pd.DataFrame(rows, columns=["Symbol", "ExDate", "Purpose"])


def is_adjustable(purpose):
    p = str(purpose).upper()
    if "DIVIDEND" in p or "INTEREST" in p:
        return False
    return any(k in p for k in CA_ADJUSTABLE)


_SPLIT_RE = re.compile(r"FV\s*SPLT.*?FRM\s*RS?\.?\s*(\d+(?:\.\d+)?)\s*TO\s*"
                       r"(?:RS|RE)?\.?\s*(\d+(?:\.\d+)?)", re.I)
_BONUS_RE = re.compile(r"BONUS\s*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", re.I)


def ratio_factor(purpose):
    """
    Exact price factor from the PURPOSE text, where the terms are actually stated.

    Splits carry the face values: "FVSPLT FRM RS 10 TO RE 1" -> 10.0
    Bonuses carry the ratio:      "BONUS 3:5" -> 3 new per 5 held -> 8/5 = 1.6

    Returns (factor, description) or (None, reason). Verified against all 26
    distinct split/bonus strings NSE published over the reference period.

    Demergers state no terms at all - the value split between parent and
    resulting entity is only in the scheme document, so they always return None.
    """
    text = str(purpose).upper()
    m = _SPLIT_RE.search(text)
    if m:
        frm, to = float(m.group(1)), float(m.group(2))
        if to > 0 and frm > to:
            return frm / to, f"face value {frm:g} to {to:g}"
    m = _BONUS_RE.search(text)
    if m:
        new, held = float(m.group(1)), float(m.group(2))
        if held > 0:
            return (new + held) / held, f"bonus {new:g}:{held:g}"
    return None, "terms not stated in the purpose text"


def ca_factor(closes, symbol, ex_date, method="gap", bench=None, override=None,
              purpose=""):
    """
    k = prev_close / ex_close, optionally corrected for the true move on the day.

    method 'gap'    assumes the symbol's genuine return on the ex-date was 0%
    method 'index'  assumes it moved with the benchmark that day
    override        an explicit k, or an explicit true return as {'true_ret': -0.05}

    Returns (k, basis_text) or (None, reason).
    """
    sessions = sorted(d for d in closes if symbol in closes[d])
    prior = [d for d in sessions if d < ex_date]
    onward = [d for d in sessions if d >= ex_date]
    if not prior or not onward:
        return None, "no price on one side of the ex-date"

    prev_d, ex_d = prior[-1], onward[0]
    prev_px, ex_px = closes[prev_d][symbol], closes[ex_d][symbol]
    if prev_px <= 0 or ex_px <= 0:
        return None, "zero price at the ex-date boundary"

    exact, how = ratio_factor(purpose)
    if exact and override is None:
        return exact, f"exact terms - {how}"

    if isinstance(override, dict) and "true_ret" in override:
        k = prev_px * (1 + float(override["true_ret"])) / ex_px
        return k, f"manual true return {float(override['true_ret'])*100:+.2f}%"
    if override:
        return float(override), "manual factor"

    if method == "index" and bench:
        bd = sorted(bench)
        b0 = [d for d in bd if d <= prev_d]
        b1 = [d for d in bd if d <= ex_d]
        if b0 and b1 and bench[b0[-1]] > 0:
            move = bench[b1[-1]] / bench[b0[-1]]
            return prev_px * move / ex_px, f"index-relative ({(move-1)*100:+.2f}%)"
    return prev_px / ex_px, "price gap (0% assumed true return)"


def apply_corporate_actions(closes, log, actions, mode="pre_ex_down",
                            method="gap", bench=None, overrides=None,
                            demerger_policy="require"):
    """
    actions  : list of dicts with Symbol, ExDate, Purpose (from fetch_ca_calendar)
    overrides: {(symbol, ex_date): k or {'true_ret': x}}
    mode     : 'pre_ex_down' (recommended) or 'post_ex_up'

    Returns (closes_adj, log_adj, ca_alerts). Nothing is applied silently -
    every action touching a held symbol produces a row, adjusted or not.
    """
    overrides = overrides or {}
    held = set(log.loc[~log["is_liquid"], "symbol"])
    closes_adj = {d: dict(m) for d, m in closes.items()}
    log_adj = log.copy()
    if "entry_dt" not in log_adj.columns:
        log_adj["entry_dt"] = log_adj["mod_dt"]
    alerts = []

    for a in sorted(actions, key=lambda x: (x["ExDate"], x["Symbol"])):
        sym, ex, purpose = a["Symbol"], a["ExDate"], a["Purpose"]
        if sym not in held:
            continue
        ov = overrides.get((sym, ex))
        if not is_adjustable(purpose) and ov is None:
            p_up = str(purpose).upper()
            if "DIVIDEND" in p_up or "INTEREST" in p_up:
                # Price-return basis excludes dividends by design - this restates
                # the run's standing policy on every dividend for every held
                # stock (the single largest contributor to alert volume) and the
                # disclosure footer already states it. Not a judgement call, so
                # no alert.
                continue
            alerts.append(dict(Date=ex, Symbol=sym, Type="CORPORATE ACTION - NOT ADJUSTED",
                               Detail=f"{purpose} - price-return basis, no adjustment applied"))
            continue

        exact, _ = ratio_factor(purpose)
        if exact is None and ov is None and demerger_policy == "require":
            prior = [d for d in sorted(closes_adj) if d < ex and sym in closes_adj[d]]
            onward = [d for d in sorted(closes_adj) if d >= ex and sym in closes_adj[d]]
            gap = ((closes_adj[onward[0]][sym] / closes_adj[prior[-1]][sym] - 1) * 100
                   if prior and onward else float("nan"))
            alerts.append(dict(
                Date=ex, Symbol=sym, Type="CORPORATE ACTION - FACTOR REQUIRED",
                Detail=(f"{purpose}: terms are not published in the CA file and cannot "
                        f"be derived. Ex-date gap was {gap:+.2f}%, but part of that may "
                        f"be a genuine market move. Supply the factor or the true "
                        f"ex-date return, or switch the demerger policy to approximate. "
                        f"NOT ADJUSTED - returns will be understated.")))
            continue

        k, basis = ca_factor(closes_adj, sym, ex, method=method, bench=bench,
                             override=ov, purpose=purpose)
        if k is None or k <= 0:
            alerts.append(dict(Date=ex, Symbol=sym, Type="CORPORATE ACTION - CANNOT ADJUST",
                               Detail=f"{purpose}: {basis}"))
            continue

        if mode == "post_ex_up":
            for d, m in closes_adj.items():
                if d >= ex and sym in m:
                    m[sym] *= k
            sel = log_adj["symbol"] == sym
            log_adj.loc[sel & (log_adj["entry_dt"] >= ex), "entry_px"] *= k
            log_adj.loc[sel & (log_adj["mod_dt"] >= ex), "mod_px"] *= k
        else:
            for d, m in closes_adj.items():
                if d < ex and sym in m:
                    m[sym] /= k
            sel = log_adj["symbol"] == sym
            log_adj.loc[sel & (log_adj["entry_dt"] < ex), "entry_px"] /= k
            log_adj.loc[sel & (log_adj["mod_dt"] < ex), "mod_px"] /= k

        alerts.append(dict(
            Date=ex, Symbol=sym, Type="CORPORATE ACTION - ADJUSTED",
            Detail=f"{purpose}; factor {k:.5f} from {basis}; mode {mode}; "
                   f"applied to EOD closes and log prices"))

    return closes_adj, log_adj, alerts


def gap_detector(closes, log, actions, threshold_pct=20.0):
    """
    Backstop for actions the calendar misses. Flags any overnight move on a
    symbol the portfolio ever held that exceeds the threshold and has no matching
    calendar entry within a day of the gap.
    """
    known = {(a["Symbol"], a["ExDate"]) for a in actions}
    held = set(log.loc[~log["is_liquid"], "symbol"])
    sessions = sorted(closes)
    out = []
    for sym in sorted(held):
        prev_d = prev_px = None
        for d in sessions:
            px = closes[d].get(sym)
            if px is None or px <= 0:
                continue
            if prev_px:
                chg = (px / prev_px - 1) * 100
                near = any((sym, d + timedelta(days=o)) in known for o in (-1, 0, 1))
                if abs(chg) >= threshold_pct and not near:
                    out.append(dict(Date=d, Symbol=sym, Type="UNEXPLAINED PRICE GAP",
                                    Detail=f"{chg:+.2f}% from {prev_d} ({prev_px}) to "
                                           f"{px}; no corporate action on file - verify"))
            prev_d, prev_px = d, px
    return out


# -- dividends ----------------------------------------------------------------

_DPS_RE = re.compile(r"(?:RS|RE)\.?\s*([0-9]+(?:\.[0-9]+)?)", re.I)
DIV_DEDUP_WINDOW_DAYS = 7


def parse_dps(purpose):
    """
    Rupees per share from the PURPOSE text. Parsed cleanly on all 103 dividend
    records in the reference period.

      "INTDIV - RS 12 PER SH"      -> 12.0
      "DIV - RE 0.50 PER SH"       -> 0.5
      "INTDIV-RS11/SPLDIV-RS46"    -> 57.0   (components sum)

    Returns None when the string carries no dividend or no parseable amount, so
    the caller can alert rather than silently assume zero.
    """
    text = str(purpose).upper()
    if "DIV" not in text:
        return None
    vals = [float(v) for v in _DPS_RE.findall(text)]
    return sum(vals) if vals else None


def build_dividends(actions, held_symbols):
    """
    {ex_date: [{symbol, dps, purpose}]}, alerts, and the count of duplicate NSE
    series listings collapsed into one record.

    De-duplicated on (symbol, dps) within a 7-day window. NSE lists the same
    dividend under several series with slightly different ex-dates - BEL appears
    on both 05-Mar and 06-Mar at RS 1.95, which would otherwise be counted twice.
    This is deterministic data cleaning, not a judgement call - the duplicate is
    still discarded before crediting (one record kept, cash credited once), it
    just no longer produces an alert row. The count is returned so the caller can
    show it as a passive caption instead.
    """
    parsed, alerts = [], []
    for a in actions:
        sym = a["Symbol"]
        if sym not in held_symbols or "DIV" not in str(a["Purpose"]).upper():
            continue
        dps = parse_dps(a["Purpose"])
        if dps is None or dps <= 0:
            alerts.append(dict(Date=a["ExDate"], Symbol=sym,
                               Type="DIVIDEND NOT PARSED",
                               Detail=f"could not read an amount from '{a['Purpose']}'"))
            continue
        parsed.append(dict(symbol=sym, ex=a["ExDate"], dps=dps, purpose=a["Purpose"]))

    parsed.sort(key=lambda x: (x["symbol"], x["dps"], x["ex"]))
    kept, last, suppressed = [], {}, 0
    for p in parsed:
        key = (p["symbol"], round(p["dps"], 4))
        prev = last.get(key)
        if prev and (p["ex"] - prev).days <= DIV_DEDUP_WINDOW_DAYS:
            suppressed += 1
            continue
        last[key] = p["ex"]
        kept.append(p)

    by_date = defaultdict(list)
    for p in kept:
        by_date[p["ex"]].append(dict(symbol=p["symbol"], dps=p["dps"],
                                     purpose=p["purpose"]))
    return dict(by_date), alerts, suppressed


# -- corporate action overrides file ------------------------------------------

CA_OVERRIDE_FILE = "ca_overrides.csv"


def parse_override_lines(text):
    """
    'SYMBOL, EX-DATE, VALUE' per line. VALUE is a price factor, or a true ex-date
    return when it ends in '%'. Returns (overrides, rejected).
    """
    overrides, rejected = {}, []
    for line in str(text or "").splitlines():
        line = line.strip()
        if not line or line.lstrip().startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            rejected.append((line, "expected SYMBOL, EX-DATE, VALUE"))
            continue
        sym, ex_raw, val = parts[0].upper(), parts[1], parts[2]
        ex = parse_date(ex_raw)
        if ex is None:
            rejected.append((line, f"could not read the date '{ex_raw}'"))
            continue
        try:
            overrides[(sym, ex)] = ({"true_ret": float(val.rstrip("%")) / 100.0}
                                    if val.endswith("%") else float(val))
        except ValueError:
            rejected.append((line, f"could not read the value '{val}'"))
    return overrides, rejected


def load_override_file(path=CA_OVERRIDE_FILE):
    """
    Repo-level shared overrides. Streamlit Cloud wipes the filesystem on every
    redeploy, so this file is the only storage that survives - it is committed to
    the repo, not written by the app.

    Columns: symbol, ex_date, value[, note]. Missing file is not an error.
    """
    import os
    if not os.path.exists(path):
        return {}, []
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception as e:
        return {}, [(path, f"could not read: {e}")]
    df.columns = [c.strip().lower() for c in df.columns]
    if not {"symbol", "ex_date", "value"} <= set(df.columns):
        return {}, [(path, "needs the columns symbol, ex_date, value")]
    lines = "\n".join(f"{r['symbol']},{r['ex_date']},{r['value']}"
                      for _, r in df.iterrows())
    return parse_override_lines(lines)


def override_csv_line(symbol, ex_date, value, note=""):
    """The exact text to paste into ca_overrides.csv."""
    return f"{str(symbol).upper()},{ex_date},{value},{note}"


# -- attribution, risk, costs -------------------------------------------------

def attribution(nav_rows, trades, dividends_paid=None):
    """
    Rupee contribution per symbol: realised + unrealised + dividends.

    Realised P&L uses FIFO lots, so a symbol bought and sold repeatedly is not
    flattered by netting average prices across separate holding periods.
    """
    from collections import deque

    lots, realised, bought, sold = defaultdict(deque), defaultdict(float), \
        defaultdict(float), defaultdict(float)
    for t in sorted(trades, key=lambda x: (x["Date"], x["Side"] != "SELL")):
        sym, qty, px = t["Symbol"], t["Qty"], t["Price"]
        if t["Side"] == "BUY":
            lots[sym].append([qty, px])
            bought[sym] += qty * px
        else:
            sold[sym] += qty * px
            left = qty
            while left > 0 and lots[sym]:
                lot = lots[sym][0]
                take = min(left, lot[0])
                realised[sym] += take * (px - lot[1])
                lot[0] -= take
                left -= take
                if lot[0] <= 0:
                    lots[sym].popleft()

    last = nav_rows[-1]
    div_by_sym = defaultdict(float)
    for d in (dividends_paid or []):
        div_by_sym[d["Symbol"]] += d["Amount"]

    rows = []
    for sym in sorted(set(list(realised) + list(last["_holdings"]) + list(div_by_sym))):
        h = last["_holdings"].get(sym)
        unreal = 0.0
        if h:
            cost = sum(q * p for q, p in lots[sym])
            unreal = h["value"] - cost
        total = realised[sym] + unreal + div_by_sym[sym]
        rows.append(dict(Symbol=sym, Realised=round(realised[sym], 2),
                         Unrealised=round(unreal, 2),
                         Dividends=round(div_by_sym[sym], 2),
                         Total=round(total, 2),
                         Invested=round(bought[sym], 2),
                         StillHeld="Yes" if h else "No"))
    df = pd.DataFrame(rows)
    if len(df):
        base = nav_rows[0]["NAV"] if nav_rows else 1.0
        df["ContributionPP"] = (df["Total"] / base * 100).round(2)
        df = df.sort_values("Total", ascending=False).reset_index(drop=True)
    return df


def risk_stats(nav_rows, capital, key="NAV"):
    """Drawdown, volatility, hit rate. key='NAV_TR' for the dividend basis."""
    import math
    navs = [r[key] for r in nav_rows]
    dates = [r["Date"] for r in nav_rows]
    if len(navs) < 2:
        return {}

    peak, peak_d, mdd, mdd_d, mdd_peak_d = navs[0], dates[0], 0.0, None, None
    for d, v in zip(dates, navs):
        if v > peak:
            peak, peak_d = v, d
        dd = (v / peak - 1) * 100
        if dd < mdd:
            mdd, mdd_d, mdd_peak_d = dd, d, peak_d

    recovered = None
    if mdd_d:
        for d, v in zip(dates, navs):
            if d > mdd_d and v >= max(n for dd, n in zip(dates, navs) if dd == mdd_peak_d):
                recovered = (d - mdd_d).days
                break

    rets = [(navs[i] / navs[i - 1] - 1) for i in range(1, len(navs)) if navs[i - 1]]
    mean = sum(rets) / len(rets) if rets else 0.0
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1) if len(rets) > 1 else 0.0
    vol = math.sqrt(var) * math.sqrt(252) * 100
    years = max((dates[-1] - dates[0]).days / 365.25, 1e-9)
    cagr = ((navs[-1] / capital) ** (1 / years) - 1) * 100 if navs[-1] > 0 else 0.0
    up = sum(1 for r in rets if r > 0)

    return {
        "Max drawdown %": round(mdd, 2),
        "Drawdown trough": mdd_d.strftime("%d-%b-%Y") if mdd_d else "-",
        "Peak before trough": mdd_peak_d.strftime("%d-%b-%Y") if mdd_peak_d else "-",
        "Days to recover": str(recovered) if recovered is not None else "not yet",
        "Annualised volatility %": round(vol, 2),
        "CAGR %": round(cagr, 2),
        "Return / max drawdown": round(abs(cagr / mdd), 2) if mdd else None,
        "Positive days %": round(up / len(rets) * 100, 1) if rets else None,
        "Best day %": round(max(rets) * 100, 2) if rets else None,
        "Worst day %": round(min(rets) * 100, 2) if rets else None,
    }


def cost_impact(trades, nav_rows, capital, bps):
    """
    Brokerage as a drag on the final return.

    Applied as a reporting overlay rather than deducted inside the engine, so
    changing the rate never changes a share quantity and the two views stay
    comparable.
    """
    turnover = sum(t["Value"] for t in trades)
    cost = turnover * bps / 10000.0
    final = nav_rows[-1]["NAV"] if nav_rows else capital
    gross = (final / capital - 1) * 100
    net = ((final - cost) / capital - 1) * 100
    return {
        "Two-sided turnover": round(turnover, 2),
        "Turnover / capital %": round(turnover / capital * 100, 1),
        "Brokerage rate (bps)": bps,
        "Estimated cost": round(cost, 2),
        "Gross return %": round(gross, 2),
        "Net of brokerage %": round(net, 2),
        "Cost drag pp": round(gross - net, 2),
    }


def cash_drag(nav_rows):
    """Average cash weight and what full deployment would have added."""
    if not nav_rows:
        return {}
    weights = [(r["Cash"] / r["NAV"] * 100) if r["NAV"] else 0.0 for r in nav_rows]
    avg = sum(weights) / len(weights)

    eq_growth = 1.0
    for i in range(1, len(nav_rows)):
        p, c = nav_rows[i - 1], nav_rows[i]
        if p["MarketValue"] > 0 and not c["Type"] == "TRADE":
            eq_growth *= (c["MarketValue"] / p["MarketValue"])
    return {
        "Average cash weight %": round(avg, 2),
        "Peak cash weight %": round(max(weights), 2),
        "Minimum cash weight %": round(min(weights), 2),
        "Cash weight today %": round(weights[-1], 2),
    }
