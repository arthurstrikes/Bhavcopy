"""
mtf_leverage.py  -  2x MTF leverage overlay for the IMP NAV Calculator.
Repo root, imported by pages/nav_calculator.py. Does not modify imp_engine.py.

Read-only pass over a base-engine trade list. It never changes what is bought,
when, or how much - only how an already-decided purchase is funded.

State (changed ONLY by trade events, never by prices):
    cash            actual rupees, starts at the client's own contribution
    leverage[sym]   debt outstanding, FIXED FACE VALUE, never reprices
    qty[sym]        share count
    own_qty/lev_qty unfloored bookkeeping fractions for the 50/50 split

Derived FRESH each day, never accumulated:
    gross_assets = sum(qty * last_known_close)
    net_worth    = cash + gross_assets - total_leverage

Because net worth is recomputed from current state rather than accumulated
day-over-day, double-counting is structurally impossible.

wavg leverage price = leverage[sym] / lev_qty[sym], exact by construction
(leverage[sym] == sum(lev_qty_i * price_i)), so no funding-event list is needed.

Simulation and costs are deliberately split into two functions. run_leverage()
is pure trading economics and is expensive; apply_costs() is pure arithmetic on
its output and is cheap. Changing an interest rate therefore never re-runs the
simulation or re-fetches a price.
"""
from datetime import date

SPLIT = 0.50                 # margin share of every funding event (spec-fixed)
DEF_INTEREST_PA = 0.15
DEF_PLEDGE = 29.50


# --------------------------------------------------------------- simulation
def run_leverage(trades, closes, calendar, own_capital):
    """Pre-cost simulation. Returns one row per session."""
    by_day = {}
    for t in trades:
        by_day.setdefault(t["Date"], []).append(t)

    cash = float(own_capital)
    qty, own_qty, lev_qty, leverage = {}, {}, {}, {}
    last_close = {}
    rows, prev_day = [], None

    for day in calendar:
        today = closes.get(day, {})
        legs = 0
        trade_value = 0.0
        shortfall = 0.0

        for t in sorted(by_day.get(day, []), key=lambda x: (x["Side"], x["Symbol"])):
            s, px, q = t["Symbol"], float(t["Price"]), int(t["Qty"])
            if q <= 0 or px <= 0:
                continue
            val = q * px
            if t["Side"] == "BUY":
                m_drawn = val * SPLIT
                l_drawn = val - m_drawn
                cash -= m_drawn
                leverage[s] = leverage.get(s, 0.0) + l_drawn
                own_qty[s] = own_qty.get(s, 0.0) + m_drawn / px
                lev_qty[s] = lev_qty.get(s, 0.0) + l_drawn / px
                qty[s] = qty.get(s, 0) + q
            else:
                held = qty.get(s, 0)
                if held <= 0:
                    continue
                q = min(q, held)
                frac = q / held
                debt_owed = leverage.get(s, 0.0) * frac
                proceeds = q * px
                # Debt on the sold portion is ALWAYS retired in full; cash
                # absorbs any shortfall. Debt is never quietly written off.
                if proceeds < debt_owed:
                    shortfall += debt_owed - proceeds
                cash += proceeds - debt_owed
                leverage[s] = leverage.get(s, 0.0) - debt_owed
                lev_qty[s] = lev_qty.get(s, 0.0) - lev_qty.get(s, 0.0) * frac
                own_qty[s] = own_qty.get(s, 0.0) - own_qty.get(s, 0.0) * frac
                qty[s] = held - q
                if qty[s] <= 0:
                    for d in (qty, own_qty, lev_qty, leverage):
                        d.pop(s, None)
                val = proceeds
            legs += 1
            trade_value += val

        # last_known_close tracks the TRUE EOD close, carried forward for
        # symbols with no print. It is NEVER overwritten with an execution price.
        for s in qty:
            p = today.get(s)
            if p and p > 0:
                last_close[s] = p

        gross = sum(n * last_close.get(s, 0.0) for s, n in qty.items())
        total_lev = sum(leverage.values())
        net = cash + gross - total_lev
        cal_days = (day - prev_day).days if prev_day is not None else 0
        prev_day = day

        rows.append(dict(
            Date=day, GrossAssets=gross, Cash=cash, TotalLeverage=total_lev,
            NetWorth=net, Positions=len(qty), TradeLegs=legs,
            TradeValue=trade_value, CalDays=cal_days, Shortfall=shortfall,
            StockPerRupee=(gross / net) if net > 0 else None))
    return rows


# -------------------------------------------------------------------- costs
def apply_costs(rows, interest_pa=DEF_INTEREST_PA, pledge=DEF_PLEDGE,
                brokerage_pct=0.0, statutory_pct=0.0):
    """
    Cost Ledger. Accrues alongside the simulation and never feeds back into it.
    Cheap - safe to call on every widget change.
    """
    out, ci, cp, cb, cs = [], 0.0, 0.0, 0.0, 0.0
    for r in rows:
        ci += r["TotalLeverage"] * interest_pa / 365.0 * r["CalDays"]
        cp += r["TradeLegs"] * pledge
        cb += r["TradeValue"] * brokerage_pct / 100.0
        cs += r["TradeValue"] * statutory_pct / 100.0
        tot = ci + cp + cb + cs
        d = dict(r)
        d.update(Interest=ci, Pledge=cp, Brokerage=cb, Statutory=cs,
                 AllCosts=tot, NetAfterCosts=r["NetWorth"] - tot)
        out.append(d)
    return out


def roe_after_costs(rows, own_capital, **kw):
    return apply_costs(rows, **kw)[-1]["NetAfterCosts"] / own_capital - 1


def breakeven_interest(rows, own_capital, cash_model_net_return,
                       pledge=DEF_PLEDGE, brokerage_pct=0.0, statutory_pct=0.0):
    """Interest rate at which MTF stops beating the cash model. None if never."""
    f = lambda rt: roe_after_costs(rows, own_capital, interest_pa=rt,
                                   pledge=pledge, brokerage_pct=brokerage_pct,
                                   statutory_pct=statutory_pct)
    if f(0.0) <= cash_model_net_return:
        return None
    lo, hi = 0.0, 3.0
    if f(hi) > cash_model_net_return:
        return None
    for _ in range(50):
        mid = (lo + hi) / 2.0
        if f(mid) > cash_model_net_return:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


# ------------------------------------------------------------- audit oracle
def fifo_pnl(trades, final_closes):
    """Independent FIFO realised + unrealised P&L, for the accuracy check."""
    lots, realised = {}, 0.0
    for t in sorted(trades, key=lambda x: (x["Date"], x["Side"], x["Symbol"])):
        s, px, q = t["Symbol"], float(t["Price"]), int(t["Qty"])
        if q <= 0 or px <= 0:
            continue
        if t["Side"] == "BUY":
            lots.setdefault(s, []).append([q, px])
        else:
            left = q
            while left > 0 and lots.get(s):
                lot = lots[s][0]
                take = min(left, lot[0])
                realised += take * (px - lot[1])
                lot[0] -= take
                left -= take
                if lot[0] == 0:
                    lots[s].pop(0)
    unrealised = 0.0
    for s, ls in lots.items():
        for q, px in ls:
            unrealised += q * (final_closes.get(s, 0.0) - px)
    return realised, unrealised


def max_drawdown(values):
    peak, dd, s, e, pi = -1e18, 0.0, None, None, 0
    for i, v in enumerate(values):
        if v > peak:
            peak, pi = v, i
        if peak > 0 and v / peak - 1 < dd:
            dd, s, e = v / peak - 1, pi, i
    return dd, s, e, peak
