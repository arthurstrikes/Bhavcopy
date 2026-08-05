"""
One-page PDF factsheet for an IMP NAV run.

Chart is drawn with reportlab's own graphics rather than exporting the Plotly
figure, so the app needs no kaleido/chrome image pipeline - one pure-python
dependency (reportlab) instead of a ~40MB headless browser that is fragile on
Streamlit Cloud.

Palette is passed in by the caller, so the internal shell (generic blue) and a
future external MOFSL shell share this module without a fork.
"""

from __future__ import annotations

import io
from datetime import date

from reportlab.graphics.charts.legends import Legend
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Drawing, Line, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (BaseDocTemplate, Frame, KeepTogether, PageTemplate,
                                Paragraph, Spacer, Table, TableStyle)

FONT = "Helvetica"
FONT_B = "Helvetica-Bold"


def _c(hex_str):
    return colors.HexColor(hex_str)


def _styles(pal):
    ink, muted, primary = _c(pal["ink"]), _c(pal["muted"]), _c(pal["primary"])
    return {
        "h1": ParagraphStyle("h1", fontName=FONT_B, fontSize=16, leading=19,
                             textColor=primary, spaceAfter=1),
        "sub": ParagraphStyle("sub", fontName=FONT, fontSize=8, leading=11,
                              textColor=muted, spaceAfter=6),
        "h2": ParagraphStyle("h2", fontName=FONT_B, fontSize=8.5, leading=11,
                             textColor=primary, spaceBefore=6, spaceAfter=3),
        "body": ParagraphStyle("body", fontName=FONT, fontSize=7.5, leading=10,
                               textColor=ink, alignment=TA_LEFT),
        "foot": ParagraphStyle("foot", fontName=FONT, fontSize=6.2, leading=8.2,
                               textColor=muted),
    }


def _kpi_band(items, pal, width):
    """Row of headline figures, each in its own tinted cell."""
    cells = []
    for label, value, tone in items:
        col = {"pos": _c(pal["pos"]), "neg": _c(pal["neg"])}.get(tone, _c(pal["ink"]))
        cells.append([
            Paragraph(f'<font size="13"><b>{value}</b></font>',
                      ParagraphStyle("v", fontName=FONT_B, fontSize=13, leading=15,
                                     textColor=col)),
            Paragraph(label, ParagraphStyle("l", fontName=FONT, fontSize=6.2,
                                            leading=8, textColor=_c(pal["muted"]))),
        ])
    data = [[c[0] for c in cells], [c[1] for c in cells]]
    n = len(cells)
    t = Table(data, colWidths=[width / n] * n, rowHeights=[16, 10])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _c(pal["tint8"])),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, 0), 5),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEAFTER", (0, 0), (-2, -1), 0.5, _c(pal["bg"])),
    ]))
    return t


def _table(header, rows, pal, width, aligns=None, widths=None):
    data = [header] + rows
    n = len(header)
    widths = widths or [width / n] * n
    t = Table(data, colWidths=widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), _c(pal["primary"])),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), FONT_B),
        ("FONTNAME", (0, 1), (-1, -1), FONT),
        ("FONTSIZE", (0, 0), (-1, -1), 6.8),
        ("LEADING", (0, 0), (-1, -1), 9),
        ("TEXTCOLOR", (0, 1), (-1, -1), _c(pal["ink"])),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, _c(pal["rule"])),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), _c(pal["tint8"])))
    for col in (aligns or []):
        style.append(("ALIGN", (col, 0), (col, -1), "RIGHT"))
    t.setStyle(TableStyle(style))
    return t


def _nav_chart(nav_rows, bench, pal, width, height=52 * mm, bench_name="Benchmark"):
    """Rebased portfolio vs benchmark. Both legs start at 100."""
    d = Drawing(width, height)
    dates = [r["Date"] for r in nav_rows]
    d0 = dates[0]
    port = [((x - d0).days, r["Rebased"]) for x, r in zip(dates, nav_rows)]

    series, names, cols = [port], ["Portfolio"], [_c(pal["primary"])]
    bd = sorted(x for x in (bench or {}) if dates[0] <= x <= dates[-1])
    if bd:
        b0 = bench[bd[0]]
        if b0:
            series.append([((x - d0).days, bench[x] / b0 * 100) for x in bd])
            names.append(bench_name)
            cols.append(_c(pal["muted"]))

    lp = LinePlot()
    lp.x, lp.y = 26, 20
    lp.width, lp.height = width - 34, height - 34
    lp.data = series
    lp.joinedLines = 1
    for i, col in enumerate(cols):
        lp.lines[i].strokeColor = col
        lp.lines[i].strokeWidth = 1.4 if i == 0 else 0.9
    if len(cols) > 1:
        lp.lines[1].strokeDashArray = (2, 2)

    allv = [v for s in series for _, v in s]
    lo, hi = min(allv), max(allv)
    pad = max((hi - lo) * 0.12, 1.5)
    lp.yValueAxis.valueMin = lo - pad
    lp.yValueAxis.valueMax = hi + pad
    lp.yValueAxis.labels.fontName = FONT
    lp.yValueAxis.labels.fontSize = 5.6
    lp.yValueAxis.labels.fillColor = _c(pal["muted"])
    lp.yValueAxis.strokeColor = _c(pal["rule"])
    lp.yValueAxis.gridStrokeColor = _c(pal["rule"])
    lp.yValueAxis.gridStrokeWidth = 0.25
    lp.yValueAxis.visibleGrid = 1
    lp.yValueAxis.labelTextFormat = "%0.0f"

    span = (dates[-1] - d0).days or 1
    lp.xValueAxis.valueMin, lp.xValueAxis.valueMax = 0, span
    lp.xValueAxis.valueSteps = [0, span // 4, span // 2, 3 * span // 4, span]
    lp.xValueAxis.labels.fontName = FONT
    lp.xValueAxis.labels.fontSize = 5.6
    lp.xValueAxis.labels.fillColor = _c(pal["muted"])
    lp.xValueAxis.strokeColor = _c(pal["rule"])
    lp.xValueAxis.labelTextFormat = (
        lambda v: (d0.fromordinal(d0.toordinal() + int(v))).strftime("%b %y"))
    d.add(lp)

    leg = Legend()
    leg.x, leg.y = 26, height - 8
    leg.deltax = 62
    leg.boxAnchor = "nw"
    leg.columnMaximum = 1
    leg.fontName = FONT
    leg.fontSize = 6
    leg.alignment = "right"
    leg.dxTextSpace = 3
    leg.dx = leg.dy = 3
    leg.colorNamePairs = list(zip(cols, names))
    d.add(leg)
    return d


def build_factsheet(*, nav_rows, perf_df, holdings_df, risk, costs, cashd,
                    attrib_df, bench, bench_name, capital, portfolio_name,
                    palette, div_total=0.0, external=False, notes=None):
    """
    Returns PDF bytes.

    external=True applies the commercial framing rules: max drawdown is presented
    as a recovery statistic rather than a bare red negative, and starting capital
    is omitted so it cannot be read as a minimum investment.
    """
    pal = palette
    S = _styles(pal)
    buf = io.BytesIO()

    pw, ph = A4
    ml = mr = 13 * mm
    cw = pw - ml - mr

    doc = BaseDocTemplate(buf, pagesize=A4, leftMargin=ml, rightMargin=mr,
                          topMargin=12 * mm, bottomMargin=12 * mm,
                          title=f"{portfolio_name} - performance factsheet",
                          author="Motilal Oswal Financial Services")
    frame = Frame(ml, 12 * mm, cw, ph - 24 * mm, id="f", showBoundary=0,
                  leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)

    def decorate(canv, _doc):
        canv.saveState()
        canv.setFillColor(_c(pal["primary"]))
        canv.rect(0, ph - 4, pw, 4, stroke=0, fill=1)
        canv.setFont(FONT, 6)
        canv.setFillColor(_c(pal["muted"]))
        canv.drawString(ml, 8 * mm, "Internal use - model portfolio simulation")
        canv.drawRightString(pw - mr, 8 * mm,
                             f"Generated {date.today():%d %b %Y}")
        canv.restoreState()

    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=decorate)])

    last = nav_rows[-1]
    first = nav_rows[0]
    total_return = (last["NAV"] - capital) / capital * 100
    tone = "pos" if total_return >= 0 else "neg"

    story = [
        Paragraph(portfolio_name, S["h1"]),
        Paragraph(
            f"Model portfolio simulation &middot; {first['Date']:%d %b %Y} to "
            f"{last['Date']:%d %b %Y} &middot; {len(nav_rows)} sessions &middot; "
            f"benchmark {bench_name} &middot; price return",
            S["sub"]),
    ]

    kpis = [("Return since launch", f"{total_return:+.2f}%", tone),
            ("Rebased NAV", f"{last['Rebased']:.1f}", "ink")]
    if not external:
        kpis.insert(1, ("Portfolio value", f"{last['NAV']:,.0f}", "ink"))
    since = perf_df[perf_df["Period"] == "Since launch"]
    ocol = "Outperf pp"
    _o = since.iloc[0].get(ocol) if len(since) else None
    if _o is not None and _o == _o:
        o = _o
        kpis.append(("Outperformance", f"{o:+.2f} pp", "pos" if o >= 0 else "neg"))
    if risk.get("Max drawdown %") is not None:
        if external:
            kpis.append(("Recovery from deepest decline",
                         risk.get("Days to recover", "-") + " days", "ink"))
        else:
            kpis.append(("Max drawdown", f"{risk['Max drawdown %']:.2f}%", "neg"))
    story += [_kpi_band(kpis, pal, cw), Spacer(1, 7)]

    story += [Paragraph("Rebased NAV (base 100)", S["h2"]),
              _nav_chart(nav_rows, bench, pal, cw, bench_name=bench_name),
              Spacer(1, 4)]

    # performance + holdings side by side
    pcol = [c for c in perf_df.columns if c.endswith("%") and c != "Portfolio %"]
    bcol = pcol[0] if pcol else None
    def _n(v):
        """None and NaN both mean 'period predates inception' - never print nan."""
        try:
            if v is None or v != v:
                return "n/a"
            return f"{float(v):+.2f}"
        except (TypeError, ValueError):
            return "n/a"

    prows = []
    for _, r in perf_df.iterrows():
        prows.append([str(r["Period"]), _n(r["Portfolio %"]),
                      _n(r[bcol]) if bcol else "n/a", _n(r[ocol])])
    ptab = _table(["Period", "Portfolio %", f"{bench_name} %", "Outperf pp"],
                  prows, pal, cw / 2 - 3, aligns=[1, 2, 3])

    hrows = [[r["Symbol"], f"{r['Achieved Wt %']:.2f}", f"{r['Model Wt %']:.2f}"]
             for _, r in holdings_df.head(10).iterrows()]
    if not hrows:
        hrows = [["Fully in cash", "-", "-"]]
    htab = _table(["Top holdings", "Weight %", "Model %"], hrows, pal,
                  cw / 2 - 3, aligns=[1, 2])

    inner = Table([[ptab, htab]], colWidths=[cw / 2, cw / 2])
    inner.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                               ("LEFTPADDING", (0, 0), (-1, -1), 0),
                               ("RIGHTPADDING", (0, 0), (-1, -1), 6)]))
    story += [Paragraph("Performance and portfolio", S["h2"]), inner, Spacer(1, 4)]

    # contributors + risk
    if len(attrib_df):
        top = attrib_df.head(5)
        bot = attrib_df.tail(5).iloc[::-1]
        crows = [[r["Symbol"], f"{r['ContributionPP']:+.2f}"] for _, r in top.iterrows()]
        drows = [[r["Symbol"], f"{r['ContributionPP']:+.2f}"] for _, r in bot.iterrows()]
        ctab = _table(["Key contributors", "pp"], crows, pal, cw / 3 - 4, aligns=[1])
        dtab = _table(["Key detractors", "pp"], drows, pal, cw / 3 - 4, aligns=[1])
    else:
        ctab = dtab = Spacer(1, 1)

    rrows = []
    if external:
        if risk.get("Max drawdown %") is not None:
            rrows.append(["Deepest decline", f"{abs(risk['Max drawdown %']):.2f}%"])
            rrows.append(["Recovered in", f"{risk.get('Days to recover','-')} days"])
    else:
        for k in ("Max drawdown %", "Days to recover"):
            if risk.get(k) is not None:
                rrows.append([k, str(risk[k])])
    for k in ("Annualised volatility %", "Positive days %"):
        if risk.get(k) is not None:
            rrows.append([k, str(risk[k])])
    rrows.append(["Average cash weight %", str(cashd.get("Average cash weight %", "-"))])
    if costs.get("Brokerage rate (bps)"):
        rrows.append(["Net of brokerage %", str(costs.get("Net of brokerage %"))])
    rtab = _table(["Risk and costs", "Value"], rrows, pal, cw / 3 - 4, aligns=[1])

    trio = Table([[ctab, dtab, rtab]], colWidths=[cw / 3] * 3)
    trio.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                              ("LEFTPADDING", (0, 0), (-1, -1), 0),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 6)]))
    story += [KeepTogether([Paragraph("Attribution and risk", S["h2"]), trio]),
              Spacer(1, 6)]

    base = ("Simulated performance of the model portfolio for a client investing at "
            "inception, driven by the research advice log. Prices are official NSE "
            "closing prices; trades are executed at the prices recorded in the advice "
            "log. Returns are price returns and exclude dividends unless stated. "
            "The benchmark is a price index and excludes its constituents' dividends. "
            "Corporate actions are restated so that ex-date price adjustments are not "
            "counted as performance. This is a simulation, not the record of any "
            "individual client account; actual results vary with entry date, "
            "execution and costs. Past performance is not indicative of future "
            "results.")
    story += [Paragraph(base + (" " + notes if notes else ""), S["foot"])]

    doc.build(story)
    return buf.getvalue()
