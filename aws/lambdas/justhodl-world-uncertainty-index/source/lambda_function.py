"""justhodl-world-uncertainty-index — World Uncertainty Index (Ahir, Bloom & Furceri), quarterly 1990→ (public Excel, no key).

The data page links a dated workbook (…/uploads/YYYY/MM/WUI_Data.xlsx); the link is harvested each run.
  T1  global / regional WUI aggregates (simple and GDP-weighted), 1990q1→
  T2  WUI by country (143 countries), 1952q1→  (frequency of "uncertainty" in EIU country reports per 1,000 words ×1,000)
  T6  three-quarter weighted moving average by country
  T8  World Trade Uncertainty index (aggregate and by country)
Descriptive only.
"""
from __future__ import annotations

import re

import risk_sources as RS

SLUG = "world-uncertainty-index"
PAGE = "https://worlduncertaintyindex.com/data/"
QRE = re.compile(r"^(\d{4})q([1-4])$", re.I)


def find_workbook(html):
    m = re.search(r'href="(https?://worlduncertaintyindex\.com/wp-content/uploads/\d{4}/\d{2}/WUI_Data\.xlsx)"', html)
    return m.group(1) if m else None


def parse_table(rows):
    """header row + rows labelled YYYYqN -> {label: [[date, v]]}"""
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    out = {h: [] for h in header[1:] if h}
    for r in rows[1:]:
        if not r or r[0] is None:
            continue
        m = QRE.match(str(r[0]).strip())
        if not m:
            continue
        d = RS.quarter_end(int(m.group(1)), int(m.group(2)))
        for i, h in enumerate(header[1:], 1):
            if h and i < len(r):
                v = RS.parse_num(r[i])
                if v is not None:
                    out[h].append([d, v])
    return {h: p for h, p in out.items() if p}


def _sid(s):
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()


def build(event):
    pk = RS.Packet(
        SLUG, "World Uncertainty Index", "World Uncertainty Index (WUI) — Ahir, Bloom & Furceri",
        "Quarterly text-based measure of uncertainty for 143 countries since the 1950s: the frequency of the word "
        "'uncertainty' in Economist Intelligence Unit country reports, aggregated into global and regional indices "
        "(simple and GDP-weighted) and a World Trade Uncertainty index. Unlike EPU it covers emerging and frontier "
        "economies with one consistent source. Read from the authors' public workbook.",
        {"provider": "Ahir, Bloom & Furceri — worlduncertaintyindex.com (IMF / Stanford)", "url": PAGE, "docs": "https://worlduncertaintyindex.com/methodology/",
         "license": "Public academic data (cite Ahir, Bloom & Furceri 2022)", "cadence": "quarterly"},
        cadence="weekly")
    html = RS.http_text(PAGE, timeout=60)
    url = find_workbook(html)
    if not url:
        raise RuntimeError("WUI data page: WUI_Data.xlsx link not found")
    blob = RS.http_get(url, timeout=90)
    pk.raw("WUI_Data.xlsx", blob, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=url)
    x = RS.Xlsx(blob)
    names = x.sheet_names()
    for need in ("T1", "T2"):
        if need not in names:
            raise RuntimeError(f"WUI workbook tabs changed: {names}")
    for label, pts in parse_table(x.rows("T1")).items():
        pk.add_series("GLOBAL_" + _sid(label), f"WUI — {label}", pts, unit="index", freq="quarterly", group="global")
    cty = parse_table(x.rows("T2"))
    grid = []
    for iso, pts in cty.items():
        s = pk.add_series("CTY_" + iso, f"{iso} — WUI", pts, unit="index", freq="quarterly", group="country", iso3=iso)
        if s:
            vals = [v for _, v in s["_points"]]
            avg8 = sum(vals[-8:]) / min(8, len(vals))
            grid.append([iso, round(s["latest"], 3), s["date"], round(avg8, 3), s["pct_rank"], round(s["latest"] - avg8, 3)])
    if "T6" in names:
        for iso, pts in parse_table(x.rows("T6")).items():
            pk.add_series("MA3_" + iso, f"{iso} — WUI, 3-quarter weighted MA", pts, unit="index", freq="quarterly", group="country_ma", iso3=iso)
    if "T8" in names:
        for label, pts in parse_table(x.rows("T8")).items():
            if len(label) > 3:          # the two aggregates: "(equally weighted average)" / "(GDP weighted average)"
                kind = "GDP_WEIGHTED" if "gdp" in label.lower() else "SIMPLE"
                pk.add_series("WTU_GLOBAL_" + kind, f"World Trade Uncertainty — {label}", pts, unit="index", freq="quarterly", group="trade")
            else:
                pk.add_series("WTU_" + label, f"{label} — World Trade Uncertainty", pts, unit="index", freq="quarterly", group="trade_country", iso3=label)
    grid.sort(key=lambda r: -(r[4] or 0))
    pk.add_table("countries", ["ISO3", "Latest", "Quarter", "2y avg", "Pct rank (own history)", "Latest − 2y avg"], grid,
                 title="WUI by country", note="Country series are noisy quarter to quarter; the 3-quarter weighted MA series (MA3_*) smooth them.")
    g = pk.series.get("GLOBAL_GLOBAL_GDP_WEIGHTED_AVERAGE") or next((s for k, s in pk.series.items() if k.startswith("GLOBAL_")), None)
    if g:
        peak = max(g["_points"], key=lambda p: p[1])
        pk.kpi("World Uncertainty Index", f"{g['latest']:,.0f}", f"{g['date']} · GDP-weighted · {RS.ordinal(g['pct_rank'])} pct since 1990", "warning" if g["pct_rank"] and g["pct_rank"] > 85 else "info")
        pk.kpi("Peak", f"{peak[1]:,.0f}", peak[0], "mute")
    t = pk.series.get("WTU_GLOBAL_GDP_WEIGHTED")
    if t:
        pk.kpi("World Trade Uncertainty", f"{t['latest']:,.1f}", f"{t['date']} · GDP-weighted · {RS.ordinal(t['pct_rank'])} pct since 1996", "warning" if t["pct_rank"] and t["pct_rank"] > 85 else "info")
    if grid:
        pk.kpi("Countries", str(len(grid)), f"highest own-history rank: {grid[0][0]} ({grid[0][4]} pct)", "info")
    pk.note("WUI = count of 'uncertainty' (and variants) per 1,000 words in EIU country reports, ×1,000; the GDP-weighted global index "
            "uses current-dollar GDP weights. For ~30 large countries the quarterly value since 2020Q4 averages the monthly reports.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
