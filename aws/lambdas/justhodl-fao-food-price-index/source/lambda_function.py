"""justhodl-fao-food-price-index — FAO Food Price Index (FFPI) and its five sub-indices, monthly since 1990.

Public CSV on fao.org (no key): Date, Food Price Index, Meat, Dairy, Cereals, Oils, Sugar (2014-16 = 100).
The CSV link carries a cache token that changes per release, so the link is harvested from the FFPI page each run
with the last-known URL as fallback. Descriptive only.
"""
from __future__ import annotations

import re

import risk_sources as RS

SLUG = "fao-food-price-index"
PAGE = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
FALLBACK = "https://www.fao.org/media/docs/worldfoodsituationlibraries/wfs-library/food_price_indices_data.csv?download=true"
COLS = {"Food Price Index": ("FFPI", "FAO Food Price Index"), "Meat": ("MEAT", "Meat price index"),
        "Dairy": ("DAIRY", "Dairy price index"), "Cereals": ("CEREALS", "Cereals price index"),
        "Oils": ("OILS", "Vegetable oils price index"), "Sugar": ("SUGAR", "Sugar price index")}


def find_csv(html):
    m = re.search(r'href="([^"]*food_price_indices_data\.csv[^"]*)"', html)
    if not m:
        return FALLBACK
    url = m.group(1).replace("&amp;", "&")
    return url if url.startswith("http") else "https://www.fao.org" + url


def parse(text):
    rows = RS.csv_rows(text)
    header = None
    out = {}
    for r in rows:
        if not r:
            continue
        if header is None:
            if r[0].strip().lower() == "date":
                header = [c.strip() for c in r]
                out = {h: [] for h in header[1:] if h}
            continue
        m = re.match(r"^(\d{4})-(\d{2})$", r[0].strip())
        if not m:
            continue
        d = RS.month_end(int(m.group(1)), int(m.group(2)))
        for i, h in enumerate(header[1:], 1):
            if h and i < len(r):
                v = RS.parse_num(r[i])
                if v is not None:
                    out[h].append([d, v])
    if header is None:
        raise RuntimeError("FFPI CSV: no 'Date' header row found")
    return out


def build(event):
    pk = RS.Packet(
        SLUG, "FAO Food Price Index", "FAO Food Price Index (FFPI) — monthly, 2014-16 = 100",
        "The FAO Food Price Index tracks monthly international prices of a basket of food commodities, as the weighted "
        "average of five commodity-group indices (cereals, vegetable oils, dairy, meat, sugar). Food-price spikes are a "
        "classic trigger of sovereign and social stress in import-dependent countries. Read from FAO's public CSV since 1990.",
        {"provider": "FAO — Markets and Trade Division", "url": PAGE, "docs": PAGE,
         "license": "FAO open data (CC BY-NC-SA 3.0 IGO)", "cadence": "monthly (first Friday of the month)"},
        cadence="daily")
    try:
        html = RS.http_text(PAGE, timeout=60)
        url = find_csv(html)
    except Exception:  # noqa: BLE001
        url = FALLBACK
    txt = RS.http_text(url, timeout=60)
    pk.raw("food_price_indices_data.csv", txt.encode("utf-8"), "text/csv", url=url)
    cols = parse(txt)
    for name, (sid, label) in COLS.items():
        if cols.get(name):
            pk.add_series(sid, label, cols[name], unit="index 2014-16=100", freq="monthly", group="index")
    f = pk.series.get("FFPI")
    if f:
        pts = f["_points"]
        yoy = next((v for d, v in pts if d[:7] == f"{int(f['date'][:4]) - 1}-{f['date'][5:7]}"), None)
        peak = max(pts, key=lambda p: p[1])
        pk.kpi("FAO Food Price Index", f"{f['latest']:.1f}", f"{f['date'][:7]} · Δ {f['chg']:+.1f} m/m" if f["chg"] is not None else f["date"][:7],
               "warning" if f["pct_rank"] and f["pct_rank"] > 85 else "info")
        if yoy:
            pk.kpi("Year on year", f"{100 * (f['latest'] / yoy - 1):+.1f}%", f"vs {yoy:.1f} a year earlier", "neg" if f["latest"] / yoy - 1 > 0.15 else "info")
        pk.kpi("Percentile since 1990", RS.ordinal(f["pct_rank"]), f"peak {peak[1]:.1f} in {peak[0][:7]}", "info")
        pk.kpi("Distance from peak", f"{100 * (f['latest'] / peak[1] - 1):+.1f}%", f"{peak[0][:7]} all-time high", "mute")
        rows = []
        for name, (sid, label) in COLS.items():
            s = pk.series.get(sid)
            if not s:
                continue
            y = next((v for d, v in s["_points"] if d[:7] == f"{int(s['date'][:4]) - 1}-{s['date'][5:7]}"), None)
            rows.append([label, round(s["latest"], 1), round(s["chg"], 1) if s["chg"] is not None else None,
                         round(100 * (s["latest"] / y - 1), 1) if y else None, s["pct_rank"], round(s["max"], 1)])
        pk.add_table("subindices", ["Index", "Latest", "Δ m/m", "Δ y/y %", "Pct rank since 1990", "Record"], rows,
                     title="FFPI and sub-indices", note="FAO revises recent months as trade-weighted prices firm up.")
    pk.note("The FFPI is the trade-weighted average of the five group indices (2014-16 = 100). FAO publishes on the first Friday of "
            "the month and revises the previous month. The 2022-03 reading (159.7) is the series high; 2008 and 2011 were the earlier spikes.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
