"""justhodl-nyfed-cmdi — Federal Reserve Bank of New York, Corporate Bond Market Distress Index.

The CMDI is the Fed's own summary gauge of U.S. corporate bond market functioning,
combining primary-market (issuance, pricing) and secondary-market (spreads,
liquidity, default-adjusted) measures into one 0–1 index for the whole market,
investment grade and high yield, weekly since January 2005. The interactive's
own workbook is public (no key):

  https://www.newyorkfed.org/medialibrary/research/interactives/data/cmdi/cmdi_interactive_data.xlsx
  columns: eow_friday (Excel serial), Market CMDI, IG CMDI, HY CMDI, p5…p99 (historical percentile bands)

We mirror the workbook verbatim, parse the three indices and the percentile
bands into series, and place today's reading against the full history (GFC,
2020) — descriptive only. Weekly cadence; checked daily.
"""
from __future__ import annotations

import risk_sources as RS

SLUG = "nyfed-cmdi"
XLSX = "https://www.newyorkfed.org/medialibrary/research/interactives/data/cmdi/cmdi_interactive_data.xlsx"
PAGE = "https://www.newyorkfed.org/research/policy/cmdi"
LABELS = {"Market CMDI": ("MARKET", "CMDI — whole corporate bond market"),
          "IG CMDI": ("IG", "CMDI — investment grade"),
          "HY CMDI": ("HY", "CMDI — high yield")}


def build(event):
    pk = RS.Packet(
        SLUG, "NY Fed CMDI", "NY Fed Corporate Bond Market Distress Index (CMDI)",
        "The New York Fed's weekly 0–1 index of U.S. corporate bond market functioning for the whole market, "
        "investment grade and high yield, built from primary- and secondary-market measures (issuance, pricing, "
        "spreads, liquidity, default-adjusted spreads). Read from the interactive's own public workbook since 2005.",
        {"provider": "Federal Reserve Bank of New York — Research", "url": PAGE, "docs": PAGE + "#/faq",
         "license": "Public (NY Fed terms of use)", "cadence": "weekly (Friday), published with a short lag"},
        cadence="weekly")
    blob = RS.http_get(XLSX, timeout=90)
    pk.raw("cmdi_interactive_data.xlsx", blob,
           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", url=XLSX)
    x = RS.Xlsx(blob)
    rows = x.rows(0)
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    col = {h: i for i, h in enumerate(header)}
    if "eow_friday" not in col:
        raise RuntimeError(f"CMDI workbook header changed: {header}")
    cols = {k: [] for k in header if k != "eow_friday"}
    for r in rows[1:]:
        if not r or r[0] is None:
            continue
        d = RS.excel_date(r[0])
        if not d:
            continue
        for k, i in col.items():
            if k == "eow_friday" or i >= len(r):
                continue
            v = RS.parse_num(r[i])
            if v is not None:
                cols[k].append([d, v])
    for k, (sid, label) in LABELS.items():
        if cols.get(k):
            pk.add_series(sid, label, cols[k], unit="index 0–1", freq="weekly", group="index")
    for k in header:
        if k.startswith("p") and k[1:].isdigit() and cols.get(k):
            pk.add_series(f"BAND_{k.upper()}", f"Historical percentile band {k}", cols[k], unit="index 0–1",
                          freq="weekly", group="band")
    m = pk.series.get("MARKET")
    if m:
        pts = m["_points"]
        peak = max(pts, key=lambda p: p[1])
        pk.kpi("Market CMDI", f"{m['latest']:.2f}", f"week of {m['date']} · {m['pct_rank']}th pct since 2005",
               "neg" if m["pct_rank"] and m["pct_rank"] > 85 else "info")
        pk.kpi("All-time peak", f"{peak[1]:.2f}", f"{peak[0]} (GFC)", "mute")
        for sid in ("IG", "HY"):
            s = pk.series.get(sid)
            if s:
                pk.kpi(f"{sid} CMDI", f"{s['latest']:.2f}", f"{s['pct_rank']}th pct · Δ {s['chg']:+.2f} w/w" if s['chg'] is not None else "", "info")
        # yearly table of annual mean / max
        years = {}
        for d, v in pts:
            y = d[:4]
            years.setdefault(y, []).append(v)
        pk.add_table("annual", ["Year", "Mean", "Max", "Min", "Weeks"],
                     [[y, round(sum(v) / len(v), 3), round(max(v), 3), round(min(v), 3), len(v)] for y, v in sorted(years.items())],
                     title="Market CMDI by year", note="Simple statistics of the weekly Market CMDI.")
    pk.note("The CMDI is published weekly for the Friday end of week and is revised as inputs update. 0 = best observed functioning, 1 = worst. "
            "Percentile bands (p5 … p99) are the NY Fed's own historical reference lines.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
