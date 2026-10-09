"""justhodl-worldbank-wgi — World Bank Worldwide Governance Indicators (Kaufmann & Kraay), annual 1996→.

Public Excel in the WGI download zip (no key):
  https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset_excel.zip  -> wgidataset.xlsx (long format)
  columns: code, countryname, year, indicator (cc ge pv rq rl va), estimate, stddev, nsource, pctrank, pctranklower, pctrankupper
Six dimensions: Voice & Accountability, Political Stability / No Violence, Government Effectiveness, Regulatory Quality,
Rule of Law, Control of Corruption. We publish the estimate (−2.5 … +2.5) per country per dimension and a latest-year
percentile grid. Descriptive only.
"""
from __future__ import annotations

import zipfile
import io

import risk_sources as RS

SLUG = "worldbank-wgi"
ZIP = "https://www.worldbank.org/content/dam/sites/govindicators/doc/wgidataset_excel.zip"
PAGE = "https://www.worldbank.org/en/publication/worldwide-governance-indicators"
DIMS = {"va": "Voice and Accountability", "pv": "Political Stability and Absence of Violence",
        "ge": "Government Effectiveness", "rq": "Regulatory Quality", "rl": "Rule of Law", "cc": "Control of Corruption"}


def parse(rows):
    """long rows -> {(code, ind): {'name':..., 'est': [[date, v]], 'rank': [[date, v]]}}"""
    header = [str(h).strip().lower() if h is not None else "" for h in rows[0]]
    col = {h: i for i, h in enumerate(header)}
    for need in ("code", "countryname", "year", "indicator", "estimate", "pctrank"):
        if need not in col:
            raise RuntimeError(f"WGI workbook header changed: {header}")
    out = {}
    for r in rows[1:]:
        if not r or len(r) <= col["pctrank"]:
            continue
        code, ind = r[col["code"]], r[col["indicator"]]
        if not code or ind not in DIMS:
            continue
        try:
            y = int(float(r[col["year"]]))
        except (TypeError, ValueError):
            continue
        d = f"{y}-12-31"
        slot = out.setdefault((str(code), ind), {"name": r[col["countryname"]], "est": [], "rank": []})
        v = RS.parse_num(r[col["estimate"]])
        if v is not None:
            slot["est"].append([d, v])
        v = RS.parse_num(r[col["pctrank"]])
        if v is not None:
            slot["rank"].append([d, v])
    return out


def build(event):
    pk = RS.Packet(
        SLUG, "World Bank WGI", "World Bank Worldwide Governance Indicators (WGI)",
        "Six aggregate governance indicators for ~210 economies since 1996 — voice and accountability, political stability "
        "and absence of violence, government effectiveness, regulatory quality, rule of law, control of corruption — "
        "combining 30+ underlying data sources. Governance quality is the slow-moving backdrop behind sovereign ratings "
        "and default histories. Read from the World Bank's public download.",
        {"provider": "World Bank — Kaufmann & Kraay", "url": PAGE, "docs": "https://www.worldbank.org/en/publication/worldwide-governance-indicators/interactive-data-access",
         "license": "CC BY 4.0", "cadence": "annual (autumn update)"},
        cadence="weekly")
    pk.hot_tail = 8                     # 1,284 annual series: eight years in the hot packet, full history in warm
    blob = RS.http_get(ZIP, timeout=120)
    pk.raw("wgidataset_excel.zip", blob, "application/zip", url=ZIP)
    z = zipfile.ZipFile(io.BytesIO(blob))
    name = next((n for n in z.namelist() if n.lower().endswith(".xlsx")), None)
    if not name:
        raise RuntimeError(f"WGI zip has no xlsx: {z.namelist()}")
    data = parse(RS.Xlsx(z.read(name)).rows(0))
    latest_year = max(int(p[0][:4]) for s in data.values() for p in s["est"]) if data else None
    grid = {}
    for (code, ind), s in sorted(data.items()):
        rec = pk.add_series(f"{ind.upper()}_{code}", f"{s['name']} — {DIMS[ind]} (estimate)", s["est"], unit="estimate −2.5…+2.5",
                            freq="annual", group=ind, iso3=code, country=s["name"])
        if rec:
            g = grid.setdefault(code, {"name": s["name"], "year": rec["date"][:4]})
            rk = next((v for d, v in reversed(s["rank"]) if d == rec["date"]), None)
            g[ind] = round(rec["latest"], 2)
            g[ind + "_rank"] = round(rk, 1) if rk is not None else None
            prior = next((v for d, v in s["est"] if d[:4] == str(int(rec["date"][:4]) - 5)), None)
            g[ind + "_chg5"] = round(rec["latest"] - prior, 2) if prior is not None else None
    rows = []
    for code, g in grid.items():
        avg = [g.get(k) for k in DIMS if g.get(k) is not None]
        rows.append([code, g["name"], g["year"]] + [g.get(k + "_rank") for k in DIMS] +
                    [round(sum(avg) / len(avg), 2) if avg else None, g.get("pv_chg5")])
    rows.sort(key=lambda r: (r[-2] if r[-2] is not None else 9))
    pk.add_table("grid", ["ISO3", "Country", "Year"] + [DIMS[k] + " (pct rank)" for k in DIMS] + ["Mean estimate", "Δ5y political stability"], rows,
                 title=f"Governance percentile ranks, {latest_year}", note="Percentile rank 0–100 among all economies that year; mean estimate averages the six point estimates (−2.5 … +2.5). Sorted weakest first.")
    if rows:
        pk.kpi("Economies covered", str(len(rows)), f"latest year {latest_year} · six dimensions", "info")
        worst = rows[0]
        pk.kpi("Weakest mean governance", f"{worst[1]}", f"mean estimate {worst[-2]}", "neg")
        fall = sorted([r for r in rows if r[-1] is not None], key=lambda r: r[-1])[:3]
        if fall:
            pk.kpi("Largest 5y fall, political stability", f"{fall[0][1]} {fall[0][-1]:+.2f}", ", ".join(f"{r[1]} {r[-1]:+.2f}" for r in fall[1:]), "warning")
        us = grid.get("USA")
        if us:
            pk.kpi("United States", f"RL {us.get('rl')} · PV {us.get('pv')}", f"rule of law / political stability estimates, {us['year']}", "info")
    pk.note("Estimates are in standard-normal units (mean 0, s.d. 1 across economies each year); differences of less than ~0.3 are usually "
            "within the margin of error. The World Bank updates the series each autumn with a one-year lag.")
    return pk


def lambda_handler(event, context=None):
    return RS.run(SLUG, build, event)
